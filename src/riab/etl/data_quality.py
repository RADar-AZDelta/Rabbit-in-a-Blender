# Copyright 2024 RADar-AZDelta
# SPDX-License-Identifier: gpl3+

import json
import logging
import uuid
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from time import time
from typing import Any, List, Optional, cast

import polars as pl
from humanfriendly import format_timespan

from .etl_base import EtlBase
from .sql_render_base import SqlRenderBase
from .utils import StringConverter


class DataQuality(SqlRenderBase, EtlBase, ABC):
    """
    Class that runs the data quality checks
    """

    def __init__(
        self,
        data_quality_dashboard_version: str = "1.4.1",
        json_path: str | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)

        self.data_quality_dashboard_version = data_quality_dashboard_version
        self.json_path = json_path

    def run(self, tables_to_exclude: list[str] | None = None):
        if not tables_to_exclude:
            tables_to_exclude = [
                "CONCEPT",
                "VOCABULARY",
                "CONCEPT_ANCESTOR",
                "CONCEPT_RELATIONSHIP",
                "CONCEPT_CLASS",
                "CONCEPT_SYNONYM",
                "RELATIONSHIP",
                "DOMAIN",
            ]

        logging.info("Checking data quality")

        start = time()

        df_check_descriptions = pl.read_csv(
            str(
                Path(__file__).parent.parent.resolve()
                / "libs"
                / "DataQualityDashboard"
                / "inst"
                / "csv"
                / f"OMOP_CDMv{self._omop_cdm_version}_Check_Descriptions.csv"
            )
        )

        df_table_level = pl.read_csv(
            str(
                Path(__file__).parent.parent.resolve()
                / "libs"
                / "DataQualityDashboard"
                / "inst"
                / "csv"
                / f"OMOP_CDMv{self._omop_cdm_version}_Table_Level.csv"
            )
        )
        df_field_level = pl.read_csv(
            str(
                Path(__file__).parent.parent.resolve()
                / "libs"
                / "DataQualityDashboard"
                / "inst"
                / "csv"
                / f"OMOP_CDMv{self._omop_cdm_version}_Field_Level.csv"
            )
        )
        df_concept_level = pl.read_csv(
            str(
                Path(__file__).parent.parent.resolve()
                / "libs"
                / "DataQualityDashboard"
                / "inst"
                / "csv"
                / f"OMOP_CDMv{self._omop_cdm_version}_Concept_Level.csv"
            ),
            dtypes={
                "conceptId": pl.Utf8,  # type: ignore
            },
        )

        # ensure we use only checks that are intended to be run
        df_table_level = df_table_level.filter(~pl.col("cdmTableName").is_in(tables_to_exclude))
        df_field_level = df_field_level.filter(~pl.col("cdmTableName").is_in(tables_to_exclude))
        df_concept_level = df_concept_level.filter(~pl.col("cdmTableName").is_in(tables_to_exclude))

        # remove offset from being checked
        df_field_level = df_field_level.filter(pl.col("cdmFieldName").ne("offset"))

        metadata = self._capture_check_metadata()

        check_results = pl.DataFrame()
        for index, check in enumerate(df_check_descriptions.iter_rows(named=True)):
            check_results = pl.concat(
                [
                    check_results,
                    self._run_check(
                        check,
                        cast(int, index),
                        df_table_level,
                        df_field_level,
                        df_concept_level,
                    ),
                ]
            )

        end = time()
        execution_time = end - start

        check_summary = {
            "startTimestamp": datetime.fromtimestamp(start),
            "endTimestamp": datetime.fromtimestamp(end),
            "executionTime": format_timespan(execution_time),
            "Overview": self._summarize_check_results(check_results),
            "Metadata": metadata,
        }

        # write results to database
        dqd_run = check_summary["Overview"]
        dqd_run["startTimestamp"] = check_summary["startTimestamp"]
        dqd_run["endTimestamp"] = check_summary["endTimestamp"]
        dqd_run["executionTime"] = check_summary["executionTime"]
        dqd_run["id"] = str(uuid.uuid4())
        self._store_dqd_run(dqd_run)

        dqd_results = check_results.clone()
        dqd_results = dqd_results.with_columns(pl.lit(dqd_run["id"]).alias("run_id"))
        dqd_results = dqd_results.drop("_row")
        self._store_dqd_result(dqd_results)

        if self.json_path:
            # uppercase columns names
            check_results.columns = [
                column.upper() if column not in ["checkId", "_row"] else column for column in check_results.columns
            ]
            check_summary["CheckResults"] = [self._cleanNullTerms(row) for row in check_results.iter_rows(named=True)]
            with open(
                self.json_path,
                "w",
                encoding="utf-8",
            ) as file:  # outputFile <- sprintf("%s-%s.json", tolower(metadata$CDM_SOURCE_ABBREVIATION),endTimestamp)
                json.dump(check_summary, file, indent=4, sort_keys=True, default=str)

    def _capture_check_metadata(self):
        cdm_soures = self._get_cdm_sources()
        metadata = [dict((k.upper(), v) for k, v in cdm_soure.items()) for cdm_soure in cdm_soures]
        for item in metadata:
            item["DQD_VERSION"] = self.data_quality_dashboard_version
        return metadata

    def _cleanNullTerms(self, d: dict):
        clean = {}
        for k, v in d.items():
            if isinstance(v, dict):
                nested = self._cleanNullTerms(v)
                if len(nested.keys()) > 0:
                    clean[k] = nested
            elif v is not None:
                clean[k] = v
        return clean

    def _run_check(
        self,
        check: Any,
        row: int,
        df_table_level: pl.DataFrame,
        df_field_level: pl.DataFrame,
        df_concept_level: pl.DataFrame,
    ) -> pl.DataFrame:
        data_frame = None
        match check["checkLevel"]:
            case "TABLE":
                data_frame = df_table_level
            case "FIELD":
                data_frame = df_field_level
            case "CONCEPT":
                data_frame = df_concept_level
            case _:
                raise Exception(f"Unknown check level: {check["checkLevel"]}")

        try:
            data_frame = data_frame.filter(pl.sql_expr(check["evaluationFilter"]))
        except Exception:
            # fallback to pandas eval
            pd_df = data_frame.to_pandas()
            import pandas as pd

            data_frame = pl.from_pandas(pd.DataFrame(pd_df[pd_df.eval(check["evaluationFilter"])]))

        check_results = []
        with ThreadPoolExecutor(max_workers=self._max_worker_threads_per_table) as executor:
            futures = [
                executor.submit(
                    self._run_check_query,
                    check,
                    f"{int(row) + 1}.{cast(int, index) + 1}",
                    item,
                )
                for index, item in enumerate(data_frame.iter_rows(named=True))
            ]
            # wait(futures, return_when=ALL_COMPLETED)
            for result in as_completed(futures):
                check_result = result.result()
                check_results.append(check_result)

        schema = {
            "run_id": pl.Utf8,
            "checkid": pl.Utf8,
            "num_violated_rows": pl.Int64,
            "pct_violated_rows": pl.Float64,
            "num_denominator_rows": pl.Int64,
            "execution_time": pl.Utf8,
            "query_text": pl.Utf8,
            "check_name": pl.Utf8,
            "check_level": pl.Utf8,
            "check_description": pl.Utf8,
            "cdm_table_name": pl.Utf8,
            "cdm_field_name": pl.Utf8,
            "concept_id": pl.Utf8,
            "unit_concept_id": pl.Utf8,
            "sql_file": pl.Utf8,
            "category": pl.Utf8,
            "subcategory": pl.Utf8,
            "context": pl.Utf8,
            "warning": pl.Utf8,
            "error": pl.Utf8,
            "failed": pl.Int64,
            "threshold_value": pl.Int64,
            "notes_value": pl.Utf8,
            "_row": pl.Utf8,
        }
        return pl.from_dicts(check_results, schema=schema).sort("_row")

    @abstractmethod
    def _run_check_query(self, check: Any, row: str, item: Any) -> Any:
        pass

    @abstractmethod
    def _get_cdm_sources(self) -> List[Any]:
        """Merges the uploaded custom concepts in the OMOP concept table.

        Returns:
            RowIterator | _EmptyRowIterator: The result rows
        """
        pass

    def _summarize_check_results(self, check_results: pl.DataFrame) -> Any:
        countTotal = len(check_results)
        countThresholdFailed = len(check_results.filter(pl.col("failed").eq(1) & pl.col("error").is_null()))
        countErrorFailed = len(check_results.filter(~pl.col("error").is_null()))
        countOverallFailed = len(check_results.filter(pl.col("failed").eq(1)))
        countPassed = countTotal - countOverallFailed
        percentPassed = round((countPassed / countTotal) * 100)
        percentFailed = round((countOverallFailed / countTotal) * 100)
        countTotalPlausibility = len(check_results.filter(pl.col("category") == "Plausibility"))
        countTotalConformance = len(check_results.filter(pl.col("category") == "Conformance"))
        countTotalCompleteness = len(check_results.filter(pl.col("category") == "Completeness"))
        countFailedPlausibility = len(
            check_results.filter(pl.col("failed").eq(1) & (pl.col("category") == "Plausibility"))
        )
        countFailedConformance = len(
            check_results.filter(pl.col("failed").eq(1) & (pl.col("category") == "Conformance"))
        )
        countFailedCompleteness = len(
            check_results.filter(pl.col("failed").eq(1) & (pl.col("category") == "Completeness"))
        )

        check_summary = {
            "countTotal": countTotal,
            "countThresholdFailed": countThresholdFailed,
            "countErrorFailed": countErrorFailed,
            "countOverallFailed": countOverallFailed,
            "countPassed": countPassed,
            "percentPassed": percentPassed,
            "percentFailed": percentFailed,
            "countTotalPlausibility": countTotalPlausibility,
            "countTotalConformance": countTotalConformance,
            "countTotalCompleteness": countTotalCompleteness,
            "countFailedPlausibility": countFailedPlausibility,
            "countFailedConformance": countFailedConformance,
            "countFailedCompleteness": countFailedCompleteness,
            "countPassedPlausibility": countTotalPlausibility - countFailedPlausibility,
            "countPassedConformance": countTotalConformance - countFailedConformance,
            "countPassedCompleteness": countTotalCompleteness - countFailedCompleteness,
        }

        return check_summary

    def _evaluate_check_threshold(
        self,
        check: Any,
        item: Any,
        result: Any,
    ) -> Any:
        threshold_field = f"{check["checkName"]}Threshold"
        notes_field = f"{check["checkName"]}Notes"
        check_threshold: dict[str, Any] = {"failed": 0}

        if hasattr(item, threshold_field):
            try:
                check_threshold["threshold_value"] = float(item[threshold_field])
            except ValueError:
                check_threshold["threshold_value"] = 0
            if hasattr(item, notes_field):
                check_threshold["notes_value"] = item[notes_field]

        if ("threshold_value" not in check_threshold) or (check_threshold["threshold_value"] == 0):
            if result["num_violated_rows"] and result["num_violated_rows"] > 0:
                check_threshold["failed"] = 1
        else:
            if result["pct_violated_rows"] * 100 > check_threshold["threshold_value"]:
                check_threshold["failed"] = 1

        return check_threshold

    def _process_check(
        self,
        check: Any,
        row: str,
        item: Any,
        sql: Optional[str],
        result: Optional[Any],
        execution_time: Optional[float],
        exception: Optional[str],
    ) -> Any:
        check_result = {
            "num_violated_rows": result["num_violated_rows"] if result else None,
            "pct_violated_rows": round(result["pct_violated_rows"], 4) if result else None,
            "num_denominator_rows": result["num_denominator_rows"] if result else None,
            "execution_time": f"{execution_time:.6f} secs",
            "query_text": sql,
            "check_name": check["checkName"],
            "check_level": check["checkLevel"],
            "check_description": check["checkDescription"],
            "cdm_table_name": item.cdmTableName if hasattr(item, "cdmTableName") else None,
            "cdm_field_name": item.cdmFieldName if hasattr(item, "cdmFieldName") else None,
            "concept_id": item.conceptTd if hasattr(item, "conceptTd") else None,
            "unit_concept_id": item.unitConceptId if hasattr(item, "unitConceptId") else None,
            "sql_file": check["sqlFile"],
            "category": check["kahnCategory"],
            "subcategory": check["kahnSubcategory"],
            "context": check["kahnContext"],
            # "warning": warning,
            "error": exception,
            "checkId": self._get_check_id(check, item),
            # row.names = NULL,
            # stringsAsFactors = FALSE
            "_row": row,
        }
        if exception:
            check_result["failed"] = 1
        elif result:
            check_result.update(self._evaluate_check_threshold(check, item, result))
        return check_result

    def _get_check_id(self, check: Any, item: Any):
        id = [check["checkLevel"].lower(), check["checkName"].lower()]
        if hasattr(item, "cdmTableName"):
            id.append(item.cdmTableName.lower())
        if hasattr(item, "cdmFieldName"):
            id.append(item.cdmFieldName.lower())
        if hasattr(item, "conceptId"):
            id.append(item.conceptId.lower())
        if hasattr(item, "unitConceptId"):
            id.append(item.unitConceptId.lower())
        return "_".join(id)

    @abstractmethod
    def _store_dqd_run(self, dqd_run: dict):
        pass

    @abstractmethod
    def _store_dqd_result(self, dqd_result: pl.DataFrame):
        pass

    def _render_sqlfile(self, sql_file: str, parameters: dict):
        with open(
            Path(__file__).parent.parent.resolve()
            / "libs"
            / "DataQualityDashboard"
            / "inst"
            / "sql"
            / "sql_server"
            / sql_file,
            "r",
            encoding="utf-8",
        ) as file:
            sql = file.read()

        rendered_sql = self._render_sql(self._db_engine, sql, parameters)

        return rendered_sql
