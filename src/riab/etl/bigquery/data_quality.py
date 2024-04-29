# Copyright 2024 RADar-AZDelta
# SPDX-License-Identifier: gpl3+

import logging
from typing import Any, Optional

import polars as pl

from ..data_quality import DataQuality
from .etl_base import BigQueryEtlBase


class BigQueryDataQuality(DataQuality, BigQueryEtlBase):
    def __init__(
        self,
        **kwargs,
    ):
        super().__init__(**kwargs)

    def _run_check_query(
        self, check: Any, row: str, parameters: Any, cohort_definition_id: Optional[int] = None
    ) -> Any:
        sql = None
        result = None
        exception: str | None = None
        execution_time = -1
        try:
            parameters["cdmDatabaseSchema"] = self._dataset_omop
            parameters["cohortDatabaseSchema"] = self._dataset_omop
            parameters["cohortTableName"] = "cohort"
            parameters["cohortDefinitionId"] = cohort_definition_id if cohort_definition_id else 0
            parameters["vocabDatabaseSchema"] = self._dataset_omop
            # parameters["cohort"] = True if cohort_definition_id else False
            parameters["cohort"] = "TRUE" if cohort_definition_id else "FALSE"
            parameters["schema"] = self._dataset_omop

            sql = self._render_sqlfile(check["sqlFile"], parameters)

            rows, execution_time = self._gcp.run_query_job_with_benchmark(sql)

            result = dict(next(rows))
        except Exception as ex:
            logging.warn(f"Failed to run QDQ check {check['checkName']}\nquery:\n{sql}\n{ex}")
            # with open(
            #     Path(__file__).parent.parent.parent.resolve()
            #     / "libs"
            #     / "DataQualityDashboard"
            #     / "inst"
            #     / "sql"
            #     / "sql_server"
            #     / check["sqlFile"],
            #     "r",
            #     encoding="utf-8",
            # ) as file:
            #     rendered_sql = self._render_sql(self._db_engine, file.read(), parameters)
            exception = str(ex)
            # if __debug__:
            #     breakpoint()

        return self._process_check(check, row, parameters, sql, result, execution_time, exception)

    # def _render_sqlfile(self, sql_file: str, parameters: dict):
    #     d: dict = {}
    #     for k, v in parameters.items():
    #         if "Table" in k and isinstance(v, str):
    #             d[k.lower()] = v.lower()
    #         else:
    #             d[k.lower()] = v

    #     template = self._template_env.get_template(f"dqd/{sql_file}.jinja")
    #     jinja_sql = template.render(d)

    #     return jinja_sql

    def _get_cdm_sources(self) -> list[Any]:
        """Merges the uploaded custom concepts in the OMOP concept table.

        Returns:
            RowIterator | _EmptyRowIterator: The result rows
        """
        template = self._template_env.from_string("select * from {{dataset_omop}}.cdm_source;")
        sql = template.render(
            dataset_omop=self._dataset_omop,
        )
        rows = self._gcp.run_query_job(sql)
        return [dict(row.items()) for row in rows]

    def _store_dqd_run(self, dqd_run: dict):
        df = pl.from_dicts([dqd_run])
        self._append_dataframe_to_bigquery_table(df, self._dataset_dqd, "dqdashboard_runs")

    def _store_dqd_result(self, dqd_result: pl.DataFrame):
        dqd_result.with_columns(
            [
                pl.col("num_violated_rows").replace(None, 0).alias("num_violated_rows"),
                pl.col("num_denominator_rows").replace(None, 0).alias("num_denominator_rows"),
                pl.col("threshold_value").replace(None, 0).alias("threshold_value"),
            ]
        )
        self._append_dataframe_to_bigquery_table(dqd_result, self._dataset_dqd, "dqdashboard_results")
