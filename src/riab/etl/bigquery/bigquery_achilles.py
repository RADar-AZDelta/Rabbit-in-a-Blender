# Copyright 2024 RADar-AZDelta
# SPDX-License-Identifier: gpl3+

import logging
import traceback
from typing import Tuple, cast

import polars as pl

from ..achilles import Achilles
from .bigquery_etl_base import BigQueryEtlBase


class BigQueryAchilles(Achilles, BigQueryEtlBase):
    """
    BigQuery implementation that runs the descriptive statistics
    """

    def __init__(
        self,
        **kwargs,
    ):
        """Constructor"""
        super().__init__(target_dialect="bigquery", **kwargs)

    def _run_query(self, sql) -> Tuple[pl.DataFrame, float]:
        try:
            result, execution_time = self._gcp.run_query_job_with_benchmark(sql)
            data_frame = pl.from_arrow(result.to_arrow())
            return cast(pl.DataFrame, data_frame), execution_time
        except Exception:
            logging.warning(traceback.format_exc())
            return (pl.DataFrame([]), -1)

    def _store_analysis_details(self, data_frame: pl.DataFrame):
        table = data_frame.to_arrow()
        self._upload_arrow_table(table, "achilles_analysis")

    @property
    def _supports_temp_tables(self) -> bool:
        return False

    @property
    def _schema_delim(self) -> str:
        return "."

    def _pre_prep_query(self, sql: str) -> str:
        return sql.replace("#", f"{self._temp_emulation_schema}.")

    def _post_prep_query(self, sql: str) -> str:
        return sql.replace("CREATE TABLE", "CREATE OR REPLACE TABLE")

    def _get_indices_sqls(self, achilles_tables: list[str] | None = None) -> list[str]:
        if not achilles_tables:
            achilles_tables = ["achilles_results", "achilles_results_dist"]
        return ["/* INDEX CREATION SKIPPED, INDICES NOT SUPPORTED IN BIGQUERY */"]
