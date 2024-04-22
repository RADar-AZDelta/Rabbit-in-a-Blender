# Copyright 2024 RADar-AZDelta
# SPDX-License-Identifier: gpl3+

import logging
import traceback
from typing import Tuple

import polars as pl

from ..achilles import Achilles
from .etl_base import SqlServerEtlBase


class SqlServerAchilles(Achilles, SqlServerEtlBase):
    """
    SqlServer implementation that runs the descriptive statistics
    """

    def __init__(
        self,
        **kwargs,
    ):
        """Constructor"""
        super().__init__(**kwargs)

    def _run_query(self, sql) -> Tuple[pl.DataFrame, float]:
        try:
            rows, execution_time = self._db.run_query_with_benchmark(sql)
            data_frame = pl.from_dicts(rows or []) if rows else pl.DataFrame()
            return data_frame, execution_time
        except Exception:
            logging.warning(traceback.format_exc())
            return (pl.DataFrame([]), -1)

    def _store_analysis_details(self, data_frame: pl.DataFrame):
        self._upload_dataframe(
            self._achilles_database_catalog, self._achilles_database_schema, "achilles_analysis", data_frame
        )

    @property
    def _temp_emulation_schema(self) -> str:
        return f"[{self._achilles_database_catalog}].[{self._achilles_database_schema}]"

    @property
    def _voc_database_schema(self) -> str:
        return f"[{self._omop_database_catalog}].[{self._omop_database_schema}]"

    @property
    def _cdm_database_schema(self) -> str:
        return f"[{self._omop_database_catalog}].[{self._omop_database_schema}]"

    @property
    def _scratch_database_schema(self) -> str:
        return f"[{self._achilles_database_catalog}].[{self._achilles_database_schema}]"  # multithreaded doesn't support tempdb

    @property
    def _results_database_schema(self) -> str:
        return f"[{self._achilles_database_catalog}].[{self._achilles_database_schema}]"

    @property
    def _supports_temp_tables(self) -> bool:
        return False  # multithreaded doesn't support tempdb

    @property
    def _schema_delim(self) -> str:
        return "."  # multithreaded doesn't support tempdb

    def _pre_prep_analysis_query(self, sql: str) -> str:
        return sql.replace("#", f"[{self._dqd_database_catalog}].[{self._dqd_database_schema}].")

    def _post_prep_analysis_query(self, sql: str) -> str:
        return sql.replace("CREATE TABLE", "CREATE OR REPLACE TABLE")

    @property
    def _drop_index_sql(self) -> str:
        return "drop index if exists @resultsDatabaseSchema.@indexName;"
