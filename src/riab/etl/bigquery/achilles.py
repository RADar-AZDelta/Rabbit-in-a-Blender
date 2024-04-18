# Copyright 2024 RADar-AZDelta
# SPDX-License-Identifier: gpl3+

import logging
import traceback
from typing import Tuple, cast

import polars as pl

from ..achilles import Achilles
from .etl_base import BigQueryEtlBase


class BigQueryAchilles(Achilles, BigQueryEtlBase):
    """
    BigQuery implementation that runs the descriptive statistics
    """

    def __init__(
        self,
        **kwargs,
    ):
        """Constructor"""
        super().__init__(**kwargs)

    def _run_query(self, sql) -> Tuple[pl.DataFrame, float]:
        try:
            result, execution_time = self._gcp.run_query_job_with_benchmark(sql)
            data_frame = pl.from_arrow(result.to_arrow())
            return cast(pl.DataFrame, data_frame), execution_time
        except Exception:
            logging.warning(traceback.format_exc())
            return (pl.DataFrame([]), -1)

    def _store_analysis_details(self, data_frame: pl.DataFrame):
        self._append_dataframe_to_bigquery_table(data_frame, self._dataset_achilles, "achilles_analysis")

    @property
    def _temp_emulation_schema(self) -> str:
        return self._dataset_achilles

    @property
    def _voc_database_schema(self) -> str:
        return self._dataset_omop

    @property
    def _cdm_database_schema(self) -> str:
        return self._dataset_omop

    @property
    def _scratch_database_schema(self) -> str:
        return self._dataset_achilles

    @property
    def _results_database_schema(self) -> str:
        return self._dataset_achilles

    @property
    def _supports_temp_tables(self) -> bool:
        return False

    @property
    def _create_indices(self) -> bool:
        return False

    @property
    def _schema_delim(self) -> str:
        return "."

    def _pre_prep_analysis_query(self, sql: str) -> str:
        return sql.replace("#", f"{self._dataset_dqd}.")

    def _post_prep_analysis_query(self, sql: str) -> str:
        return sql.replace("CREATE TABLE", "CREATE OR REPLACE TABLE")
