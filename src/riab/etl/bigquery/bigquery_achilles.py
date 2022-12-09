# Copyright 2022 RADar-AZDelta
# SPDX-License-Identifier: gpl3+

import logging
import traceback
from pathlib import Path
from tempfile import NamedTemporaryFile, TemporaryDirectory
from time import time
from typing import Any, List, Tuple, Union

import google.cloud.bigquery as bq
import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from ..achilles import Achilles
from .bigquery_etl_base import BigQueryEtlBase


class BigQueryAchilles(Achilles, BigQueryEtlBase):
    def __init__(
        self,
        **kwargs,
    ):
        super().__init__(target_dialect="bigquery", **kwargs)

    def _run_query(self, sql) -> Tuple[Union[pl.DataFrame, pl.Series] | None, float]:
        try:
            result, execution_time = self._gcp.run_query_job_with_benchmark(sql)
            return pl.from_arrow(result.to_arrow()), execution_time
        except Exception as ex:
            logging.warn(traceback.format_exc())
            return (None, -1)

    def _store_analysis_details(self, df: pl.DataFrame):
        table = df.to_arrow()
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

    def _get_indices_sqls(
        self, achilles_tables: list[str] = ["achilles_results", "achilles_results_dist"]
    ) -> list[str]:
        return ["/* INDEX CREATION SKIPPED, INDICES NOT SUPPORTED IN BIGQUERY */"]
