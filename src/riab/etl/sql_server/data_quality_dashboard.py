# Copyright 2024 RADar-AZDelta
# SPDX-License-Identifier: gpl3+

from typing import Any,  cast

import polars as pl

from ..data_quality_dashboard import DataQualityDashboard
from .etl_base import SqlServerEtlBase


class SqlServerDataQualityDashboard(DataQualityDashboard, SqlServerEtlBase):
    def __init__(
        self,
        **kwargs,
    ):
        super().__init__(**kwargs)

    def _get_last_runs(self) -> list[Any]:
        template = self._template_env.get_template("dqd/get_last_dqd_runs.sql.jinja")
        sql = template.render(
            dqd_database_catalog=self._dqd_database_catalog,
            dqd_database_schema=self._dqd_database_schema,
        )
        rows = self._db.run_query(sql)
        return rows or []

    def _get_run(self, id: str) -> Any:
        template = self._template_env.get_template("dqd/get_dqd_run.sql.jinja")
        sql = template.render(
            dqd_database_catalog=self._dqd_database_catalog,
            dqd_database_schema=self._dqd_database_schema,
        )
        rows = self._db.run_query(sql, {
                "id": id,
            })
        return rows[0] if rows else None

    def _get_results(self, run_id: str) -> pl.DataFrame:
        template = self._template_env.get_template("dqd/get_dqd_run_results.sql.jinja")
        sql = template.render(
            dqd_database_catalog=self._dqd_database_catalog,
            dqd_database_schema=self._dqd_database_schema,
        )
        rows = self._db.run_query(sql, {
                "id": run_id,
            })
        data_frame = pl.from_dicts(rows or [])
        data_frame = data_frame.with_columns(
            [
                pl.col("query_text").str.replace_all("<br>", "\n").alias("query_text"),
            ]
        )
        return cast(pl.DataFrame, data_frame)
