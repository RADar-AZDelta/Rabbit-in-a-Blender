# Copyright 2024 RADar-AZDelta
# SPDX-License-Identifier: gpl3+

from typing import Any, cast

import google.cloud.bigquery as bq
import polars as pl

from ..data_quality_dashboard import DataQualityDashboard
from .etl_base import BigQueryEtlBase


class BigQueryDataQualityDashboard(DataQualityDashboard, BigQueryEtlBase):
    def __init__(
        self,
        **kwargs,
    ):
        super().__init__(**kwargs)

    def _get_last_runs(self) -> list[Any]:
        template = self._template_env.get_template("dqd/get_last_dqd_runs.sql.jinja")
        sql = template.render(
            dataset_dqd=self._dataset_dqd,
        )
        rows = self._gcp.run_query_job(sql)
        return [dict(row.items()) for row in rows]

    def _get_run(self, id: str) -> Any:
        template = self._template_env.get_template("dqd/get_dqd_run.sql.jinja")
        sql = template.render(
            dataset_dqd=self._dataset_dqd,
        )
        rows = self._gcp.run_query_job(
            sql,
            query_parameters=[bq.ScalarQueryParameter("id", "STRING", id)],
        )
        return dict(next(rows))

    def _get_results(self, run_id: str) -> pl.DataFrame:
        template = self._template_env.get_template("dqd/get_dqd_run_results.sql.jinja")
        sql = template.render(
            dataset_dqd=self._dataset_dqd,
        )
        rows = self._gcp.run_query_job(
            sql,
            query_parameters=[bq.ScalarQueryParameter("id", "STRING", run_id)],
        )
        data_frame = pl.from_arrow(rows.to_arrow())
        return cast(pl.DataFrame, data_frame)
