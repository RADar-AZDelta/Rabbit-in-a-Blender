# Copyright 2022 RADar-AZDelta
# SPDX-License-Identifier: gpl3+

from typing import Any, List, cast

import google.cloud.bigquery as bq
import polars as pl

from ..data_quality_dashboard import DataQualityDashboard
from .bigquery_etl_base import BigQueryEtlBase


class BigQueryDataQualityDashboard(DataQualityDashboard, BigQueryEtlBase):
    def __init__(
        self,
        **kwargs,
    ):
        super().__init__(**kwargs)

    def _get_last_runs(self) -> List[Any]:
        template = self._template_env.from_string(
            """
select FORMAT_TIMESTAMP("%Y-%m-%d %T", startTimestamp) as label, id as value
from {{project_id}}.{{dataset_id_omop}}.dqdashboard_runs
order by startTimestamp desc
limit 15;
"""
        )
        sql = template.render(
            project_id=self._project_id,
            dataset_id_omop=self._dataset_id_omop,
        )
        rows = self._gcp.run_query_job(sql)
        return [dict(row.items()) for row in rows]

    def _get_run(self, id: str) -> Any:
        template = self._template_env.from_string(
            """
select *
from {{project_id}}.{{dataset_id_omop}}.dqdashboard_runs
where id = @id
"""
        )
        sql = template.render(
            project_id=self._project_id,
            dataset_id_omop=self._dataset_id_omop,
        )
        rows = self._gcp.run_query_job(
            sql,
            query_parameters=[bq.ScalarQueryParameter("id", "STRING", id)],
        )
        return dict(next(rows))

    def _get_results(self, run_id: str) -> pl.DataFrame:
        template = self._template_env.from_string(
            """
select *
from {{project_id}}.{{dataset_id_omop}}.dqdashboard_results
where run_id = @id
"""
        )
        sql = template.render(
            project_id=self._project_id,
            dataset_id_omop=self._dataset_id_omop,
        )
        rows = self._gcp.run_query_job(
            sql,
            query_parameters=[bq.ScalarQueryParameter("id", "STRING", run_id)],
        )
        data_frame = pl.from_arrow(rows.to_arrow())
        return cast(pl.DataFrame, data_frame)
