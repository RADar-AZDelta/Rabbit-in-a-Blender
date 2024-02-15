# Copyright 2024 RADar-AZDelta
# SPDX-License-Identifier: gpl3+

import logging
import traceback
from time import time
from typing import Any, List

import pandas as pd
import pyarrow as pa

from ..data_quality import DataQuality
from .bigquery_etl_base import BigQueryEtlBase


class BigQueryDataQuality(DataQuality, BigQueryEtlBase):
    def __init__(
        self,
        **kwargs,
    ):
        super().__init__(target_dialect="bigquery", **kwargs)

    def _run_check_query(self, check: Any, row: str, item: Any) -> Any:
        sql = None
        result = None
        exception: str | None = None
        execution_time = -1
        try:
            parameters: dict = item.to_dict()

            parameters["cohort"] = "TRUE" if hasattr(parameters, "cohortDefinitionId") else "FALSE"
            parameters["cdmDatabaseSchema"] = f"{self._dataset_omop}"
            parameters["vocabDatabaseSchema"] = f"{self._dataset_omop}"

            sql = self._render_sqlfile(check.sqlFile, parameters)

            start = time()
            rows = self._gcp.run_query_job(sql)
            end = time()
            execution_time = end - start
            result = dict(next(rows))
        except Exception as ex:
            logging.warn(traceback.format_exc())
            exception = str(ex)
            # if __debug__:
            #     breakpoint()

        return self._process_check(check, row, item, sql, result, execution_time, exception)

    def _get_cdm_sources(self) -> List[Any]:
        """Merges the uploaded custom concepts in the OMOP concept table.

        Returns:
            RowIterator | _EmptyRowIterator: The result rows
        """
        template = self._template_env.from_string("select * from {{dataset_omop}}.cdm_source;")
        sql = template.render(
            dataset_id_omop=self._dataset_omop,
        )
        rows = self._gcp.run_query_job(sql)
        return [dict(row.items()) for row in rows]

    def _store_dqd_run(self, dqd_run: dict):
        table = pa.Table.from_pylist([dqd_run])
        self._upload_arrow_table(table, "dqdashboard_runs")

    def _store_dqd_result(self, dqd_result: pd.DataFrame):
        table = pa.Table.from_pandas(dqd_result)
        self._upload_arrow_table(table, "dqdashboard_results")
