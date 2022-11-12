# Copyright 2022 RADar-AZDelta
# SPDX-License-Identifier: gpl3+

import logging
import re
import traceback
from time import time
from typing import Any, List

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
        exception = None
        execution_time = -1
        try:
            parameters = item.to_dict()

            parameters["cohort"] = (
                "TRUE" if hasattr(parameters, "cohortDefinitionId") else "FALSE"
            )
            parameters[
                "cdmDatabaseSchema"
            ] = f"{self._project_id}.{self._dataset_id_omop}"

            sql = self._render_sqlfile(
                check.sqlFile,
                list(parameters.keys()),
                list(parameters.values()),
            )

            start = time()
            rows = self._gcp.run_query_job(sql)
            end = time()
            execution_time = end - start
            result = dict(next(rows))
        except Exception as ex:
            logging.warn(traceback.format_exc())
            exception = ex
            # if __debug__:
            #     breakpoint()

        return self._process_check(
            check, row, item, sql, result, execution_time, exception
        )

    def _get_cdm_sources(self) -> List[Any]:
        """Merges the uploaded custom concepts in the OMOP concept table.

        Returns:
            RowIterator | _EmptyRowIterator: The result rows
        """
        template = self._template_env.get_template("dqd/cdm_sources.sql.jinja")
        sql = template.render(
            project_id=self._project_id,
            dataset_id_omop=self._dataset_id_omop,
        )
        rows = self._gcp.run_query_job(sql)
        return [dict(row.items()) for row in rows]
