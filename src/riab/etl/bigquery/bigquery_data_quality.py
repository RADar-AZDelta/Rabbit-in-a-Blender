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
        super().__init__(**kwargs)

    def _run_check_query(self, check: Any, row: str, item: Any) -> Any:
        sql = None
        result = None
        exception = None
        execution_time = -1
        try:
            template = self._template_env.get_template(f"dqd/{check.sqlFile}.jinja")

            cdmFieldName = None
            if hasattr(item, "cdmFieldName"):
                cdmFieldName = item.cdmFieldName.lower()
                cdmFieldName = re.sub(
                    r"domain_concept_id_", r"field_concept_id_", cdmFieldName
                )
                cdmFieldName = re.sub(
                    r"cost_domain_id", r"cost_field_concept_id", cdmFieldName
                )

            plausibleValueHigh = None
            if hasattr(item, "plausibleValueHigh"):
                plausibleValueHigh = item.plausibleValueHigh
                if plausibleValueHigh == "DATEADD(dd,1,GETDATE())":
                    plausibleValueHigh = "date_add(current_date(), interval 1 day)"
                elif plausibleValueHigh == "YEAR(GETDATE())+1":
                    plausibleValueHigh = "extract(year from current_date()) + 1"

            sql = template.render(
                project_id=self._project_id,
                dataset_id_omop=self._dataset_id_omop,
                cdmTableName=item.cdmTableName.lower()
                if hasattr(item, "cdmTableName")
                else None,
                cdmFieldName=cdmFieldName,
                cohortDefinitionId=item.cohortDefinitionId
                if hasattr(item, "cohortDefinitionId")
                else None,
                cdmSourceFieldName=item.cdmSourceFieldName.lower()
                if hasattr(item, "cdmSourceFieldName")
                else None,
                fkTableName=item.fkTableName.lower()
                if hasattr(item, "fkTableName")
                else None,
                fkFieldName=item.fkFieldName.lower()
                if hasattr(item, "fkFieldName")
                else None,
                standardConceptFieldName=item.standardConceptFieldName.lower()
                if hasattr(item, "standardConceptFieldName")
                else None,
                plausibleValueHigh=plausibleValueHigh,
                plausibleValueLow=item.plausibleValueLow
                if hasattr(item, "plausibleValueLow")
                else None,
                cdmDatatype=item.cdmDatatype.lower()
                if hasattr(item, "cdmDatatype")
                else None,
                plausibleTemporalAfterTableName=item.plausibleTemporalAfterTableName.lower()
                if hasattr(item, "plausibleTemporalAfterTableName")
                else None,
                plausibleTemporalAfterFieldName=item.plausibleTemporalAfterFieldName.lower()
                if hasattr(item, "plausibleTemporalAfterFieldName")
                else None,
                fkDomain=item.fkDomain.lower() if hasattr(item, "fkDomain") else None,
                fkClass=item.fkClass.lower() if hasattr(item, "fkClass") else None,
                conceptId=item.conceptId.lower()
                if hasattr(item, "conceptId")
                else None,
                unitConceptId=item.unitConceptId.lower()
                if hasattr(item, "unitConceptId")
                else None,
                plausibleUnitConceptIds=item.plausibleUnitConceptIds.lower()
                if hasattr(item, "plausibleUnitConceptIds")
                else None,
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
