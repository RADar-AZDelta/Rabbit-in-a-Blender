# Copyright 2022 RADar-AZDelta
# SPDX-License-Identifier: gpl3+

import logging

from ..create_omop_db import CreateOmopDb
from .bigquery_etl_base import BigQueryEtlBase


class BigQueryCreateOmopDb(CreateOmopDb, BigQueryEtlBase):
    def __init__(
        self,
        **kwargs,
    ):
        super().__init__(target_dialect="bigquery", **kwargs)

    def run(self) -> None:
        """Create OMOP tables in the omop-dataset in BigQuery and apply clustering"""
        logging.info("Creating OMOP CDM database from DDL (Data Definition Language)")

        #         template = self._template_env.get_template("DataQualityDashboard_ddl.sql.jinja")
        #         dqdashboard_ddl = template.render(
        #             project_id=self._project_id,
        #             dataset_id_omop=self._dataset_id_omop,
        #         )

        #         ddl = f"""{self._ddl}

        # {dqdashboard_ddl}"""

        self._gcp.run_query_job(self._ddl)

        for table, fields in self._clustering_fields.items():
            self._gcp.set_clustering_fields_on_table(
                self._project_id, self._dataset_id_omop, table, fields
            )
