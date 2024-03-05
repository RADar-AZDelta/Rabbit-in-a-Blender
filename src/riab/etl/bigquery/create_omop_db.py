# Copyright 2024 RADar-AZDelta
# SPDX-License-Identifier: gpl3+

import logging

from ..create_omop_db import CreateOmopDb
from .etl_base import BigQueryEtlBase


class BigQueryCreateOmopDb(CreateOmopDb, BigQueryEtlBase):
    def __init__(
        self,
        **kwargs,
    ):
        super().__init__(**kwargs)

    def _run_cdm_ddl_query(self, ddl_part: str) -> None:
        """Runs a specific ddl query"""
        match ddl_part:
            case "primary_keys" | "constraints" | "indices":
                return
        logging.info(f"Running DDL (Data Definition Language) query: OMOPCDM_{self._db_engine}_5.4_{ddl_part}.sql")

        template = self._template_env.get_template(f"ddl/OMOPCDM_{self._db_engine}_5.4_{ddl_part}.sql.jinja")
        sql = template.render(
            dataset_omop=self._dataset_omop,
        )
        self._gcp.run_query_job(sql)

    def _run_source_id_to_omop_id_map_table_ddl_query(self) -> None:
        """Creates the source_id_to_omop_id_map table"""
        logging.info("Running DDL (Data Definition Language) query: SOURCE_ID_TO_OMOP_ID_MAP_ddl.sql")
        template = self._template_env.get_template("ddl/SOURCE_ID_TO_OMOP_ID_MAP_ddl.sql.jinja")
        sql = template.render(
            dataset_omop=self._dataset_omop,
        )
        self._gcp.run_query_job(sql)

    def _run_dqd_ddl_query(self) -> None:
        """Creates the Data Quality Dashboard tables"""
        logging.info("Running DDL (Data Definition Language) query: DataQualityDashboard_ddl.sql")
        template = self._template_env.get_template("ddl/DataQualityDashboard_ddl.sql.jinja")
        sql = template.render(
            dataset_dqd=self._dataset_dqd,
        )
        self._gcp.run_query_job(sql)
