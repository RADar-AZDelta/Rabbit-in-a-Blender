# Copyright 2024 RADar-AZDelta
# SPDX-License-Identifier: gpl3+

import logging

from sqlalchemy import text

from ..create_omop_db import CreateOmopDb
from .etl_base import SqlServerEtlBase

# pylint: disable=too-few-public-methods


class SqlServerCreateOmopDb(CreateOmopDb, SqlServerEtlBase):
    def __init__(
        self,
        **kwargs,
    ):
        super().__init__(**kwargs)

    def _run_cdm_ddl_query(self, ddl_part: str) -> None:
        """Runs a specific ddl query"""
        logging.info(f"Running DDL (Data Definition Language) query: OMOPCDM_{self._db_engine}_5.4_{ddl_part}.sql")
        template = self._template_env.get_template(f"ddl/OMOPCDM_{self._db_engine}_5.4_{ddl_part}.sql.jinja")
        sql = template.render(
            omop_database_catalog=self._omop_database_catalog,
            omop_database_schema=self._omop_database_schema,
        )
        # with self._db_connect() as conn:
        #     cursor = conn.cursor()
        #     cursor.execute(sql)
        #     conn.commit()
        with self._engine.connect() as conn:
            conn.execute(text(sql))
            conn.commit()

    def _run_source_id_to_omop_id_map_table_ddl_query(self) -> None:
        """Creates the source_id_to_omop_id_map table"""
        logging.info("Running DDL (Data Definition Language) query: SOURCE_ID_TO_OMOP_ID_MAP_ddl.sql")
        template = self._template_env.get_template("ddl/SOURCE_ID_TO_OMOP_ID_MAP_ddl.sql.jinja")
        sql = template.render(
            omop_database_catalog=self._omop_database_catalog,
            omop_database_schema=self._omop_database_schema,
        )
        with self._engine.connect() as conn:
            conn.execute(text(sql))
            conn.commit()

    def _run_dqd_ddl_query(self) -> None:
        """Creates the Data Quality Dashboard tables"""
        logging.info("Running DDL (Data Definition Language) query: DataQualityDashboard_ddl.sql")
        template = self._template_env.get_template("ddl/DataQualityDashboard_ddl.sql.jinja")
        sql = template.render(
            dqd_database_catalog=self._dqd_database_catalog,
            dqd_database_schema=self._dqd_database_schema,
        )
        with self._engine.connect() as conn:
            conn.execute(text(sql))
            conn.commit()
