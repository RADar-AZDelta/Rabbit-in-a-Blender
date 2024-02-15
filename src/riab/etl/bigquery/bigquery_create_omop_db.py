# Copyright 2024 RADar-AZDelta
# SPDX-License-Identifier: gpl3+

import json
import logging
import re
from pathlib import Path
from threading import Lock
from typing import Dict, List

from simple_ddl_parser import DDLParser

from ..create_omop_db import CreateOmopDb
from .bigquery_etl_base import BigQueryEtlBase


class BigQueryDdl(BigQueryEtlBase):
    def __init__(
        self,
        **kwargs,
    ):
        super().__init__(**kwargs)

        self.__clustering_fields = None
        self.__parsed_ddl = None

        self._lock_ddl = Lock()

    @property
    def _ddl(self):
        with open(
            str(Path(__file__).parent.resolve() / "templates" / "ddl" / "OMOPCDM_bigquery_5.4_ddl.sql"),
            "r",
            encoding="UTF8",
        ) as file:
            ddl = file.read()

        ddl = re.sub(
            r"(?:create table @cdmDatabaseSchema)(\S*)",
            rf"create table if not exists {self._dataset_omop}\1",
            ddl,
        )
        ddl = re.sub(r".(?<!not )null", r"", ddl)
        ddl = re.sub(r"\"", r"", ddl)

        template = self._template_env.get_template("ddl/SOURCE_ID_TO_OMOP_ID_MAP_create.sql.jinja")
        ddl2 = template.render(
            dataset_omop=self._dataset_omop,
        )

        template = self._template_env.get_template("ddl/DataQualityDashboard_ddl.sql.jinja")
        ddl3 = template.render(
            dataset_dqd=self._dataset_dqd,
        )

        ddl += f"""
        
{ddl2}

{ddl3}
"""
        return ddl

    @property
    def _parsed_ddl(self) -> List[Dict]:
        """Holds the parsed DDL

        Returns:
            List[Dict]: the parsed DDL
        """
        if not self.__parsed_ddl:
            self._lock_ddl.acquire()
            try:
                self.__parsed_ddl = DDLParser(self._ddl).run(output_mode="sql")
            except Exception as ex:
                raise ex
            finally:
                self._lock_ddl.release()
        return self.__parsed_ddl

    @property
    def _clustering_fields(self) -> Dict[str, List[str]]:
        """The BigQuery clustering fields for every OMOP table

        Returns:
            Dict[str, List[str]]: A dictionary that holds for every OMOP table the clustering fields.
        """
        if not self.__clustering_fields:
            with open(
                str(
                    Path(__file__).parent.resolve()
                    / "templates"
                    / "ddl"
                    / "OMOPCDM_bigquery_5.4_clustering_fields.json"
                ),
                "r",
                encoding="UTF8",
            ) as file:
                self.__clustering_fields = json.load(file)
        return self.__clustering_fields


class BigQueryCreateOmopDb(CreateOmopDb, BigQueryDdl):
    def __init__(
        self,
        **kwargs,
    ):
        super().__init__(target_dialect="bigquery", **kwargs)

    def run(self) -> None:
        """Create OMOP tables in the omop-dataset in BigQuery and apply clustering"""
        logging.info("Creating OMOP CDM database from DDL (Data Definition Language)")

        self._gcp.run_query_job(self._ddl)

        for table, fields in self._clustering_fields.items():
            self._gcp.set_clustering_fields_on_table(self._dataset_omop, table, fields)
