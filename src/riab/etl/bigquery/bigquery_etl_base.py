# Copyright 2022 RADar-AZDelta
# SPDX-License-Identifier: gpl3+

# pylint: disable=unsubscriptable-object
"""Holds the BigQuery ETL base class"""
import json
import logging
import re
from abc import ABC
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Lock
from typing import Dict, List, Optional, cast

import google.auth
import google.cloud.bigquery as bq
import jinja2 as jj
import pyarrow as pa
import pyarrow.parquet as pq
from jinja2.utils import select_autoescape
from simple_ddl_parser import DDLParser

from ..etl_base import EtlBase
from .gcp import Gcp


class BigQueryEtlBase(EtlBase, ABC):
    def __init__(
        self,
        credentials_file: Optional[str],
        project_id: Optional[str],
        location: Optional[str],
        dataset_id_raw: str,
        dataset_id_work: str,
        dataset_id_omop: str,
        bucket_uri: str,
        **kwargs,
    ):
        super().__init__(**kwargs)

        if credentials_file:
            credentials, project = google.auth.load_credentials_from_file(
                credentials_file
            )
        else:
            credentials, project = google.auth.default()

        if not project_id:
            project_id = project

        self._gcp = Gcp(credentials=credentials, location=location or "EU")
        self._project_id = cast(str, project_id)
        self._dataset_id_raw = dataset_id_raw
        self._dataset_id_work = dataset_id_work
        self._dataset_id_omop = dataset_id_omop
        self._bucket_uri = bucket_uri

        template_dir = Path(__file__).resolve().parent / "templates"
        template_loader = jj.FileSystemLoader(searchpath=template_dir)
        self._template_env = jj.Environment(
            autoescape=select_autoescape(["sql"]), loader=template_loader
        )

        self.__clustering_fields = None
        self.__parsed_ddl = None

        self._lock_ddl = Lock()

    @property
    def _ddl(self):
        with open(
            str(
                Path(__file__).parent.resolve()
                / "templates"
                / "OMOPCDM_bigquery_5.4_ddl.sql"
            ),
            "r",
            encoding="UTF8",
        ) as file:
            ddl = file.read()

        ddl = re.sub(
            r"(?:create table @cdmDatabaseSchema)(\S*)",
            rf"create table if not exists {self._project_id}.{self._dataset_id_omop}\1",
            ddl,
        )
        ddl = re.sub(r".(?<!not )null", r"", ddl)
        ddl = re.sub(r"\"", r"", ddl)
        ddl = re.sub(r"domain_concept_id_", r"field_concept_id_", ddl)
        ddl = re.sub(r"cost_domain_id STRING", r"cost_field_concept_id INT64", ddl)

        template = self._template_env.get_template(
            "SOURCE_ID_TO_OMOP_ID_MAP_create.sql.jinja"
        )
        ddl2 = template.render(
            project_id=self._project_id,
            dataset_id_omop=self._dataset_id_omop,
        )

        ddl += f"""
        
{ddl2}"""
        return ddl

    @property
    def _parsed_ddl(self) -> List[Dict]:
        """Holds the parsed DDL

        Returns:
            List[Dict]: the parsed DDL
        """
        self._lock_ddl.acquire()
        if not self.__parsed_ddl:
            self.__parsed_ddl = DDLParser(self._ddl).run(output_mode="sql")
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
                    / "OMOPCDM_bigquery_5.4_clustering_fields.json"
                ),
                "r",
                encoding="UTF8",
            ) as file:
                self.__clustering_fields = json.load(file)
        return self.__clustering_fields

    def _get_column_names(self, omop_table_name: str) -> List[str]:
        """Get list of column names of a omop table.

        Args:
            omop_table_name (str): OMOP table

        Returns:
            List[str]: list of column names
        """
        columns = self._gcp.get_columns(
            self._project_id, self._dataset_id_omop, omop_table_name
        )
        return [column["column_name"] for column in columns]

    def _get_required_column_names(self, omop_table_name: str) -> List[str]:
        """Get list of required column names of a omop table.

        Args:
            omop_table_name (str): OMOP table

        Returns:
            List[str]: list of column names
        """
        columns = self._gcp.get_columns(
            self._project_id, self._dataset_id_omop, omop_table_name
        )
        return [
            column["column_name"] for column in columns if column["is_nullable"] == "NO"
        ]

    def _upload_arrow_table(self, table: pa.Table, table_name: str):
        with TemporaryDirectory(prefix="riab_") as tmp_dir:
            logging.debug("Writing DQD results to parquet")
            tmp_file = str(Path(tmp_dir) / f"{table_name}.parquet")
            pq.write_table(table, where=tmp_file)

            logging.debug("Loading DQD results parquet into BigQuery table")
            # upload the Parquet file to the Cloud Storage Bucket
            uri = self._gcp.upload_file_to_bucket(tmp_file, self._bucket_uri)
            # load the uploaded Parquet file from the bucket into the specific standardised vocabulary table
            self._gcp.batch_load_from_bucket_into_bigquery_table(
                uri,
                self._project_id,
                self._dataset_id_omop,
                table_name,
                write_disposition=bq.WriteDisposition.WRITE_APPEND,
            )
