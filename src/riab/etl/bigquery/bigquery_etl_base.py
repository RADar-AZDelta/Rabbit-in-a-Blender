# Copyright 2024 RADar-AZDelta
# SPDX-License-Identifier: gpl3+

# pylint: disable=unsubscriptable-object
"""Holds the BigQuery ETL base class"""
import logging
from abc import ABC
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import List, Optional, cast

import google.auth
import google.cloud.bigquery as bq
import jinja2 as jj
import pyarrow as pa
import pyarrow.parquet as pq
from jinja2.utils import select_autoescape

from ..etl_base import EtlBase
from .gcp import Gcp


class BigQueryEtlBase(EtlBase, ABC):
    def __init__(
        self,
        credentials_file: Optional[str],
        location: Optional[str],
        project_raw: Optional[str],
        dataset_work: str,
        dataset_omop: str,
        dataset_dqd: str,
        dataset_achilles: str,
        bucket: str,
        **kwargs,
    ):
        """This class holds the BigQuery specific methods of the ETL process

        Args:
            credentials_file (Optional[str]): The credentials file must be a service account key, stored authorized user credentials, external account credentials, or impersonated service account credentials. (see https://google-auth.readthedocs.io/en/master/reference/google.auth.html#google.auth.load_credentials_from_file), Alternatively, you can also use 'Application Default Credentials' (ADC) (see https://cloud.google.com/sdk/gcloud/reference/auth/application-default/login)
            location (Optional[str]): Location where to run the BigQuery jobs. Must match the location of the datasets used in the query. (important for GDPR)
            project_raw (Optional[str]): Can be handy if you use jinja templates for your ETL queries (ex if you are using development-staging-production environments). Must have the following format: PROJECT_ID
            dataset_work (str): The dataset that will hold RiaB's housekeeping tables. Must have the following format: PROJECT_ID.DATASET_ID
            dataset_omop (str): The dataset that will hold the OMOP table. Must have the following format: PROJECT_ID.DATASET_ID
            bucket (str): The Cloud Storage bucket uri, that will hold the uploaded Usagi and custom concept files. (the uri has format 'gs://{bucket_name}/{bucket_path}')
        """
        super().__init__(**kwargs)

        if credentials_file:
            credentials, project_id = google.auth.load_credentials_from_file(credentials_file)
        else:
            credentials, project_id = google.auth.default()

        # if not project_id:
        #     project_id = project

        self._gcp = Gcp(credentials=credentials, location=location or "EU")
        self._project_raw = cast(str, project_raw)
        self._dataset_work = dataset_work
        self._dataset_omop = dataset_omop
        self._dataset_dqd = dataset_dqd
        self._dataset_achilles = dataset_achilles
        self._bucket_uri = bucket

        template_dir = Path(__file__).resolve().parent / "templates"
        template_loader = jj.FileSystemLoader(searchpath=template_dir)
        self._template_env = jj.Environment(autoescape=select_autoescape(["sql"]), loader=template_loader)

    def __del__(self):
        logging.info("Total BigQuery cost: %s€", self._gcp.total_cost)
        EtlBase.__del__(self)

    def _get_omop_column_names(self, omop_table_name: str) -> List[str]:
        """Get list of column names of a omop table.

        Args:
            omop_table_name (str): OMOP table

        Returns:
            List[str]: list of column names
        """
        template = self._template_env.get_template("get_table_columns.sql.jinja")
        sql = template.render(dataset=self._dataset_omop, table_name=omop_table_name)
        columns = self._gcp.run_query_job(sql)
        return [column["column_name"] for column in columns]

    def _get_required_omop_column_names(self, omop_table_name: str) -> List[str]:
        """Get list of required column names of a omop table.

        Args:
            omop_table_name (str): OMOP table

        Returns:
            List[str]: list of column names
        """
        template = self._template_env.get_template("get_table_columns.sql.jinja")
        sql = template.render(dataset=self._dataset_omop, table_name=omop_table_name)
        columns = self._gcp.run_query_job(sql)
        return [column["column_name"] for column in columns if column["is_nullable"] == "NO"]

    def _get_all_table_names(self, dataset: str) -> List[str]:
        """Get all table names from a specific dataset in Big Query

        Args:
            dataset (str): dataset (format: PROJECT_ID.DATASET_ID)

        Returns:
            List[str]: list of table names
        """
        template = self._template_env.get_template("all_table_names.sql.jinja")
        sql = template.render(dataset=dataset)
        rows = self._gcp.run_query_job(sql)
        return [row.table_name for row in rows]

    def _upload_arrow_table(self, table: pa.Table, table_name: str):
        with TemporaryDirectory(prefix="riab_") as tmp_dir:
            tmp_file = str(Path(tmp_dir) / f"{table_name}.parquet")
            logging.debug("Writing arrow table to parquet file '{tmp_file}'")
            pq.write_table(table, where=tmp_file)

            # upload the Parquet file to the Cloud Storage Bucket
            uri = self._gcp.upload_file_to_bucket(tmp_file, self._bucket_uri)
            # load the uploaded Parquet file from the bucket into the specific standardised vocabulary table
            self._gcp.batch_load_from_bucket_into_bigquery_table(
                uri,
                self._dataset_omop,
                table_name,
                write_disposition=bq.WriteDisposition.WRITE_APPEND,
            )
