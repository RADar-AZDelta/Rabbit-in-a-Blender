# Copyright 2024 RADar-AZDelta
# SPDX-License-Identifier: gpl3+

import logging
from pathlib import Path

import google.cloud.bigquery as bq

from ..import_vocabularies import ImportVocabularies
from .etl_base import BigQueryEtlBase


class BigQueryImportVocabularies(ImportVocabularies, BigQueryEtlBase):
    """
    Class that imports the downloaded vocabulary zip from the Athena website.
    """

    def __init__(
        self,
        **kwargs,
    ):
        super().__init__(**kwargs)

    def _pre_load(self):
        """Stuff to do before the load (ex remove constraints from omop tables)"""
        pass

    def _post_load(self):
        """Stuff to do after the load (ex re-add constraints to omop tables)"""
        pass

    def _load_vocabulary_parquet_in_upload_table(self, vocabulary_table: str, parquet_file: Path) -> None:
        """Loads the CSV file in the specific standardised vocabulary table

        Args:
            vocabulary_table (str): The standardised vocabulary table
            parquet_file (Path): Path to the CSV file
        """
        logging.debug("Loading '%s' into vocabulary table %s", parquet_file, vocabulary_table)
        # upload the Parquet file to the Cloud Storage Bucket
        uri = self._gcp.upload_file_to_bucket(parquet_file, self._bucket_uri)
        # load the uploaded Parquet file from the bucket into the specific standardised vocabulary table
        self._gcp.batch_load_from_bucket_into_bigquery_table(
            uri,
            self._dataset_work,
            vocabulary_table,
            write_disposition=bq.WriteDisposition.WRITE_EMPTY,
        )

    def _clear_vocabulary_upload_table(self, vocabulary_table: str) -> None:
        """Removes a specific standardised vocabulary table

        Args:
            vocabulary_table (str): The standardised vocabulary table
        """
        logging.debug("Deleting vocabulary table %s", vocabulary_table)
        self._gcp.delete_table(self._dataset_work, vocabulary_table)

    def _refill_vocabulary_table(self, vocabulary_table: str) -> None:
        """Recreates a specific standardised vocabulary table from the upload table

        Args:
            vocabulary_table (str): The standardised vocabulary table
        """
        template = self._template_env.get_template("vocabulary/vocabulary_table_refill.sql.jinja")
        sql = template.render(
            dataset_omop=self._dataset_omop,
            dataset_work=self._dataset_work,
            vocabulary_table=vocabulary_table,
        )
        self._gcp.run_query_job(sql)
