# Copyright 2024 RADar-AZDelta
# SPDX-License-Identifier: gpl3+

"""This module holds the Gcp class,
Google Cloud Provider class with usefull methods for ETL"""

# pylint: disable=no-member
import logging
import math
import os
import time
from pathlib import Path
from threading import Lock
from typing import Optional, Sequence, Tuple, Union
from urllib.parse import urlparse

import google.cloud.bigquery as bq
import google.cloud.storage as cs
from google.auth.credentials import Credentials
from google.cloud.bigquery.schema import SchemaField
from google.cloud.bigquery.table import RowIterator, _EmptyRowIterator
from google.cloud.exceptions import NotFound
from requests.adapters import HTTPAdapter


class Gcp:
    """
    Google Cloud Provider class with usefull methods for ETL
    Local Query --> Parquet --> Cloud Storage --> Bigquery
    """

    _MEGA = 1024**2
    _GIGA = 1024**3
    _COST_PER_10_MB = 6 / 1024 / 1024 * 10

    def __init__(self, credentials: Credentials, location: str = "EU"):
        """Constructor

        Args:
            credentials (Credentials): The Google auth credentials (see https://google-auth.readthedocs.io/en/stable/reference/google.auth.credentials.html)
            location (str): The location in GCP (see https://cloud.google.com/about/locations/)
        """  # noqa: E501 # pylint: disable=line-too-long
        logging.debug("Creating Google Cloud Storage client")
        self._cs_client = cs.Client(credentials=credentials)
        logging.debug("Creating BigQuery client")
        self._bq_client = bq.Client(credentials=credentials)
        self._location = location
        self._total_cost = 0
        self._lock_total_cost = Lock()

        # increase connection pool size
        adapter = HTTPAdapter(pool_connections=128, pool_maxsize=128, max_retries=3)
        self._cs_client._http.mount("https://", adapter)
        self._cs_client._http._auth_request.session.mount("https://", adapter)
        self._bq_client._http.mount("https://", adapter)
        self._bq_client._http._auth_request.session.mount("https://", adapter)

    @property
    def total_cost(self):
        """Gets the total BigQuery cost

        Returns:
            float: total cost in €
        """
        return self._total_cost

    def run_query_job(
        self,
        query: str,
        query_parameters: Union[list[bq.ScalarQueryParameter], None] = None,
    ) -> Union[RowIterator, _EmptyRowIterator]:
        """Runs a query with or without parameters on Big Query
        Calculates and logs the billed cost of the query

        Args:
            query (str): the sql query
            query_parameters (list[bigquery.ScalarQueryParameter], optional): the query parameters

        Returns:
            RowIterator: row iterator
        """  # noqa: E501 # pylint: disable=line-too-long

        try:
            result, execution_time = self.run_query_job_with_benchmark(query, query_parameters)
            return result
        except Exception as e:
            raise Exception(
                f"Failed to run query!\n\nQuery parameters: {str(query_parameters)}\n\nQuery:\n{query}"
            ) from e

    def run_query_job_with_benchmark(
        self,
        query: str,
        query_parameters: Union[list[bq.ScalarQueryParameter], None] = None,
    ) -> Tuple[Union[RowIterator, _EmptyRowIterator], float]:
        """Runs a query with or without parameters on Big Query
        Calculates and logs the billed cost of the query

        Args:
            query (str): the sql query
            query_parameters (list[bigquery.ScalarQueryParameter], optional): the query parameters

        Returns:
            RowIterator: row iterator
        """  # noqa: E501 # pylint: disable=line-too-long
        try:
            job_config = bq.QueryJobConfig(
                query_parameters=query_parameters or [],
            )
            logging.debug("Running query: %s\nWith parameters: %s", query, str(query_parameters))
            start = time.time()
            query_job = self._bq_client.query(query, job_config=job_config, location=self._location)
            result = query_job.result()
            end = time.time()
            # cost berekening $6.00 per TB (afgerond op 10 MB naar boven)
            cost_per_10_mb = 6 / 1024 / 1024 * 10
            total_10_mbs_billed = math.ceil((query_job.total_bytes_billed or 0) / (Gcp._MEGA * 10))
            cost = total_10_mbs_billed * cost_per_10_mb
            execution_time = end - start

            self._lock_total_cost.acquire()
            try:
                self._total_cost += cost
            finally:
                self._lock_total_cost.release()

            logging.debug(
                "Query processed %.2f MB (%.2f MB billed) in %.2f seconds" " (%.2f seconds slot time): %.8f $ billed",
                (query_job.total_bytes_processed or 0) / Gcp._MEGA,
                (query_job.total_bytes_billed or 0) / Gcp._MEGA,
                execution_time,
                (query_job.slot_millis or 0) / 1000,
                cost,
            )
            # if execution_time > 60:
            #     logging.warning(
            #         "Long query time (%.2f seconds) for query: %s",
            #         execution_time,
            #         query,
            #     )
            return result, execution_time
        except Exception as ex:
            logging.debug("FAILED QUERY: %s\nWith parameters: %s", query, str(query_parameters))
            raise ex

    def delete_table(self, dataset: str, table_name: str):
        """Delete a table from BigQuery
        see https://cloud.google.com/bigquery/docs/samples/bigquery-delete-table#bigquery_delete_table-python

        Args:
            dataset (str): dataset (format: PROJECT_ID.DATASET_ID)
            table_name (str): table name
        """  # noqa: E501 # pylint: disable=line-too-long
        logging.debug("Dropping BigQuery table '%s.%s'", dataset, table_name)
        dataset_parts = dataset.split(".")
        table = self._bq_client.dataset(dataset_parts[1], dataset_parts[0]).table(table_name)
        self._bq_client.delete_table(table, not_found_ok=True)

    def delete_from_bucket(self, bucket_uri: str):
        """Delete a blob from a Cloud Storage bucket
        see https://cloud.google.com/storage/docs/deleting-objects#code-samples

        Args
            bucket (str): The bucket uri
        """
        try:
            scheme, netloc, path, params, query, fragment = urlparse(bucket_uri)
            logging.debug("Delete path '%s' from bucket '%s", netloc, path)
            bucket = self._cs_client.bucket(netloc)
            blobs = bucket.list_blobs(prefix=path.lstrip("/"))
            for blob in blobs:
                blob.delete()
        except NotFound:
            pass

    def upload_file_to_bucket(self, source_file_path: Union[str, Path], bucket_uri: str):
        """Upload a local file to a Cloud Storage bucket
        see https://cloud.google.com/storage/docs/uploading-objects

        Args:
            source_file_path (Path): Path to the local file
            bucket_uri (str): Name of the Cloud Storage bucket and the path in the bucket (directory) to store the file (with format: 'gs://{bucket_name}/{bucket_path}')
        """  # noqa: E501 # pylint: disable=line-too-long
        logging.debug(
            "Upload file '%s' to bucket '%s'",
            str(source_file_path),
            bucket_uri,
        )
        scheme, netloc, path, params, query, fragment = urlparse(bucket_uri)
        bucket = self._cs_client.bucket(netloc)
        filename_w_ext = Path(source_file_path).name
        blob_name = os.path.join(path.lstrip("/"), filename_w_ext)
        blob = bucket.blob(blob_name)
        blob.upload_from_filename(str(source_file_path))
        return f"{bucket_uri}/{filename_w_ext}"  # urljoin doesn't work with protocol gs

    def batch_load_from_bucket_into_bigquery_table(
        self,
        uri: str,
        dataset: str,
        table_name: str,
        write_disposition: str = bq.WriteDisposition.WRITE_APPEND,
        schema: Optional[Sequence[SchemaField]] = None,
    ):
        """Batch load parquet files from a Cloud Storage bucket to a Big Query table
        see https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-parquet#python

        Args:
            uri (str): the uri of the bucket blob(s) in the form of 'gs://{bucket_name}/{bucket_path}/{blob_name(s)}.parquet'
            dataset (str): dataset (format: PROJECT_ID.DATASET_ID)
            table_name (str): table name
        """  # noqa: E501 # pylint: disable=line-too-long
        logging.debug(
            "Append bucket files '%s' to BigQuery table '%s.%s'",
            uri,
            dataset,
            table_name,
        )
        dataset_parts = dataset.split(".")
        table = self._bq_client.dataset(dataset_parts[1], dataset_parts[0]).table(table_name)
        job_config = bq.LoadJobConfig(
            write_disposition=write_disposition,
            schema_update_options=bq.SchemaUpdateOption.ALLOW_FIELD_ADDITION
            if write_disposition == bq.WriteDisposition.WRITE_APPEND
            or write_disposition == bq.WriteDisposition.WRITE_TRUNCATE
            else None,
            source_format=bq.SourceFormat.PARQUET,
            schema=schema,
            autodetect=False if schema else True,
        )
        load_job = self._bq_client.load_table_from_uri(uri, table, job_config=job_config)  # Make an API request.
        load_job.result()  # Waits for the job to complete.

        table = self._bq_client.get_table(bq.DatasetReference(dataset_parts[0], dataset_parts[1]).table(table_name))
        logging.debug(
            "Loaded %i rows into '%s.%s'",
            table.num_rows,
            dataset,
            table_name,
        )
