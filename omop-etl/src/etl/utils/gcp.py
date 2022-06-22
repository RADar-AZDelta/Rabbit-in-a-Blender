"""This module holds the Gcp class,
Google Cloud Provider class with usefull methods for ETL"""
# pylint: disable=no-member
import logging
import math
import os
import tempfile
import time
from copy import deepcopy
from typing import Any, Iterable, List, Tuple, Union

import backoff
import connectorx as cx
import google.cloud.bigquery as bq
import google.cloud.storage as cs
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
from google.auth.credentials import Credentials
from google.cloud.bigquery.table import RowIterator, _EmptyRowIterator
from google.cloud.exceptions import Conflict, NotFound


class Gcp:
    """
    Google Cloud Provider class with usefull methods for ETL
    Local Query --> Parquet --> Cloud Storage --> Bigquery
    """

    _MEGA = math.pow(1024, 2)
    _GIGA = math.pow(1024, 3)

    def __init__(
        self,
        cs_credentials: Union[Credentials, None],
        bq_credentials: Union[Credentials, None],
    ):
        """Constructor

        Args:
            cs_credentials (Credentials): The Cloud Storage credentials see https://googleapis.dev/python/storage/latest/client.html
            bq_credentials (Credentials): The Big Query credentials see https://googleapis.dev/python/bigquery/latest/index.html
        """  # noqa: E501 # pylint: disable=line-too-long
        if cs_credentials:
            self._cs_client = cs.Client(credentials=cs_credentials)
        if bq_credentials:
            self._bq_client = bq.Client(credentials=bq_credentials)
        self.__session_id = None

    @property
    def _session_id(self) -> Union[str, None]:
        if not self.__session_id:
            self.__session_id = self._create_session()
        return self.__session_id

    def create_dataset(self, project_id: str, dataset_id: str) -> bq.Dataset:
        """Create dataset if not yet exists in 'europe-west1' loaction.

        Args:
            project_id (str): project ID
            dataset_id (str): dataset ID

        Returns:
            Dataset: the created or existing dataset
        """
        dataset = bq.Dataset(f"{project_id}.{dataset_id}")
        dataset.location = "europe-west1"
        return self._bq_client.create_dataset(dataset, exists_ok=True)

    def create_bucket(self, bucket_name: str):
        """Create bucket if not yet exists in 'europe-west1' loaction.
        see https://googleapis.dev/python/storage/latest/client.html?highlight=create_bucket#google.cloud.storage.client.Client.create_bucket

        Args:
            bucket_name (str): The bucket resource to pass or name to create.
        """  # noqa: E501 # pylint: disable=line-too-long
        try:
            self._cs_client.create_bucket(bucket_name, location="europe-west1")
        except Conflict:
            pass

    def get_table_names(self, project_id: str, dataset_id: str) -> List[str]:
        """Get all table names from a specific dataset in Big Query

        Args:
            project_id (str): project ID
            dataset_id (str): dataset ID

        Returns:
            List[str]: list of table names
        """
        query = f"""
SELECT table_name
FROM `{project_id}.{dataset_id}.INFORMATION_SCHEMA.COLUMNS`
ORDER BY ordinal_position"""
        rows = self.run_query_job(query)
        return [row.table_name for row in rows]

    def get_column_names(
        self, project_id: str, dataset_id: str, table_name: str
    ) -> List[str]:
        """Get all column names of a specific table in a Big Query dataset

        Args:
            project_id (str): project ID
            dataset_id (str): dataset ID
            table_name (str): table name

        Returns:
            List[str]: list of column names
        """
        query = f"""
SELECT column_name
FROM `{project_id}.{dataset_id}.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = @table_name
ORDER BY ordinal_position"""
        query_parameters = [
            bq.ScalarQueryParameter("table_name", "STRING", table_name)
        ]
        rows = self.run_query_job(query, query_parameters)
        return [row.column_name for row in rows]

    def get_column_metadata(
        self, project_id: str, dataset_id: str, table_name: str, column_name: str
    ) -> Any:
        """Get metadata from a specific column in a table in a Big Query dataset

        Args:
            project_id (str): project ID
            dataset_id (str): dataset ID
            table_name (str): table name
            column_name (str): column name

        Returns:
            Any: the column metadata
        """
        query = f"""
SELECT *
FROM `{project_id}.{dataset_id}.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = @table_name and column_name = @column_name
ORDER BY ordinal_position"""
        query_parameters = [
            bq.ScalarQueryParameter("table_name", "STRING", table_name),
            bq.ScalarQueryParameter("column_name", "STRING", column_name),
        ]
        rows = self.run_query_job(query, query_parameters)
        first_row = next(iter(rows), None)
        return first_row

    def run_query_job(
        self,
        query: str,
        query_parameters: Union[List[bq.ScalarQueryParameter], None] = None,
    ) -> (Union[RowIterator, _EmptyRowIterator]):
        """Runs a query with or without parameters on Big Query
        Calculates and logs the billed cost of the query

        Args:
            query (str): the sql query
            query_parameters (List[bigquery.ScalarQueryParameter], optional): the query parameters

        Returns:
            RowIterator: row iterator
        """  # noqa: E501 # pylint: disable=line-too-long
        job_config = bq.QueryJobConfig(
            query_parameters=query_parameters or [],
            connection_properties=[
                bq.ConnectionProperty("session_id", str(self._session_id)),
            ],
        )
        logging.info("Running query: %s", query)
        start = time.time()
        query_job = self._bq_client.query(
            query, job_config=job_config, location="europe-west1"
        )
        result = query_job.result()
        end = time.time()
        # cost berekening $6.00 per TB (afgerond op 10 MB naar boven)
        cost_per_10_mb = 6 / 1024 / 1024 * 10
        total_10_mbs_billed = math.ceil(
            (query_job.total_bytes_billed or 0) / (Gcp._MEGA * 10)
        )
        cost = total_10_mbs_billed * cost_per_10_mb
        logging.info(
            "Query processed %.2f MB (%.2f MB billed) in %.2f seconds"
            " (%.2f seconds slot time): %.8f $ billed",
            (query_job.total_bytes_processed or 0) / Gcp._MEGA,
            (query_job.total_bytes_billed or 0) / Gcp._MEGA,
            end - start,
            (query_job.slot_millis or 0) / 1000,
            cost,
        )
        return result

    def _create_session(self) -> Union[str, None]:
        job_config = bq.QueryJobConfig()
        job_config.create_session = True
        query = "select true"
        query_job = self._bq_client.query(
            query, job_config=job_config, location="europe-west1"
        )
        query_job.result()
        return (
            query_job.session_info.session_id
            if query_job.session_info and query_job.session_info.session_id
            else None
        )

    def delete_table(self, table_id: str):
        """Delete a table from Big Query
        see https://cloud.google.com/bigquery/docs/samples/bigquery-delete-table#bigquery_delete_table-python

        Args:
            table_id (str): the id of the table in the form of '{project_id}.{dataset_id}.{table_name}'
        """  # noqa: E501 # pylint: disable=line-too-long
        logging.info("Dropping BigQuery table %s", table_id)
        self._bq_client.delete_table(table_id, not_found_ok=True)

    def delete_from_bucket(self, bucket_name: str, blob_name: str):
        """Delete a blob from a Cloud Storage bucket
        see https://cloud.google.com/storage/docs/deleting-objects#code-samples

        Args:
            bucket_name (str): The name of the bucket
            blob_name (str): The name of the blob
        """
        try:
            logging.info("Delete path '%s' from bucket '%s", blob_name, bucket_name)
            bucket = self._cs_client.bucket(bucket_name)
            blobs = bucket.list_blobs(prefix=blob_name)
            for blob in blobs:
                blob.delete()
        except NotFound:
            pass

    def upload_file_to_bucket(
        self, source_file_path: str, bucket_name: str, bucket_path: str
    ):
        """Upload a local file to a Cloud Storage bucket
        see https://cloud.google.com/storage/docs/uploading-objects

        Args:
            source_file_path (str): Path to the local file
            bucket_name (str): Name of the Cloud Storage bucket
            bucket_path (str): The path in the bucket (directory) to store the file
        """  # noqa: E501 # pylint: disable=line-too-long
        logging.info(
            "Upload file '%s' to bucket '%s'",
            source_file_path,
            f"{bucket_name}/{bucket_path}",
        )
        bucket = self._cs_client.bucket(bucket_name)
        filename_w_ext = os.path.basename(source_file_path)
        blob = bucket.blob(f"{bucket_path}/{filename_w_ext}")
        blob.upload_from_filename(source_file_path)

    def batch_load_from_bucket_into_bigquery_table(self, uri: str, table_id: str):
        """Batch load parquet files from a Cloud Storage bucket to a Big Query table
        see https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-parquet#python

        Args:
            uri (str): the uri of the bucket blob(s) in the form of 'gs://{bucket_name}/{bucket_path}/{blob_name(s)}.parquet'
            table_id (str): the id of the table in the form of '{project_id}.{dataset_id}.{table_name}'
        """  # noqa: E501 # pylint: disable=line-too-long
        logging.info("Append bucket files '%s' to BigQuery table '%s'", uri, table_id)
        job_config = bq.LoadJobConfig(
            write_disposition=bq.WriteDisposition.WRITE_APPEND,
            schema_update_options="ALLOW_FIELD_ADDITION",
            source_format=bq.SourceFormat.PARQUET,
        )
        load_job = self._bq_client.load_table_from_uri(
            uri, table_id, job_config=job_config
        )  # Make an API request.
        load_job.result()  # Waits for the job to complete.

        destination_table = self._bq_client.get_table(table_id)
        logging.info("Loaded %i rows into '%s'", destination_table.num_rows, table_id)

    def write_parquet(self, local_file_path: str, table: pa.Table):
        """Write Arrow table to local parquet file

        Args:
            local_file_path (str): Path to the local parquet file to write to
            table (pa.Table): The Arrow table
        """
        logging.info("Writing parquet files to: '%s'", local_file_path)
        pq.write_table(table, local_file_path)

    @backoff.on_exception(
        backoff.expo,
        (Exception),
        max_time=600,
        max_tries=20,
        giveup=lambda e: isinstance(e, RuntimeError) and "Token error" in str(e),
    )
    def load_local_query_result(self, conn: str, query: str) -> Tuple[pa.Table, int]:
        """Executes a local query and loads the results in an Arrow table
        see https://arrow.apache.org/docs/python/generated/pyarrow.Table.html#pyarrow.Table

        Args:
            conn (str): The connection string
            query (str): The SQL statement of the select query

        Returns:
            Table: Memory efficient Arrow table, with the query results
        """
        logging.info("Running query '%s", query)
        start = time.time()
        table: pa.Table = cx.read_sql(conn, query, return_type="arrow")
        end = time.time()
        table_size = table.nbytes
        logging.info(
            "Query returned %i rows with table size %.2f MB in %.2f seconds",
            table.num_rows,
            table.nbytes / Gcp._MEGA,
            end - start,
        )
        return deepcopy(table), table_size

    def load_local_query_result_into_data_frame(
        self, conn: str, query: str
    ) -> Union[pl.DataFrame, pl.Series]:
        """Executes a local query and loads the results in a lightning-fast Polars DataFrame
        see https://pola-rs.github.io/polars/py-polars/html/reference/dataframe.html

        Args:
            conn (str): The connection string
            query (str): The SQL statement of the select query

        Returns:
            (DataFrame | Series): Memory efficient Arrow table, with the query results
        """
        table, _ = self.load_local_query_result(conn, query)
        return pl.from_arrow(table)

    def upload_local_queries_to_bigquery_table(
        self,
        conn_str: str,
        queries: Iterable[str],
        bucket_name: str,
        bucket_path: str,
        project_id: str,
        dataset_id: str,
        table_name: str,
        drop_bq_table: bool = True,
        delete_files_from_bucket: bool = True,
        query_to_parquet_and_upload_to_cs: bool = True,
        upload_to_bq: bool = True,
    ):
        """Uploads one or multiple queries to a Big Query table.
        First it stores the query result localy in on or more Parquet files.
        Secondly it uploads the Parquet files to a Cloud Storage bucket.
        Finaly a load job loads the Parquet files in a Big Query table.

        Args:
            conn_str (str): Connection string to the local database
            queries (Iterable[str]): Iterator with the query or multiple queries
            bucket_name (str): Name of the Cloud Storage bucket
            bucket_path (str): The path in the bucket (directory) to store the Parquet file(s). The Parquet files will be stored in a folder with the 'table_name' as name.
            project_id (str): project ID
            dataset_id (str): dataset ID
            table_name (str): table name
            drop_bq_table (bool, optional): Truncate the Big Query table up front? Defaults to True.
            delete_files_from_bucket (bool, optional): Remove all the parquet files from the bucket path up front? Defaults to False.
            query_to_parquet_and_upload_to_cs (bool, optional): Do the local query to Parquet file(s) and upload them to the bucket? Defaults to True.
            upload_to_bq (bool, optional): Load the Parquet files in the Big Query table? Defaults to True.
        """  # noqa: E501 # pylint: disable=line-too-long
        if drop_bq_table:
            self.delete_table(f"{project_id}.{dataset_id}.{table_name}")
        # delete the parquet file from the bucket
        if delete_files_from_bucket:
            self.delete_from_bucket(bucket_name, f"{bucket_path}/{table_name}/")

        # create temporary dir to store parquet files before uploading
        if query_to_parquet_and_upload_to_cs:
            with tempfile.TemporaryDirectory() as temp_dir_path:
                counter = 0
                table = None
                file_name = None
                table_size = 0
                for idx, query in enumerate(queries):
                    file_name = f"{table_name}_part{counter}.parquet"
                    temp_table, temp_size = self.load_local_query_result(
                        conn_str, query
                    )
                    table_size += temp_size
                    table = (
                        temp_table
                        if not table
                        else pa.concat_tables([table, temp_table])
                    )
                    logging.info("Total table size: %f MB", table_size / 1024 / 1024)
                    if table_size > Gcp._GIGA:
                        temp_file_path = os.path.join(temp_dir_path, file_name)
                        self.write_parquet(temp_file_path, table)
                        self.upload_file_to_bucket(
                            temp_file_path, bucket_name, f"{bucket_path}/{table_name}"
                        )
                        counter += 1
                        table = None
                        table_size = 0
                    logging.info("Queries processed: '%i'", idx)
                if table:
                    temp_file_path = os.path.join(temp_dir_path, str(file_name))
                    self.write_parquet(temp_file_path, table)
                    self.upload_file_to_bucket(
                        temp_file_path, bucket_name, f"{bucket_path}/{table_name}"
                    )

        # upload parquet file in bucket and append to Bigquery table
        if upload_to_bq:
            self.batch_load_from_bucket_into_bigquery_table(
                f"gs://{bucket_name}/{bucket_path}/{table_name}/*.parquet",
                f"{project_id}.{dataset_id}.{table_name}",
            )
