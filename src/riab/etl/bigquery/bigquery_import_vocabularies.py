# Copyright 2024 RADar-AZDelta
# SPDX-License-Identifier: gpl3+

import logging
from pathlib import Path
from typing import List, Tuple

import google.cloud.bigquery as bq
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pcsv
import pyarrow.parquet as pq

from ..import_vocabularies import ImportVocabularies
from .bigquery_create_omop_db import BigQueryDdl


class BigQueryImportVocabularies(ImportVocabularies, BigQueryDdl):
    """
    Class that imports the downloaded vocabulary zip from the Athena website.
    """

    def __init__(
        self,
        **kwargs,
    ):
        super().__init__(**kwargs)

    def _load_vocabulary_in_upload_table(self, csv_file: Path, vocabulary_table: str) -> None:
        """Loads the CSV file in the specific standardised vocabulary table

        Args:
            csv_file (Path): Path to the CSV file
            vocabulary_table (str): The standardised vocabulary table
        """
        logging.debug("Converting '%s.csv' to parquet", vocabulary_table)
        tab = self._read_vocabulary_csv(vocabulary_table, csv_file)
        parquet_file = csv_file.parent / f"{vocabulary_table}.parquet"
        pq.write_table(tab, where=parquet_file)

        logging.debug("Loading '%s.parquet' into vocabulary table", vocabulary_table)
        # upload the Parquet file to the Cloud Storage Bucket
        uri = self._gcp.upload_file_to_bucket(parquet_file, self._bucket_uri)
        # load the uploaded Parquet file from the bucket into the specific standardised vocabulary table
        self._gcp.batch_load_from_bucket_into_bigquery_table(
            uri,
            self._dataset_work,
            vocabulary_table,
            write_disposition=bq.WriteDisposition.WRITE_EMPTY,  # , schema
        )

    def _clear_vocabulary_upload_table(self, vocabulary_table: str) -> None:
        """Removes a specific standardised vocabulary table

        Args:
            vocabulary_table (str): The standardised vocabulary table
        """
        logging.debug("Deleting vocabulary table %s", vocabulary_table)
        self._gcp.delete_table(self._dataset_work, vocabulary_table)

    def _read_vocabulary_csv(self, vocabulary_table: str, csv_file: Path) -> pa.Table:
        """Reads a specific standardised vocabulary table CSV file and converts it into an Arrow table

        Args:
            vocabulary_table (str): The standardised vocabulary table
            csv_file (Path): Path to the CSV file

        Returns:
            pa.Table: The CSV converted in an Arrow table
        """
        schema, date_columns = self._get_vocabulary_schema(vocabulary_table)
        tab = pcsv.read_csv(
            csv_file,
            parse_options=pcsv.ParseOptions(delimiter="\t"),
            convert_options=pcsv.ConvertOptions(column_types=schema),
        )

        for date_column in date_columns:
            temp = pc.strptime(tab.column(date_column[1]), format="%Y%m%d", unit="s").cast(pa.date64())
            tab = tab.set_column(date_column[0], date_column[1], temp)

        return tab

    def _get_vocabulary_schema(self, vocabulary_table: str) -> Tuple[pa.Schema, List[Tuple[int, str]]]:
        """Returns a Arrow schema of the vocabulary table and a list of all the data columns (column index and name).

        Args:
            vocabulary_table (str): The vocabulary table.

        Returns:
            Tuple[pa.Schema, List[Tuple[int, str]]]: _description_
        """

        def _to_pa(type_: str) -> pa.DataType:
            match type_.lower():
                case "int64":
                    return pa.int64()
                case "float64":
                    return pa.float64()
                case "string":
                    return pa.string()
                case "date":
                    return pa.string()
            raise Exception(f"Unknown datatype {type_}")

        schema = []
        date_columns = []

        try:
            table_ddl = next((tab for tab in self._parsed_ddl if tab["table_name"] == vocabulary_table))
        except StopIteration as si_err:
            raise Exception(f"{vocabulary_table} not found in ddl") from si_err
        schema = [(col["name"], _to_pa(col["type"])) for col in table_ddl["columns"]]
        schema = pa.schema(schema)

        date_columns = [(idx, col["name"]) for idx, col in enumerate(table_ddl["columns"]) if col["type"] == "date"]

        return (schema, date_columns)

    def _recreate_vocabulary_table(self, vocabulary_table: str) -> None:
        """Recreates a specific standardised vocabulary table

        Args:
            vocabulary_table (str): The standardised vocabulary table
        """
        template = self._template_env.get_template("vocabulary/vocabulary_table_recreate.sql.jinja")
        sql = template.render(
            dataset_omop=self._dataset_omop,
            dataset_work=self._dataset_work,
            vocabulary_table=vocabulary_table,
        )
        self._gcp.run_query_job(sql)

        self._gcp.set_clustering_fields_on_table(
            self._dataset_omop,
            vocabulary_table,
            self._clustering_fields[vocabulary_table],
        )
