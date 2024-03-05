# Copyright 2024 RADar-AZDelta
# SPDX-License-Identifier: gpl3+

from datetime import date
from pathlib import Path
from typing import Any, List, Optional

from ..etl import Etl
from .etl_base import SqlServerEtlBase


class SqlServerEtl(Etl, SqlServerEtlBase):
    """
    ETL class that automates the extract-transfer-load process from source data to the OMOP common data model.
    """

    def __init__(
        self,
        **kwargs,
    ):
        """Constructor

        Args:
            credentials_file (str): The credentials file must be a service account key, stored authorized user credentials or external account credentials.
            project_id (str): Project ID in GCP
            location (str): The location in GCP (see https://cloud.google.com/about/locations/)
            dataset_id_raw (str): Big Query dataset ID that holds the raw tables
            dataset_id_work (str): Big Query dataset ID that holds the work tables
            dataset_id_omop (str): Big Query dataset ID that holds the omop tables
            bucket_uri (str): The name of the Cloud Storage bucket and the path in the bucket (directory) to store the Parquet file(s) (the uri has format 'gs://{bucket_name}/{bucket_path}'). These parquet files will be the converted and uploaded 'custom concept' CSV's and the Usagi CSV's.
        ```
        """  # noqa: E501 # pylint: disable=line-too-long
        super().__init__(**kwargs)

    def _combine_upload_tables(
        self,
        omop_table: str,
        upload_tables: List[str],
        columns: List[str],
    ):
        """Combine the data of the upload tables into one table.

        Args:
            omop_table_name (str): Name of the OMOP table
            upload_tables (List[str]): The list of upload tables
            columns (List[str]): The list of columns
        """
        pass

    def _merge_into_omop_work_table(
        self,
        omop_table: str,
        columns: List[str],
        required_columns: List[str],
        primary_key_column: Optional[str],
        pk_auto_numbering: bool,
        foreign_key_columns: Any,
        concept_id_columns: List[str],
        events: Any,
    ):
        """The one shot merge of the uploaded query result from the work table, with the swapped primary and foreign keys, the mapped Usagi concept and custom concepts in the destination OMOP table.

        Args:
            sql_files (List[Path]): The sql files holding the query on the raw data.
            omop_table (str): OMOP table.
            columns (List[str]): List of columns of the OMOP table.
            required_columns (List[str]): List of required columns of the OMOP table.
            pk_swap_table_name (str): The name of the swap table to convert the source value of the primary key to an auto number.
            primary_key_column (str): The name of the primary key column.
            pk_auto_numbering (bool): Is the primary key a generated incremental number?
            foreign_key_columns (Any): List of foreign key columns.
            concept_id_columns (List[str]): List of concept columns.
            events (Any): Object that holds the events of the the OMOP table.
        """  # noqa: E501 # pylint: disable=line-too-long
        pass

    def _source_to_concept_map_update_invalid_reason(self, etl_start: date) -> None:
        """Cleanup old source to concept maps by setting the invalid_reason to deleted
        for all source to concept maps with a valid_start_date before the ETL start date.

        Args:
            etl_start (date): The start data of the ETL.
        """
        pass

    def _source_id_to_omop_id_map_update_invalid_reason(self, etl_start: date) -> None:
        """Cleanup old source id's to omop id's maps by setting the invalid_reason to deleted
        for all maps with a valid_start_date before the ETL start date.

        Args:
            etl_start (date): The start data of the ETL.
        """
        pass

    def _clear_custom_concept_upload_table(self, omop_table: str, concept_id_column: str) -> None:
        """Clears the custom concept upload table (holds the contents of the custom concept CSV's)

        Args:
            omop_table (str): The omop table
            concept_id_column (str): The conept id column
        """
        pass

    def _create_custom_concept_upload_table(self, omop_table: str, concept_id_column: str) -> None:
        """Creates the custom concept upload table (holds the contents of the custom concept CSV's)

        Args:
            omop_table (str): The omop table
            concept_id_column (str): The conept id column
        """
        pass

    def _create_custom_concept_id_swap_table(self) -> None:
        """Creates the custom concept id swap tabel (swaps between source value and the concept id)"""
        pass

    def _load_custom_concepts_parquet_in_upload_table(
        self, parquet_file: Path, omop_table: str, concept_id_column: str
    ) -> None:
        """The custom concept CSV's are converted to a parquet file.
        This method loads the parquet file in a upload table.

        Args:
            parquet_file (Path): The path to the parquet file
            omop_table (str): The omop table
            concept_id_column (str): The conept id column
        """
        pass

    def _give_custom_concepts_an_unique_id_above_2bilj(self, omop_table: str, concept_id_column: str) -> None:
        """Give the custom concepts an unique id (above 2.000.000.000) and store those id's
        in the concept id swap table.

        Args:
            omop_table (str): The omop table
            concept_id_column (str): The conept id column
        """
        pass

    def _merge_custom_concepts_with_the_omop_concepts(self, omop_table: str, concept_id_column: str) -> None:
        """Merges the uploaded custom concepts in the OMOP concept table.

        Args:
            omop_table (str): The omop table
            concept_id_column (str): The conept id column
        """
        pass

    def _clear_usagi_upload_table(self, omop_table: str, concept_id_column: str) -> None:
        """Clears the usagi upload table (holds the contents of the usagi CSV's)

        Args:
            omop_table (str): The omop table
            concept_id_column (str): The conept id column
        """
        pass

    def _create_usagi_upload_table(self, omop_table: str, concept_id_column: str) -> None:
        """Creates the Usagi upload table (holds the contents of the Usagi CSV's)

        Args:
            omop_table (str): The omop table
            concept_id_column (str): The conept id column
        """
        pass

    def _load_usagi_parquet_in_upload_table(self, parquet_file: str, omop_table: str, concept_id_column: str) -> None:
        """The Usagi CSV's are converted to a parquet file.
        This method loads the parquet file in a upload table.

        Args:
            parquet_file (Path): The path to the parquet file
            omop_table (str): The omop table
            concept_id_column (str): The conept id column
        """
        pass

    def _add_custom_concepts_to_usagi(self, omop_table: str, concept_id_column: str) -> None:
        """The custom concepts are added to the upload Usagi table with status 'APPROVED'.

        Args:
            omop_table (str): The omop table
            concept_id_column (str): The conept id column
        """
        pass

    def _update_custom_concepts_in_usagi(self, omop_table: str, concept_id_column: str) -> None:
        """This method updates the Usagi upload table with with the generated custom concept ids (above 2.000.000.000).
        The concept_id column in the Usagi upload table is swapped by the generated custom concept_id (above 2.000.000.000).

        Args:
            omop_table (str): The omop table
            concept_id_column (str): The conept id column
        """
        pass

    def _cast_concepts_in_usagi(self, omop_table: str, concept_id_column: str) -> None:
        """Because we swapped the concept_id column (that for custom concepts is initially loaded with
        the concept_code, and that's a string) in the Usagi upload table with the generated custom
        concept_id (above 2.000.000.000), we need to cast it from string to an integer.

        Args:
            omop_table (str): The omop table
            concept_id_column (str): The conept id column
        """
        pass

    def _store_usagi_source_value_to_concept_id_mapping(self, omop_table: str, concept_id_column: str) -> None:
        """Fill up the SOURCE_TO_CONCEPT_MAP table with all approved mappings from the uploaded Usagi CSV's

        Args:
            omop_table (str): The omop table
            concept_id_column (str): The conept id column
        """
        pass

    def _get_query_from_sql_file(self, sql_file: Path, omop_table: str) -> str:
        """Reads the query from file. If it is a Jinja template, it renders the template.

        Args:
            sql_file (Path): Path to the sql or jinja file
            omop_table (str): _description_

        Returns:
            str: The query (if it is a Jinja template, the rendered query)
        """
        pass

    def _query_into_upload_table(self, upload_table: str, select_query: str) -> None:
        """This method inserts the results from our custom SQL queries the the work OMOP upload table.

        Args:
            upload_table (str): The work omop table
            select_query (str): The query
        """
        pass

    def _create_pk_auto_numbering_swap_table(
        self, primary_key_column: str, concept_id_columns: List[str], events: Any
    ) -> None:
        """This method created a swap table so that our source codes can be translated to auto numbering primary keys.

        Args:
            primary_key_column (str): The primary key column of the OMOP table
            concept_id_columns (List[str]): List of concept_id columns
            events (Any): Object that holds the events of the the OMOP table.
        """
        pass

    def _execute_pk_auto_numbering_swap_query(
        self,
        omop_table: str,
        primary_key_column: str,
        concept_id_columns: List[str],
        events: Any,
        sql_files: List[str],
        upload_tables: List[str],
    ) -> None:
        """This method does the swapping of our source codes to an auto number that will be the primary key
        of our OMOP table.

        Args:
            omop_table (str): The OMOP table
            primary_key_column (str): Primary key column
            concept_id_columns (List[str]): List of concept_id columns
            events (Any): Object that holds the events of the the OMOP table.
            sql_files (List[str]): List of upload SQL files
            upload_tables (List[str]): List of upload tables
        """
        pass

    def _create_omop_work_table(self, omop_table: str, events: Any) -> None:
        """Creates the OMOP work table (if it does'nt yet exists) based on the DDL.

        Args:
            omop_table (str): The OMOP table
            events (Any): Object that holds the events of the the OMOP table.
        """
        pass

    def _merge_into_omop_table(
        self,
        omop_table: str,
        columns: List[str],
        primary_key_column: Optional[str],
        events: Any,
    ):
        """The one shot merge of OMOP work table into the destination OMOP table applying the events.

        Args:
            sql_file (str): The sql file holding the query on the raw data.
            omop_table (str): OMOP table.
            columns (List[str]): List of columns of the OMOP table.
            primary_key_column (str): The name of the primary key column.
            events (Any): Object that holds the events of the the OMOP table.
        """  # noqa: E501 # pylint: disable=line-too-long
        pass

    def _store_usagi_source_id_to_omop_id_mapping(self, omop_table: str, primary_key_column: str) -> None:
        """Fill up the SOURCE_ID_TO_OMOP_ID_MAP table with all the swapped source id's to omop id's

        Args:
            omop_table (str): The omop table
            primary_key_column (str): The primary key column
        """
        pass

    def _check_usagi_fk_domains(self, omop_table: str, concept_id_column: str, domains: list[str]) -> None:
        """Checks the usagi fk domain of the concept id column.

        Args:
            omop_table (str): The omop table
            concept_id_column (str): The conept id column
            domains (list[str]): The allowed domains
        """
        pass
