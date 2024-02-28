# Copyright 2024 RADar-AZDelta
# SPDX-License-Identifier: gpl3+

import logging
from datetime import date
from pathlib import Path
from typing import Any, List, Optional

import google.cloud.bigquery as bq
import polars as pl
from google.cloud.exceptions import NotFound

from ..etl import Etl
from .bigquery_etl_base import BigQueryEtlBase


class BigQueryEtl(Etl, BigQueryEtlBase):
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

    def _source_to_concept_map_update_invalid_reason(self, etl_start: date) -> None:
        """Cleanup old source to concept maps by setting the invalid_reason to deleted
        for all source to concept maps with a valid_start_date before the ETL start date.

        Args:
            etl_start (date): The start data of the ETL.
        """
        template = self._template_env.get_template("etl/SOURCE_TO_CONCEPT_MAP_update_invalid_reason.sql.jinja")
        sql = template.render(
            dataset_omop=self._dataset_omop,
        )
        self._gcp.run_query_job(
            sql,
            query_parameters=[bq.ScalarQueryParameter("etl_start", "DATE", etl_start)],
        )

    def _source_id_to_omop_id_map_update_invalid_reason(self, etl_start: date) -> None:
        """Cleanup old source id's to omop id's maps by setting the invalid_reason to deleted
        for all maps with a valid_start_date before the ETL start date.

        Args:
            etl_start (date): The start data of the ETL.
        """
        template = self._template_env.get_template("etl/SOURCE_ID_TO_OMOP_ID_MAP_update_invalid_reason.sql.jinja")
        sql = template.render(
            dataset_omop=self._dataset_omop,
        )
        self._gcp.run_query_job(
            sql,
            query_parameters=[bq.ScalarQueryParameter("etl_start", "DATE", etl_start)],
        )

    def _clear_custom_concept_upload_table(self, omop_table: str, concept_id_column: str) -> None:
        """Clears the custom concept upload table (holds the contents of the custom concept CSV's)

        Args:
            omop_table (str): The omop table
            concept_id_column (str): The conept id column
        """
        self._gcp.delete_table(
            self._dataset_work,
            f"{omop_table}__{concept_id_column}_concept",
        )

    def _create_custom_concept_upload_table(self, omop_table: str, concept_id_column: str) -> None:
        """Creates the custom concept upload table (holds the contents of the custom concept CSV's)

        Args:
            omop_table (str): The omop table
            concept_id_column (str): The conept id column
        """
        template = self._template_env.get_template("etl/{omop_table}__{concept_id_column}_concept_create.sql.jinja")
        ddl = template.render(
            dataset_work=self._dataset_work,
            omop_table=omop_table,
            concept_id_column=concept_id_column,
        )
        self._gcp.run_query_job(ddl)

    def _create_custom_concept_id_swap_table(self) -> None:
        """Creates the custom concept id swap tabel (swaps between source value and the concept id)"""
        template = self._template_env.get_template("etl/CONCEPT_ID_swap_create.sql.jinja")
        ddl = template.render(
            dataset_work=self._dataset_work,
        )
        self._gcp.run_query_job(ddl)

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
        # upload the Parquet file to the Cloud Storage Bucket
        uri = self._gcp.upload_file_to_bucket(str(parquet_file), self._bucket_uri)
        # load the uploaded Parquet file from the bucket into the specific custom concept table in the work dataset
        self._gcp.batch_load_from_bucket_into_bigquery_table(
            uri,
            self._dataset_work,
            f"{omop_table}__{concept_id_column}_concept",
        )

    def _give_custom_concepts_an_unique_id_above_2bilj(self, omop_table: str, concept_id_column: str) -> None:
        """Give the custom concepts an unique id (above 2.000.000.000) and store those id's in the concept id swap table.

        Args:
            omop_table (str): The omop table
            concept_id_column (str): The conept id column
        """  # noqa: E501 # pylint: disable=line-too-long
        template = self._template_env.get_template("etl/CONCEPT_ID_swap_merge.sql.jinja")
        sql = template.render(
            dataset_work=self._dataset_work,
            omop_table=omop_table,
            concept_id_column=concept_id_column,
            min_custom_concept_id=Etl._CUSTOM_CONCEPT_IDS_START,
        )
        self._gcp.run_query_job(sql)

    def _merge_custom_concepts_with_the_omop_concepts(self, omop_table: str, concept_id_column: str) -> None:
        """Merges the uploaded custom concepts in the OMOP concept table.

        Args:
            omop_table (str): The omop table
            concept_id_column (str): The conept id column
        """
        template = self._template_env.get_template("etl/CONCEPT_merge.sql.jinja")
        sql = template.render(
            dataset_omop=self._dataset_omop,
            dataset_work=self._dataset_work,
            omop_table=omop_table,
            concept_id_column=concept_id_column,
        )
        self._gcp.run_query_job(sql)

    def _clear_usagi_upload_table(self, omop_table: str, concept_id_column: str) -> None:
        """Clears the usagi upload table (holds the contents of the Usagi CSV's)

        Args:
            omop_table (str): The omop table
            concept_id_column (str): The conept id column
        """
        self._gcp.delete_table(
            self._dataset_work,
            f"{omop_table}__{concept_id_column}_usagi",
        )

    def _create_usagi_upload_table(self, omop_table: str, concept_id_column: str) -> None:
        """Creates the Usagi upload table (holds the contents of the Usagi CSV's)

        Args:
            omop_table (str): The omop table
            concept_id_column (str): The conept id column
        """
        template = self._template_env.get_template("etl/{omop_table}__{concept_id_column}_usagi_create.sql.jinja")
        ddl = template.render(
            dataset_work=self._dataset_work,
            omop_table=omop_table,
            concept_id_column=concept_id_column,
        )
        self._gcp.run_query_job(ddl)

    def _load_usagi_parquet_in_upload_table(self, parquet_file: str, omop_table: str, concept_id_column: str) -> None:
        """The Usagi CSV's are converted to a parquet file.
        This method loads the parquet file in a upload table.

        Args:
            parquet_file (Path): The path to the parquet file
            omop_table (str): The omop table
            concept_id_column (str): The conept id column
        """
        # upload the Parquet file to the Cloud Storage Bucket
        uri = self._gcp.upload_file_to_bucket(parquet_file, self._bucket_uri)
        # load the uploaded Parquet file from the bucket into the specific usagi table in the work dataset
        self._gcp.batch_load_from_bucket_into_bigquery_table(
            uri,
            self._dataset_work,
            f"{omop_table}__{concept_id_column}_usagi",
        )

    def _add_custom_concepts_to_usagi(self, omop_table: str, concept_id_column: str) -> None:
        """The custom concepts are added to the upload Usagi table with status 'APPROVED'.

        Args:
            omop_table (str): The omop table
            concept_id_column (str): The conept id column
        """
        template = self._template_env.get_template(
            "etl/{omop_table}__{concept_id_column}_usagi_add_custom_concepts.sql.jinja"
        )
        sql = template.render(
            dataset_work=self._dataset_work,
            omop_table=omop_table,
            concept_id_column=concept_id_column,
            dataset_omop=self._dataset_omop,
        )
        self._gcp.run_query_job(sql)

    def _update_custom_concepts_in_usagi(self, omop_table: str, concept_id_column: str) -> None:
        """This method updates the Usagi upload table with with the generated custom concept ids (above 2.000.000.000).
        The concept_id column in the Usagi upload table is swapped by the generated custom concept_id (above 2.000.000.000).

        Args:
            omop_table (str): The omop table
            concept_id_column (str): The conept id column
        """  # noqa: E501 # pylint: disable=line-too-long
        template = self._template_env.get_template(
            "etl/{omop_table}__{concept_id_column}_usagi_update_custom_concepts.sql.jinja"
        )
        sql = template.render(
            dataset_work=self._dataset_work,
            omop_table=omop_table,
            concept_id_column=concept_id_column,
        )
        self._gcp.run_query_job(sql)

    def _cast_concepts_in_usagi(self, omop_table: str, concept_id_column: str) -> None:
        """Because we swapped the concept_id column (that for custom concepts is initially loaded with
        the concept_code, and that's a string) in the Usagi upload table with the generated custom
        concept_id (above 2.000.000.000), we need to cast it from string to an integer.

        Args:
            omop_table (str): The omop table
            concept_id_column (str): The conept id column
        """
        template = self._template_env.get_template(
            "etl/{omop_table}__{concept_id_column}_usagi_cast_concepts.sql.jinja"
        )
        sql = template.render(
            dataset_work=self._dataset_work,
            omop_table=omop_table,
            concept_id_column=concept_id_column,
            process_semi_approved_mappings=self._process_semi_approved_mappings,
        )
        self._gcp.run_query_job(sql)

    def _store_usagi_source_value_to_concept_id_mapping(self, omop_table: str, concept_id_column: str) -> None:
        """Fill up the SOURCE_TO_CONCEPT_MAP table with all approved mappings from the uploaded Usagi CSV's

        Args:
            omop_table (str): The omop table
            concept_id_column (str): The conept id column
        """
        template = self._template_env.get_template("etl/SOURCE_TO_CONCEPT_MAP_check_for_duplicates.sql.jinja")
        sql_doubles = template.render(
            dataset_work=self._dataset_work,
            omop_table=omop_table,
            concept_id_column=concept_id_column,
            dataset_omop=self._dataset_omop,
            process_semi_approved_mappings=self._process_semi_approved_mappings,
        )
        rows = self._gcp.run_query_job(sql_doubles)
        ar_table = rows.to_arrow()
        if len(ar_table):
            raise Exception(
                f"Duplicate rows supplied (combination of source_code column and target_concept_id columns must be unique)!\nCheck usagi CSV's for column '{concept_id_column}' of table '{omop_table}'\n{ar_table.to_pylist()}"
            )

        template = self._template_env.get_template("etl/SOURCE_TO_CONCEPT_MAP_merge.sql.jinja")
        sql = template.render(
            dataset_work=self._dataset_work,
            omop_table=omop_table,
            concept_id_column=concept_id_column,
            dataset_omop=self._dataset_omop,
            process_semi_approved_mappings=self._process_semi_approved_mappings,
        )
        self._gcp.run_query_job(sql)

    def _store_usagi_source_id_to_omop_id_mapping(self, omop_table: str, primary_key_column: str) -> None:
        """Fill up the SOURCE_ID_TO_OMOP_ID_MAP table with all the swapped source id's to omop id's

        Args:
            omop_table (str): The omop table
            primary_key_column (str): The primary key column
        """
        template = self._template_env.get_template("etl/SOURCE_ID_TO_OMOP_ID_MAP_merge.sql.jinja")
        sql = template.render(
            dataset_work=self._dataset_work,
            omop_table=omop_table,
            primary_key_column=primary_key_column,
            dataset_omop=self._dataset_omop,
        )
        self._gcp.run_query_job(sql)

    def _get_query_from_sql_file(self, sql_file: Path, omop_table: str) -> str:
        """Reads the query from file. If it is a Jinja template, it renders the template.

        Args:
            sql_file (Path): Path to the sql or jinja file
            omop_table (str): The omop table

        Returns:
            str: The query (if it is a Jinja template, the rendered query)
        """
        with open(sql_file, encoding="UTF8") as file:
            select_query = file.read()
            if Path(sql_file).suffix == ".jinja":
                template = self._template_env.from_string(select_query)
                select_query = template.render(
                    project_raw=self._project_raw,
                    dataset_work=self._dataset_work,
                    dataset_omop=self._dataset_omop,
                    omop_table=omop_table,
                )
        return select_query

    def _query_into_upload_table(self, upload_table: str, select_query: str) -> None:
        """This method inserts the results from our custom SQL queries the the upload OMOP table.

        Args:
            upload_table (str): The work omop table
            select_query (str): The query
        """
        template = self._template_env.get_template("etl/{omop_table}_{sql_file}_insert.sql.jinja")
        ddl = template.render(
            dataset_work=self._dataset_work,
            upload_table=upload_table,
            select_query=select_query,
        )
        self._gcp.run_query_job(ddl)

    def _create_pk_auto_numbering_swap_table(
        self, primary_key_column: str, concept_id_columns: List[str], events: Any
    ) -> None:
        """This method created a swap table so that our source codes can be translated to auto numbering primary keys.

        Args:
            primary_key_column (str): The primary key column
            concept_id_columns (List[str]): List of concept_id columns
            events (Any): Object that holds the events of the the OMOP table.
        """
        template = self._template_env.get_template("etl/{primary_key_column}_swap_create.sql.jinja")
        ddl = template.render(
            dataset_work=self._dataset_work,
            primary_key_column=primary_key_column,
            # foreign_key_columns=vars(foreign_key_columns),
            concept_id_columns=concept_id_columns,
            events=events,
        )
        self._gcp.run_query_job(ddl)

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
        template = self._template_env.get_template("etl/{primary_key_column}_swap_merge.sql.jinja")
        sql = template.render(
            dataset_work=self._dataset_work,
            primary_key_column=primary_key_column,
            concept_id_columns=concept_id_columns,
            omop_table=omop_table,
            events=events,
            sql_files=sql_files,
            upload_tables=upload_tables,
            process_semi_approved_mappings=self._process_semi_approved_mappings,
        )
        self._gcp.run_query_job(sql)

    def _combine_upload_tables(
        self,
        omop_table: str,
        upload_tables: List[str],
        columns: List[str],
    ):
        """Combine the data of the upload tables into one table.

        Args:
            omop_table_name (str): Name of the OMOP table
            upload_tables (List[Path]): Thelist of upload tables
            columns (List[str]): List of columns of the OMOP table
        """
        template = self._template_env.get_template("etl/{omop_work_table}_combine_upload_tables.sql.jinja")
        sql = template.render(
            dataset_work=self._dataset_work,
            omop_table=omop_table,
            upload_tables=upload_tables,
            columns=columns,
        )
        self._gcp.run_query_job(sql)

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
            omop_table (str): OMOP table.
            columns (List[str]): List of columns of the OMOP table.
            required_columns (List[str]): List of required columns of the OMOP table.
            primary_key_column (str): The name of the primary key column.
            pk_auto_numbering (bool): Is the primary key a generated incremental number?
            foreign_key_columns (Any): List of foreign key columns.
            concept_id_columns (List[str]): List of concept columns.
            events (Any): Object that holds the events of the the OMOP table.
        """  # noqa: E501 # pylint: disable=line-too-long
        template = self._template_env.get_template("etl/{omop_work_table}_merge_check_for_duplicate_rows.sql.jinja")
        sql_doubles = template.render(
            omop_table=omop_table,
            dataset_work=self._dataset_work,
            primary_key_column=primary_key_column,
            concept_id_columns=concept_id_columns,
            events=events,
        )
        rows = self._gcp.run_query_job(sql_doubles)
        ar_table = rows.to_arrow()
        if len(ar_table):
            raise Exception(
                f"Duplicate rows supplied (combination of id column and concept columns must be unique)! Check ETL queries for table '{omop_table}' and run the 'clean' command!\nQuery to get the duplicates:\n{sql_doubles}"
            )

        template = self._template_env.get_template("etl/{omop_work_table}_merge.sql.jinja")
        sql = template.render(
            dataset_omop=self._dataset_omop,
            omop_table=omop_table,
            dataset_work=self._dataset_work,
            columns=columns,
            required_columns=required_columns,
            primary_key_column=primary_key_column,
            foreign_key_columns=foreign_key_columns,
            concept_id_columns=concept_id_columns,
            pk_auto_numbering=pk_auto_numbering,
            events=events,
            process_semi_approved_mappings=self._process_semi_approved_mappings,
        )
        self._gcp.run_query_job(sql)

    def _merge_into_omop_table(
        self,
        omop_table: str,
        columns: List[str],
        primary_key_column: Optional[str],
        events: dict[str, str],
    ):
        """The one shot merge of OMOP work table into the destination OMOP table applying the events.

        Args:
            sql_file (str): The sql file holding the query on the raw data.
            omop_table (str): OMOP table.
            columns (List[str]): List of columns of the OMOP table.
            primary_key_column (str): The name of the primary key column.
            events (Any): Object that holds the events of the the OMOP table.
        """  # noqa: E501 # pylint: disable=line-too-long

        event_tables = {}
        try:
            if len(events) > 0:  # we have event columns
                template = self._template_env.get_template("etl/{omop_table}_get_event_tables.sql.jinja")
                sql = template.render(
                    omop_table=omop_table,
                    dataset_work=self._dataset_work,
                    events=events,
                )
                rows = self._gcp.run_query_job(sql)
                event_tables = dict(
                    (table, self._get_pk(table)) for table in (row.event_table for row in rows) if table
                )

            template = self._template_env.get_template("etl/{omop_table}_merge.sql.jinja")
            sql = template.render(
                dataset_omop=self._dataset_omop,
                omop_table=omop_table,
                dataset_work=self._dataset_work,
                columns=columns,
                primary_key_column=primary_key_column,
                events=events,
                event_tables=event_tables,
                min_custom_concept_id=Etl._CUSTOM_CONCEPT_IDS_START,
            )
            self._gcp.run_query_job(sql)
        except Exception as e:
            if isinstance(e.__cause__, NotFound):  # chained exception!!!
                logging.debug(
                    "Table %s not found in work dataset, continue without merge for this table",
                    omop_table,
                )

    def _create_omop_work_table(self, omop_table: str, events: Any) -> None:
        """Creates the OMOP work table (if it does'nt yet exists) based on the DDL.

        Args:
            omop_table (str): The OMOP table
            events (Any): Object that holds the events of the the OMOP table.
        """
        columns = (
            self._df_omop_fields.filter(pl.col("cdmTableName").str.to_lowercase() == omop_table)
            .with_columns([pl.col("cdmDatatype").map_elements(lambda s: self._get_column_type(s)).alias("cdmDatatype")])
            .rows(named=True)
        )

        cluster_fields = self._clustering_fields[omop_table] if omop_table in self._clustering_fields else []

        template = self._template_env.get_template("etl/create_omop_work_table.sql.jinja")
        sql = template.render(
            dataset_work=self._dataset_work,
            omop_table=omop_table,
            columns=columns,
            events=events,
            cluster_fields=cluster_fields,
        )
        self._gcp.run_query_job(sql)

    def _check_usagi_fk_domains(self, omop_table: str, concept_id_column: str, domains: list[str]) -> bool:
        """Checks the usagi fk domain of the concept id column.

        Args:
            omop_table (str): The omop table
            concept_id_column (str): The conept id column
            domains (list[str]): The allowed domains
        """
        template = self._template_env.get_template(
            "etl/{omop_table}__{concept_id_column}_usagi_fk_domain_check.sql.jinja"
        )
        sql = template.render(
            dataset_work=self._dataset_work,
            dataset_omop=self._dataset_omop,
            omop_table=omop_table,
            concept_id_column=concept_id_column,
            domains=domains,
        )
        rows = self._gcp.run_query_job(sql)
        ar_table = rows.to_arrow()
        if len(ar_table):
            raise Exception(
                f"Invalid concept domains found in the Usagi CSV's for concept column '{concept_id_column}' of OMOP table '{omop_table}'!\nOnly concept domains ({', '.join(domains)}) are allowed!"
            )
