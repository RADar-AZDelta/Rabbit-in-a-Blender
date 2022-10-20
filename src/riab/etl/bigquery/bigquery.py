# Copyright 2022 RADar-AZDelta
# SPDX-License-Identifier: gpl3+

# pylint: disable=unsubscriptable-object
"""Holds the BigQuery ETL class"""
import json
import logging
import re
from datetime import date
from pathlib import Path
from threading import Lock
from typing import Any, Dict, List, Optional, Tuple, cast

import google.auth
import google.cloud.bigquery as bq
import jinja2 as jj
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pcsv
import pyarrow.parquet as pq
from google.api_core.exceptions import BadRequest
from google.cloud.bigquery.table import RowIterator, _EmptyRowIterator
from google.cloud.exceptions import NotFound
from jinja2.utils import select_autoescape
from simple_ddl_parser import DDLParser

from ..etl import Etl
from .gcp import Gcp


class BigQuery(Etl):
    """
    ETL class that automates the extract-transfer-load process from source data to the OMOP common data model.
    """

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
        self._lock = Lock()

    def create_omop_db(self) -> None:
        """Create OMOP tables in the omop-dataset in BigQuery and apply clustering"""
        logging.info("Creating OMOP CDM database from DDL (Data Definition Language)")
        self._gcp.run_query_job(self._ddl)

        for table, fields in self._clustering_fields.items():
            self._gcp.set_clustering_fields_on_table(
                self._project_id, self._dataset_id_omop, table, fields
            )

    @property
    def _parsed_ddl(self) -> List[Dict]:
        """Holds the parsed DDL

        Returns:
            List[Dict]: the parsed DDL
        """
        self._lock.acquire()
        if not self.__parsed_ddl:
            self.__parsed_ddl = DDLParser(self._ddl).run(output_mode="sql")
        self._lock.release()
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

    def _source_to_concept_map_update_invalid_reason(self, etl_start: date) -> None:
        """Cleanup old source to concept maps by setting the invalid_reason to deleted
        for all source to concept maps with a valid_start_date before the ETL start date.

        Args:
            etl_start (date): The start data of the ETL.
        """
        template = self._template_env.get_template(
            "SOURCE_TO_CONCEPT_MAP_update_invalid_reason.sql.jinja"
        )
        sql = template.render(
            project_id=self._project_id,
            dataset_id_omop=self._dataset_id_omop,
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
        template = self._template_env.get_template(
            "SOURCE_ID_TO_OMOP_ID_MAP_update_invalid_reason.sql.jinja"
        )
        sql = template.render(
            project_id=self._project_id,
            dataset_id_omop=self._dataset_id_omop,
        )
        self._gcp.run_query_job(
            sql,
            query_parameters=[bq.ScalarQueryParameter("etl_start", "DATE", etl_start)],
        )

    def _get_column_names(self, omop_table_name: str) -> List[str]:
        """Get list of column names of a omop table.

        Args:
            omop_table_name (str): OMOP table

        Returns:
            List[str]: list of column names
        """
        columns = self._gcp.get_column_names(
            self._project_id, self._dataset_id_omop, omop_table_name
        )
        return columns

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

    def _is_pk_auto_numbering(
        self, omop_table_name: str, omop_table_props: Any
    ) -> bool:
        """Checks if the primary key of the OMOP table needs autonumbering.
        For example the [Person](https://ohdsi.github.io/CommonDataModel/cdm54.html#PERSON) table has an auto numbering primary key, the [Vocabulary](https://ohdsi.github.io/CommonDataModel/cdm54.html#VOCABULARY) table not.

        Args:
            omop_table_name (str): OMOP table
            omop_table_props (Any): Primary key, foreign key(s) and event(s) of the OMOP table

        Returns:
            bool: True if the PK needs autonumbering
        """  # noqa: E501 # pylint: disable=line-too-long
        if not hasattr(omop_table_props, "pk"):
            return False
        # get the primary key meta data from the the destination OMOP table
        pk_column_metadata = self._gcp.get_column_metadata(
            self._project_id,
            self._dataset_id_omop,
            omop_table_name,
            omop_table_props.pk,
        )
        # is the primary key an auto numbering column?
        pk_auto_numbering = (
            pk_column_metadata and pk_column_metadata.get("data_type") == "INT64"
        )
        return pk_auto_numbering

    def _clear_custom_concept_upload_table(
        self, omop_table: str, concept_id_column: str
    ) -> None:
        """Clears the custom concept upload table (holds the contents of the custom concept CSV's)

        Args:
            omop_table (str): The omop table
            concept_id_column (str): The conept id column
        """
        self._gcp.delete_table(
            self._project_id,
            self._dataset_id_work,
            f"{omop_table}__{concept_id_column}_concept",
        )

    def _create_custom_concept_upload_table(
        self, omop_table: str, concept_id_column: str
    ) -> None:
        """Creates the custom concept upload table (holds the contents of the custom concept CSV's)

        Args:
            omop_table (str): The omop table
            concept_id_column (str): The conept id column
        """
        template = self._template_env.get_template(
            "{omop_table}__{concept_id_column}_concept_create.sql.jinja"
        )
        ddl = template.render(
            project_id=self._project_id,
            dataset_id_work=self._dataset_id_work,
            omop_table=omop_table,
            concept_id_column=concept_id_column,
        )
        self._gcp.run_query_job(ddl)

    def _create_custom_concept_id_swap_table(self) -> None:
        """Creates the custom concept id swap tabel (swaps between source value and the concept id)"""
        template = self._template_env.get_template("CONCEPT_ID_swap_create.sql.jinja")
        ddl = template.render(
            project_id=self._project_id,
            dataset_id_work=self._dataset_id_work,
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
            self._project_id,
            self._dataset_id_work,
            f"{omop_table}__{concept_id_column}_concept",
        )

    def _give_custom_concepts_an_unique_id_above_2bilj(
        self, omop_table: str, concept_id_column: str
    ) -> None:
        """Give the custom concepts an unique id (above 2.000.000.000) and store those id's in the concept id swap table.

        Args:
            omop_table (str): The omop table
            concept_id_column (str): The conept id column
        """  # noqa: E501 # pylint: disable=line-too-long
        template = self._template_env.get_template("CONCEPT_ID_swap_merge.sql.jinja")
        sql = template.render(
            project_id=self._project_id,
            dataset_id_work=self._dataset_id_work,
            omop_table=omop_table,
            concept_id_column=concept_id_column,
            min_custom_concept_id=Etl._CUSTOM_CONCEPT_IDS_START,
        )
        self._gcp.run_query_job(sql)

    def _merge_custom_concepts_with_the_omop_concepts(
        self, omop_table: str, concept_id_column: str
    ) -> None:
        """Merges the uploaded custom concepts in the OMOP concept table.

        Args:
            omop_table (str): The omop table
            concept_id_column (str): The conept id column
        """
        template = self._template_env.get_template("CONCEPT_merge.sql.jinja")
        sql = template.render(
            project_id=self._project_id,
            dataset_id_omop=self._dataset_id_omop,
            dataset_id_work=self._dataset_id_work,
            omop_table=omop_table,
            concept_id_column=concept_id_column,
        )
        self._gcp.run_query_job(sql)

    def _clear_usagi_upload_table(
        self, omop_table: str, concept_id_column: str
    ) -> None:
        """Clears the usagi upload table (holds the contents of the Usagi CSV's)

        Args:
            omop_table (str): The omop table
            concept_id_column (str): The conept id column
        """
        self._gcp.delete_table(
            self._project_id,
            self._dataset_id_work,
            f"{omop_table}__{concept_id_column}_usagi",
        )

    def _create_usagi_upload_table(
        self, omop_table: str, concept_id_column: str
    ) -> None:
        """Creates the Usagi upload table (holds the contents of the Usagi CSV's)

        Args:
            omop_table (str): The omop table
            concept_id_column (str): The conept id column
        """
        template = self._template_env.get_template(
            "{omop_table}__{concept_id_column}_usagi_create.sql.jinja"
        )
        ddl = template.render(
            project_id=self._project_id,
            dataset_id_work=self._dataset_id_work,
            omop_table=omop_table,
            concept_id_column=concept_id_column,
        )
        self._gcp.run_query_job(ddl)

    def _load_usagi_parquet_in_upload_table(
        self, parquet_file: str, omop_table: str, concept_id_column: str
    ) -> None:
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
            self._project_id,
            self._dataset_id_work,
            f"{omop_table}__{concept_id_column}_usagi",
        )

    def _add_custom_concepts_to_usagi(
        self, omop_table: str, concept_id_column: str
    ) -> None:
        """The custom concepts are added to the upload Usagi table with status 'APPROVED'.

        Args:
            omop_table (str): The omop table
            concept_id_column (str): The conept id column
        """
        template = self._template_env.get_template(
            "{omop_table}__{concept_id_column}_usagi_add_custom_concepts.sql.jinja"
        )
        sql = template.render(
            project_id=self._project_id,
            dataset_id_work=self._dataset_id_work,
            omop_table=omop_table,
            concept_id_column=concept_id_column,
            dataset_id_omop=self._dataset_id_omop,
        )
        self._gcp.run_query_job(sql)

    def _update_custom_concepts_in_usagi(
        self, omop_table: str, concept_id_column: str
    ) -> None:
        """This method updates the Usagi upload table with with the generated custom concept ids (above 2.000.000.000).
        The concept_id column in the Usagi upload table is swapped by the generated custom concept_id (above 2.000.000.000).

        Args:
            omop_table (str): The omop table
            concept_id_column (str): The conept id column
        """  # noqa: E501 # pylint: disable=line-too-long
        template = self._template_env.get_template(
            "{omop_table}__{concept_id_column}_usagi_update_custom_concepts.sql.jinja"
        )
        sql = template.render(
            project_id=self._project_id,
            dataset_id_work=self._dataset_id_work,
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
            "{omop_table}__{concept_id_column}_usagi_cast_concepts.sql.jinja"
        )
        sql = template.render(
            project_id=self._project_id,
            dataset_id_work=self._dataset_id_work,
            omop_table=omop_table,
            concept_id_column=concept_id_column,
        )
        self._gcp.run_query_job(sql)

    def _store_usagi_source_value_to_concept_id_mapping(
        self, omop_table: str, concept_id_column: str
    ) -> None:
        """Fill up the SOURCE_TO_CONCEPT_MAP table with all approved mappings from the uploaded Usagi CSV's

        Args:
            omop_table (str): The omop table
            concept_id_column (str): The conept id column
        """
        template = self._template_env.get_template(
            "SOURCE_TO_CONCEPT_MAP_merge.sql.jinja"
        )
        sql = template.render(
            project_id=self._project_id,
            dataset_id_work=self._dataset_id_work,
            omop_table=omop_table,
            concept_id_column=concept_id_column,
            dataset_id_omop=self._dataset_id_omop,
        )
        self._gcp.run_query_job(sql)

    def _store_usagi_source_id_to_omop_id_mapping(
        self, omop_table: str, pk_swap_table_name: str
    ) -> None:
        """Fill up the SOURCE_ID_TO_OMOP_ID_MAP table with all the swapped source id's to omop id's

        Args:
            omop_table (str): The omop table
            pk_swap_table_name (str): The id swap work table
        """
        template = self._template_env.get_template(
            "SOURCE_ID_TO_OMOP_ID_MAP_merge.sql.jinja"
        )
        sql = template.render(
            project_id=self._project_id,
            dataset_id_work=self._dataset_id_work,
            omop_table=omop_table,
            pk_swap_table_name=pk_swap_table_name,
            dataset_id_omop=self._dataset_id_omop,
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
                    project_id=self._project_id,
                    dataset_id_raw=self._dataset_id_raw,
                    dataset_id_work=self._dataset_id_work,
                    dataset_id_omop=self._dataset_id_omop,
                    omop_table=omop_table,
                )
        return select_query

    def _query_into_work_table(self, work_table: str, select_query: str) -> None:
        """This method inserts the results from our custom SQL queries the the work OMOP table.

        Args:
            work_table (str): The work omop table
            select_query (str): The query
        """
        template = self._template_env.get_template(
            "{omop_table}_{sql_file}_insert.sql.jinja"
        )
        ddl = template.render(
            project_id=self._project_id,
            dataset_id_work=self._dataset_id_work,
            work_table=work_table,
            select_query=select_query,
        )
        self._gcp.run_query_job(ddl)

    def _create_pk_auto_numbering_swap_table(
        self, pk_swap_table_name: str, concept_id_columns: List[str], events: Any
    ) -> None:
        """This method created a swap table so that our source codes can be translated to auto numbering primary keys.

        Args:
            pk_swap_table_name (str): The name of our primary key swap table
            concept_id_columns (List[str]): List of concept_id columns
            events (Any): Object that holds the events of the the OMOP table.
        """
        template = self._template_env.get_template(
            "{primary_key_column}_swap_create.sql.jinja"
        )
        ddl = template.render(
            project_id=self._project_id,
            dataset_id_work=self._dataset_id_work,
            pk_swap_table_name=pk_swap_table_name,
            # foreign_key_columns=vars(foreign_key_columns),
            concept_id_columns=concept_id_columns,
            events=events,
        )
        self._gcp.run_query_job(ddl)

    def _execute_pk_auto_numbering_swap_query(
        self,
        omop_table: str,
        work_tables: List[str],
        pk_swap_table_name: str,
        primary_key_column: str,
        concept_id_columns: List[str],
        events: Any,
    ) -> None:
        """This method does the swapping of our source codes to an auto number that will be the primary key
        of our OMOP table.

        Args:
            omop_table (str): The OMOP table
            work_tables (List[str]): The OMOP work tables of the uploaded queries
            pk_swap_table_name (str): Primary key swap table
            primary_key_column (str): Primary key column
            concept_id_columns (List[str]): List of concept_id columns
            events (Any): Object that holds the events of the the OMOP table.
        """
        template = self._template_env.get_template(
            "{primary_key_column}_swap_merge.sql.jinja"
        )
        sql = template.render(
            project_id=self._project_id,
            dataset_id_work=self._dataset_id_work,
            pk_swap_table_name=pk_swap_table_name,
            primary_key_column=primary_key_column,
            concept_id_columns=concept_id_columns,
            omop_table=omop_table,
            work_tables=work_tables,
            events=events,
        )
        self._gcp.run_query_job(sql)

    def _merge_into_omop_work_table(
        self,
        sql_files: List[Path],
        omop_table: str,
        columns: List[str],
        pk_swap_table_name: Optional[str],
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
            pk_swap_table_name (str): The name of the swap table to convert the source value of the primary key to an auto number.
            primary_key_column (str): The name of the primary key column.
            pk_auto_numbering (bool): Is the primary key a generated incremental number?
            foreign_key_columns (Any): List of foreign key columns.
            concept_id_columns (List[str]): List of concept columns.
            events (Any): Object that holds the events of the the OMOP table.
        """  # noqa: E501 # pylint: disable=line-too-long
        try:
            template = self._template_env.get_template(
                "{omop_work_table}_merge.sql.jinja"
            )
            sql = template.render(
                project_id=self._project_id,
                dataset_id_omop=self._dataset_id_omop,
                omop_table=omop_table,
                dataset_id_work=self._dataset_id_work,
                sql_files=[Path(Path(sql_file).stem).stem for sql_file in sql_files],
                columns=columns,
                pk_swap_table_name=pk_swap_table_name,
                primary_key_column=primary_key_column,
                foreign_key_columns=vars(foreign_key_columns)
                if hasattr(foreign_key_columns, "__dict__")
                else foreign_key_columns,
                concept_id_columns=concept_id_columns,
                pk_auto_numbering=pk_auto_numbering,
                events=events,
            )
            self._gcp.run_query_job(sql)

            if primary_key_column or omop_table == "death":
                template = self._template_env.get_template(
                    "{omop_work_table}_merge._check_for_duplicate_rows.sql.jinja"
                )
                sql = template.render(
                    project_id=self._project_id,
                    omop_table=omop_table,
                    dataset_id_work=self._dataset_id_work,
                    primary_key_column=primary_key_column
                    if omop_table != "death"
                    else "person_id",
                )
                rows = self._gcp.run_query_job(sql)
                ar_table = rows.to_arrow()
                if len(ar_table):
                    sample_duplicate_ids = ar_table[
                        primary_key_column if omop_table != "death" else "person_id"
                    ].__str__()
                    raise Exception(
                        f"Duplicate rows supplied! Check ETL queries for table {omop_table} and run the 'clean' command!\n"
                        + sample_duplicate_ids
                    )

        except BadRequest as br:
            if br.message.startswith(
                "UPDATE/MERGE must match at most one source row for each target row"
            ):
                logging.error(
                    f"Duplicate rows supplied! Check ETL queries for table {omop_table} and run the 'clean' command!"
                )
            if __debug__:
                breakpoint()

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
        event_tables = {}
        try:
            if dict(events):  # we have event colomns
                template = self._template_env.get_template(
                    "{omop_table}_get_event_tables.sql.jinja"
                )
                sql = template.render(
                    project_id=self._project_id,
                    omop_table=omop_table,
                    dataset_id_work=self._dataset_id_work,
                    events=events,
                )
                rows = self._gcp.run_query_job(sql)
                event_tables = dict(
                    (table, vars(self._omop_tables)[table].pk)
                    for table in (row.event_table for row in rows)
                    if table
                )

            cluster_fields = (
                self._clustering_fields[omop_table]
                if omop_table in self._clustering_fields
                else []
            )

            template = self._template_env.get_template("{omop_table}_merge.sql.jinja")
            sql = template.render(
                project_id=self._project_id,
                dataset_id_omop=self._dataset_id_omop,
                omop_table=omop_table,
                dataset_id_work=self._dataset_id_work,
                columns=columns,
                primary_key_column=primary_key_column,
                events=events,
                event_tables=event_tables,
                cluster_fields=cluster_fields,
            )
            self._gcp.run_query_job(sql)
        except NotFound:
            logging.debug(
                "Table %s not found in work dataset, continue without merge for this table",
                omop_table,
            )

    def _get_work_tables(self) -> List[str]:
        """Returns a list of all our work tables (Usagi upload, custom concept upload, swap and query upload tables)

        Returns:
            List[str]: List of all the work tables
        """
        work_tables = self._gcp.get_table_names(self._project_id, self._dataset_id_work)
        return work_tables

    def _truncate_omop_table(self, table_name: str) -> None:
        """Remove all rows from an OMOP table

        Args:
            table_name (str): Omop table to truncate
        """
        logging.info("Truncate OMOP table '%s'", table_name)
        template = self._template_env.get_template("cleanup/truncate.sql.jinja")
        sql = template.render(
            project_id=self._project_id,
            dataset_id_omop=self._dataset_id_omop,
            table_name=table_name,
        )
        self._gcp.run_query_job(sql)

    def _remove_custom_concepts_from_concept_table(self) -> None:
        """Remove the custom concepts from the OMOP concept table"""
        template = self._template_env.get_template(
            "cleanup/CONCEPT_remove_custom_concepts.sql.jinja"
        )
        sql = template.render(
            project_id=self._project_id,
            dataset_id_omop=self._dataset_id_omop,
            min_custom_concept_id=Etl._CUSTOM_CONCEPT_IDS_START,
        )
        self._gcp.run_query_job(sql)

    def _remove_custom_concepts_from_concept_relationship_table(self) -> None:
        """Remove the custom concepts from the OMOP concept_relationship table"""
        template = self._template_env.get_template(
            "cleanup/CONCEPT_RELATIONSHIP_remove_custom_concepts.sql.jinja"
        )
        sql = template.render(
            project_id=self._project_id,
            dataset_id_omop=self._dataset_id_omop,
            min_custom_concept_id=Etl._CUSTOM_CONCEPT_IDS_START,
        )
        self._gcp.run_query_job(sql)

    def _remove_custom_concepts_from_concept_ancestor_table(self) -> None:
        """Remove the custom concepts from the OMOP concept_ancestor table"""
        template = self._template_env.get_template(
            "cleanup/CONCEPT_ANCESTOR_remove_custom_concepts.sql.jinja"
        )
        sql = template.render(
            project_id=self._project_id,
            dataset_id_omop=self._dataset_id_omop,
            min_custom_concept_id=Etl._CUSTOM_CONCEPT_IDS_START,
        )
        self._gcp.run_query_job(sql)

    def _remove_custom_concepts_from_vocabulary_table(self) -> None:
        """Remove the custom concepts from the OMOP vocabulary table"""
        template = self._template_env.get_template(
            "cleanup/VOCABULARY_remove_custom_concepts.sql.jinja"
        )
        sql = template.render(
            project_id=self._project_id,
            dataset_id_omop=self._dataset_id_omop,
            min_custom_concept_id=Etl._CUSTOM_CONCEPT_IDS_START,
        )
        self._gcp.run_query_job(sql)

    def _remove_custom_concepts_from_concept_table_using_usagi_table(
        self, omop_table: str, concept_id_column: str
    ) -> None:
        """Remove the custom concepts of a specific concept column of a specific OMOP table from the OMOP concept table

        Args:
            omop_table (str): The omop table
            concept_id_column (str): The conept id column
        """  # noqa: E501 # pylint: disable=line-too-long
        template = self._template_env.get_template(
            "cleanup/CONCEPT_remove_custom_concepts_by_{omop_table}__{concept_id_column}_usagi_table.sql.jinja"
        )
        sql = template.render(
            project_id=self._project_id,
            dataset_id_omop=self._dataset_id_omop,
            dataset_id_work=self._dataset_id_work,
            min_custom_concept_id=Etl._CUSTOM_CONCEPT_IDS_START,
            omop_table=omop_table,
            concept_id_column=concept_id_column,
        )
        try:
            self._gcp.run_query_job(sql)
        except NotFound:
            logging.debug(
                "Table %s__%s_usagi_table not found in work dataset",
                omop_table,
                concept_id_column,
            )

    def _remove_omop_ids_from_map_table(self, omop_table: str) -> None:
        """Remove the mapping of source to omop id's from the SOURCE_ID_TO_OMOP_ID_MAP for a specific OMOP table.

        Args:
            omop_table (str): The omop table
        """  # noqa: E501 # pylint: disable=line-too-long
        template = self._template_env.get_template(
            "cleanup/SOURCE_ID_TO_OMOP_ID_MAP_remove_ids_by_omop_table.sql.jinja"
        )
        sql = template.render(
            project_id=self._project_id,
            dataset_id_omop=self._dataset_id_omop,
            omop_table=omop_table,
        )
        self._gcp.run_query_job(sql)

    def _remove_custom_concepts_from_concept_relationship_table_using_usagi_table(
        self, omop_table: str, concept_id_column: str
    ) -> None:
        """Remove the custom concepts of a specific concept column of a specific OMOP table from the OMOP concept_relationship table

        Args:
            omop_table (str): The omop table
            concept_id_column (str): The conept id column
        """  # noqa: E501 # pylint: disable=line-too-long
        template = self._template_env.get_template(
            "cleanup/CONCEPT_RELATIONSHIP_remove_custom_concepts_by_{omop_table}__{concept_id_column}_usagi_table.sql.jinja"  # noqa: E501 # pylint: disable=line-too-long
        )
        sql = template.render(
            project_id=self._project_id,
            dataset_id_omop=self._dataset_id_omop,
            dataset_id_work=self._dataset_id_work,
            min_custom_concept_id=Etl._CUSTOM_CONCEPT_IDS_START,
            omop_table=omop_table,
            concept_id_column=concept_id_column,
        )
        try:
            self._gcp.run_query_job(sql)
        except NotFound:
            logging.debug(
                "Table %s__%s_usagi_table not found in work dataset",
                omop_table,
                concept_id_column,
            )

    def _remove_custom_concepts_from_concept_ancestor_table_using_usagi_table(
        self, omop_table: str, concept_id_column: str
    ) -> None:
        """Remove the custom concepts of a specific concept column of a specific OMOP table from the OMOP concept_ancestor table

        Args:
            omop_table (str): The omop table
            concept_id_column (str): The conept id column
        """  # noqa: E501 # pylint: disable=line-too-long
        template = self._template_env.get_template(
            "cleanup/CONCEPT_ANCESTOR_remove_custom_concepts_by_{omop_table}__{concept_id_column}_usagi_table.sql.jinja"  # noqa: E501 # pylint: disable=line-too-long
        )
        sql = template.render(
            project_id=self._project_id,
            dataset_id_omop=self._dataset_id_omop,
            dataset_id_work=self._dataset_id_work,
            min_custom_concept_id=Etl._CUSTOM_CONCEPT_IDS_START,
            omop_table=omop_table,
            concept_id_column=concept_id_column,
        )
        try:
            self._gcp.run_query_job(sql)
        except NotFound:
            logging.debug(
                "Table %s__%s_usagi_table not found in work dataset",
                omop_table,
                concept_id_column,
            )

    def _remove_custom_concepts_from_vocabulary_table_using_usagi_table(
        self, omop_table: str, concept_id_column: str
    ) -> None:
        """Remove the custom concepts of a specific concept column of a specific OMOP table from the OMOP vocabulary table

        Args:
            omop_table (str): The omop table
            concept_id_column (str): The conept id column
        """  # noqa: E501 # pylint: disable=line-too-long
        template = self._template_env.get_template(
            "cleanup/VOCABULARY_remove_custom_concepts_by_{omop_table}__{concept_id_column}_usagi_table.sql.jinja"
        )
        sql = template.render(
            project_id=self._project_id,
            dataset_id_omop=self._dataset_id_omop,
            dataset_id_work=self._dataset_id_work,
            min_custom_concept_id=Etl._CUSTOM_CONCEPT_IDS_START,
            omop_table=omop_table,
            concept_id_column=concept_id_column,
        )
        try:
            self._gcp.run_query_job(sql)
        except NotFound:
            logging.debug(
                "Table %s__%s_usagi_table not found in work dataset",
                omop_table,
                concept_id_column,
            )

    def _remove_source_to_concept_map_using_usagi_table(
        self, omop_table: str, concept_id_column: str
    ) -> None:
        """Remove the concepts of a specific concept column of a specific OMOP table from the OMOP source_to_concept_map table

        Args:
            omop_table (str): The omop table
            concept_id_column (str): The conept id column
        """  # noqa: E501 # pylint: disable=line-too-long
        template = self._template_env.get_template(
            "cleanup/SOURCE_TO_CONCEPT_MAP_remove_concepts_by_{omop_table}__{concept_id_column}_usagi_table.sql.jinja"
        )
        sql = template.render(
            project_id=self._project_id,
            dataset_id_omop=self._dataset_id_omop,
            dataset_id_work=self._dataset_id_work,
            min_custom_concept_id=Etl._CUSTOM_CONCEPT_IDS_START,
            omop_table=omop_table,
            concept_id_column=concept_id_column,
        )
        self._gcp.run_query_job(sql)

    def _delete_work_table(self, work_table: str) -> None:
        """Remove  work table

        Args:
            work_table (str): The work table
        """
        table_id = f"{self._project_id}.{self._dataset_id_work}.{work_table}"
        logging.info("Deleting table '%s'", table_id)
        self._gcp.delete_table(self._project_id, self._dataset_id_work, work_table)

    def _load_vocabulary_in_upload_table(
        self, csv_file: Path, vocabulary_table: str
    ) -> None:
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
            self._project_id,
            self._dataset_id_work,
            vocabulary_table,
            write_disposition=bq.WriteDisposition.WRITE_EMPTY,  # , schema
        )

    def _clear_vocabulary_upload_table(self, vocabulary_table: str) -> None:
        """Removes a specific standardised vocabulary table

        Args:
            vocabulary_table (str): The standardised vocabulary table
        """
        logging.debug("Deleting vocabulary table %s", vocabulary_table)
        self._gcp.delete_table(
            self._project_id, self._dataset_id_work, vocabulary_table
        )

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
            temp = pc.strptime(
                tab.column(date_column[1]), format="%Y%m%d", unit="s"
            ).cast(pa.date64())
            tab = tab.set_column(date_column[0], date_column[1], temp)

        return tab

    def _get_vocabulary_schema(
        self, vocabulary_table: str
    ) -> Tuple[pa.Schema, List[Tuple[int, str]]]:
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
            table_ddl = next(
                (
                    tab
                    for tab in self._parsed_ddl
                    if tab["table_name"] == vocabulary_table
                )
            )
        except StopIteration as si_err:
            raise Exception(f"{vocabulary_table} not found in ddl") from si_err
        schema = [(col["name"], _to_pa(col["type"])) for col in table_ddl["columns"]]
        schema = pa.schema(schema)

        date_columns = [
            (idx, col["name"])
            for idx, col in enumerate(table_ddl["columns"])
            if col["type"] == "date"
        ]

        return (schema, date_columns)

    def _recreate_vocabulary_table(self, vocabulary_table: str) -> None:
        """Recreates a specific standardised vocabulary table

        Args:
            vocabulary_table (str): The standardised vocabulary table
        """
        template = self._template_env.get_template(
            "vocabulary_table_recreate.sql.jinja"
        )
        sql = template.render(
            project_id=self._project_id,
            dataset_id_omop=self._dataset_id_omop,
            dataset_id_work=self._dataset_id_work,
            vocabulary_table=vocabulary_table,
        )
        self._gcp.run_query_job(sql)

        self._gcp.set_clustering_fields_on_table(
            self._project_id,
            self._dataset_id_omop,
            vocabulary_table,
            self._clustering_fields[vocabulary_table],
        )

    def _create_omop_work_table(self, omop_table: str, events: Any) -> None:
        """Creates the OMOP work table (if it does'nt yet exists) based on the DDL.

        Args:
            omop_table (str): The OMOP table
            events (Any): Object that holds the events of the the OMOP table.
        """
        try:
            table_ddl = next(
                (tab for tab in self._parsed_ddl if tab["table_name"] == omop_table)
            )
        except StopIteration as si_err:
            raise Exception(f"{omop_table} not found in ddl") from si_err

        cluster_fields = (
            self._clustering_fields[omop_table]
            if omop_table in self._clustering_fields
            else []
        )

        template = self._template_env.get_template("create_omop_work_table.sql.jinja")
        sql = template.render(
            project_id=self._project_id,
            dataset_id_work=self._dataset_id_work,
            omop_table=omop_table,
            columns=table_ddl["columns"],
            events=events,
            cluster_fields=cluster_fields,
        )
        self._gcp.run_query_job(sql)

    def _custom_db_engine_cleanup(self, table: str) -> None:
        """Custom cleanup method for specific database engine implementation

        Args:
            table (str): Table name (all for all tables)
        """
        if table == "all":
            self._gcp.delete_from_bucket(f"{self._bucket_uri}")
        else:
            self._gcp.delete_from_bucket(f"{self._bucket_uri}/{table}")
