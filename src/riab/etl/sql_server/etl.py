# Copyright 2024 RADar-AZDelta
# SPDX-License-Identifier: gpl3+

import logging
from datetime import date
from pathlib import Path
from threading import Lock
from typing import Any, Optional

from polars import Config as pl_Config
from polars import col, from_dicts

from ..etl import Etl
from .ctes import extract_ctes
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

        self._lock_parse_sql = Lock()

    def _pre_etl(self, etl_tables: list[str]):
        """Stuff to do before the ETL (ex remove constraints on omop tables)

        Args:
            etl_tables (list[str]): list of etl tables, eif list is empty then all tables are processed
        """
        if self._disable_fk_constraints:
            return

        if not len(etl_tables):
            self._remove_all_constraints()
        else:
            for table in etl_tables:
                self._remove_constraints(table)

    def _post_etl(self, etl_tables: list[str]):
        """Stuff to do after the ETL (ex add constraints on omop tables)

        Args:
            etl_tables (list[str]): list of etl tables, eif list is empty then all tables are processed
        """
        if self._disable_fk_constraints:
            return

        if not len(etl_tables):
            self._add_all_constraints()
        else:
            for table in etl_tables:
                self._add_constraints(table)

    def _source_to_concept_map_update_invalid_reason(self, etl_start: date) -> None:
        """Cleanup old source to concept maps by setting the invalid_reason to deleted
        for all source to concept maps with a valid_start_date before the ETL start date.

        Args:
            etl_start (date): The start data of the ETL.
        """
        template = self._template_env.get_template("etl/SOURCE_TO_CONCEPT_MAP_update_invalid_reason.sql.jinja")
        sql = template.render(
            omop_database_catalog=self._omop_database_catalog,
            omop_database_schema=self._omop_database_schema,
        )
        self._db.run_query(sql, {"etl_start": etl_start})

    def _source_id_to_omop_id_map_update_invalid_reason(self, etl_start: date) -> None:
        """Cleanup old source id's to omop id's maps by setting the invalid_reason to deleted
        for all maps with a valid_start_date before the ETL start date.

        Args:
            etl_start (date): The start data of the ETL.
        """
        template = self._template_env.get_template("etl/SOURCE_ID_TO_OMOP_ID_MAP_update_invalid_reason.sql.jinja")
        sql = template.render(
            omop_database_catalog=self._omop_database_catalog,
            omop_database_schema=self._omop_database_schema,
        )
        self._db.run_query(sql, {"etl_start": etl_start})

    def _clear_custom_concept_upload_table(self, omop_table: str, concept_id_column: str) -> None:
        """Clears the custom concept upload table (holds the contents of the custom concept CSV's)

        Args:
            omop_table (str): The omop table
            concept_id_column (str): The conept id column
        """
        template = self._template_env.get_template("etl/{omop_work}_drop_table.sql.jinja")
        ddl = template.render(
            work_database_catalog=self._work_database_catalog,
            work_database_schema=self._work_database_schema,
            work_table=f"{omop_table}__{concept_id_column}_concept",
        )
        self._db.run_query(ddl)

    def _create_custom_concept_upload_table(self, omop_table: str, concept_id_column: str) -> None:
        """Creates the custom concept upload table (holds the contents of the custom concept CSV's)

        Args:
            omop_table (str): The omop table
            concept_id_column (str): The conept id column
        """
        template = self._template_env.get_template("etl/{omop_table}__{concept_id_column}_concept_create.sql.jinja")
        ddl = template.render(
            work_database_catalog=self._work_database_catalog,
            work_database_schema=self._work_database_schema,
            omop_table=omop_table,
            concept_id_column=concept_id_column,
        )
        self._db.run_query(ddl)

    def _create_custom_concept_id_swap_table(self) -> None:
        """Creates the custom concept id swap tabel (swaps between source value and the concept id)"""
        template = self._template_env.get_template("etl/CONCEPT_ID_swap_create.sql.jinja")
        ddl = template.render(
            work_database_catalog=self._work_database_catalog,
            work_database_schema=self._work_database_schema,
        )
        self._db.run_query(ddl)

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
        self._upload_parquet(
            self._work_database_catalog,
            self._work_database_schema,
            f"{omop_table}__{concept_id_column}_concept",
            parquet_file,
        )

    def _validate_custom_concepts(self, omop_table: str, concept_id_column: str) -> None:
        """Checks that the domain_id, vocabulary_id and concept_class_id columns of the custom concept contain valid values, that exists in our uploaded vocabulary."""
        template = self._template_env.get_template("etl/CONCEPT_custom_validate.sql.jinja")
        sql = template.render(
            omop_database_catalog=self._omop_database_catalog,
            omop_database_schema=self._omop_database_schema,
            work_database_catalog=self._work_database_catalog,
            work_database_schema=self._work_database_schema,
            omop_table=omop_table,
            concept_id_column=concept_id_column,
        )
        rows = self._db.run_query(sql)
        if rows:
            df = from_dicts(rows)
            with pl_Config(fmt_str_lengths=1000, tbl_cols=len(df.columns)):
                raise Exception(
                    f"Invalid domain_id, vocabulary_id or concept_class_id supplied in the custom concept CSV's for column '{concept_id_column}' of table '{omop_table}'\n{df}\n\n{sql}"
                )

        template = self._template_env.get_template("etl/CONCEPT_custom_validate_duplicates.sql.jinja")
        sql = template.render(
            work_database_catalog=self._work_database_catalog,
            work_database_schema=self._work_database_schema,
            omop_table=omop_table,
            concept_id_column=concept_id_column,
        )
        rows = self._db.run_query(sql)
        if rows:
            df = from_dicts(rows)
            with pl_Config(fmt_str_lengths=1000, tbl_cols=len(df.columns)):
                raise Exception(
                    f"Duplicate custom concepts supplied in the custom concept CSV's for column '{concept_id_column}' of table '{omop_table}'\n{df}\n\n{sql}"
                )

    def _give_custom_concepts_an_unique_id_above_2bilj(self, omop_table: str, concept_id_column: str) -> None:
        """Give the custom concepts an unique id (above 2.000.000.000) and store those id's
        in the concept id swap table.

        Args:
            omop_table (str): The omop table
            concept_id_column (str): The conept id column
        """
        template = self._template_env.get_template("etl/CONCEPT_ID_swap_merge.sql.jinja")
        sql = template.render(
            work_database_catalog=self._work_database_catalog,
            work_database_schema=self._work_database_schema,
            omop_table=omop_table,
            concept_id_column=concept_id_column,
            min_custom_concept_id=Etl._CUSTOM_CONCEPT_IDS_START,
        )
        self._db.run_query(sql)

    def _merge_custom_concepts_with_the_omop_concepts(self, omop_table: str, concept_id_column: str) -> None:
        """Merges the uploaded custom concepts in the OMOP concept table.

        Args:
            omop_table (str): The omop table
            concept_id_column (str): The conept id column
        """
        template = self._template_env.get_template("etl/CONCEPT_merge.sql.jinja")
        sql = template.render(
            omop_database_catalog=self._omop_database_catalog,
            omop_database_schema=self._omop_database_schema,
            work_database_catalog=self._work_database_catalog,
            work_database_schema=self._work_database_schema,
            omop_table=omop_table,
            concept_id_column=concept_id_column,
        )
        self._db.run_query(sql)

    def _clear_usagi_upload_table(self, omop_table: str, concept_id_column: str) -> None:
        """Clears the usagi upload table (holds the contents of the usagi CSV's)

        Args:
            omop_table (str): The omop table
            concept_id_column (str): The conept id column
        """
        template = self._template_env.get_template("etl/{omop_work}_drop_table.sql.jinja")
        sql = template.render(
            work_database_catalog=self._work_database_catalog,
            work_database_schema=self._work_database_schema,
            work_table=f"{omop_table}__{concept_id_column}_usagi",
        )
        self._db.run_query(sql)

    def _create_usagi_upload_table(self, omop_table: str, concept_id_column: str) -> None:
        """Creates the Usagi upload table (holds the contents of the Usagi CSV's)

        Args:
            omop_table (str): The omop table
            concept_id_column (str): The conept id column
        """
        template = self._template_env.get_template("etl/{omop_table}__{concept_id_column}_usagi_create.sql.jinja")
        ddl = template.render(
            work_database_catalog=self._work_database_catalog,
            work_database_schema=self._work_database_schema,
            omop_table=omop_table,
            concept_id_column=concept_id_column,
        )
        self._db.run_query(ddl)

    def _load_usagi_parquet_in_upload_table(self, parquet_file: str, omop_table: str, concept_id_column: str) -> None:
        """The Usagi CSV's are converted to a parquet file.
        This method loads the parquet file in a upload table.

        Args:
            parquet_file (Path): The path to the parquet file
            omop_table (str): The omop table
            concept_id_column (str): The conept id column
        """
        self._upload_parquet(
            self._work_database_catalog,
            self._work_database_schema,
            f"{omop_table}__{concept_id_column}_usagi",
            Path(parquet_file),
        )

    def _update_custom_concepts_in_usagi(self, omop_table: str, concept_id_column: str) -> None:
        """This method updates the Usagi upload table with with the generated custom concept ids (above 2.000.000.000).
        The concept_id column in the Usagi upload table is swapped by the generated custom concept_id (above 2.000.000.000).

        Args:
            omop_table (str): The omop table
            concept_id_column (str): The conept id column
        """
        template = self._template_env.get_template(
            "etl/{omop_table}__{concept_id_column}_usagi_update_custom_concepts.sql.jinja"
        )
        sql = template.render(
            work_database_catalog=self._work_database_catalog,
            work_database_schema=self._work_database_schema,
            omop_table=omop_table,
            concept_id_column=concept_id_column,
            process_semi_approved_mappings=self._process_semi_approved_mappings,
        )
        self._db.run_query(sql)

    def _store_usagi_source_value_to_concept_id_mapping(self, omop_table: str, concept_id_column: str) -> None:
        """Fill up the SOURCE_TO_CONCEPT_MAP table with all approved mappings from the uploaded Usagi CSV's

        Args:
            omop_table (str): The omop table
            concept_id_column (str): The conept id column
        """
        template = self._template_env.get_template("etl/SOURCE_TO_CONCEPT_MAP_check_for_duplicates.sql.jinja")
        sql_doubles = template.render(
            work_database_catalog=self._work_database_catalog,
            work_database_schema=self._work_database_schema,
            omop_table=omop_table,
            concept_id_column=concept_id_column,
            omop_database_catalog=self._omop_database_catalog,
            omop_database_schema=self._omop_database_schema,
            process_semi_approved_mappings=self._process_semi_approved_mappings,
        )
        rows = self._db.run_query(sql_doubles)
        if rows:
            df = from_dicts(rows)
            with pl_Config(fmt_str_lengths=1000):
                raise Exception(
                    f"Duplicate rows supplied (combination of source_code column and target_concept_id columns must be unique)!\nCheck for duplicate mappings in the Usagi CSV's and custom concept CSV's for column '{concept_id_column}' of table '{omop_table}'\n{df}"
                )

        template = self._template_env.get_template("etl/SOURCE_TO_CONCEPT_MAP_merge.sql.jinja")
        sql = template.render(
            work_database_catalog=self._work_database_catalog,
            work_database_schema=self._work_database_schema,
            omop_table=omop_table,
            concept_id_column=concept_id_column,
            omop_database_catalog=self._omop_database_catalog,
            omop_database_schema=self._omop_database_schema,
            process_semi_approved_mappings=self._process_semi_approved_mappings,
        )
        self._db.run_query(sql)

    def _store_usagi_source_id_to_omop_id_mapping(self, omop_table: str, primary_key_column: str) -> None:
        """Fill up the SOURCE_ID_TO_OMOP_ID_MAP table with all the swapped source id's to omop id's

        Args:
            omop_table (str): The omop table
            primary_key_column (str): The primary key column
        """
        template = self._template_env.get_template("etl/SOURCE_ID_TO_OMOP_ID_MAP_merge.sql.jinja")
        sql = template.render(
            work_database_catalog=self._work_database_catalog,
            work_database_schema=self._work_database_schema,
            omop_table=omop_table,
            primary_key_column=primary_key_column,
            omop_database_catalog=self._omop_database_catalog,
            omop_database_schema=self._omop_database_schema,
        )
        self._db.run_query(sql)

    def _get_query_from_sql_file(self, sql_file: Path, omop_table: str) -> str:
        """Reads the query from file. If it is a Jinja template, it renders the template.

        Args:
            sql_file (Path): Path to the sql or jinja file
            omop_table (str): _description_

        Returns:
            str: The query (if it is a Jinja template, the rendered query)
        """
        with open(sql_file, encoding="UTF8") as file:
            select_query = file.read()
            if Path(sql_file).suffix == ".jinja":
                template = self._template_env.from_string(select_query)
                select_query = template.render(
                    raw_database_catalog=self._raw_database_catalog,
                    raw_database_schema=self._raw_database_schema,
                    work_database_catalog=self._work_database_catalog,
                    work_database_schema=self._work_database_schema,
                    omop_database_catalog=self._omop_database_catalog,
                    omop_database_schema=self._omop_database_schema,
                    omop_table=omop_table,
                )
        return select_query

    def _query_into_upload_table(self, upload_table: str, select_query: str, omop_table: str) -> None:
        """This method inserts the results from our custom SQL queries the the work OMOP upload table.

        Args:
            upload_table (str): The work upload table
            select_query (str): The query
            omop_table (str): The omop table
        """
        self._lock_parse_sql.acquire()
        try:
            (ctes, remainder) = extract_ctes(select_query)
        finally:
            self._lock_parse_sql.release()

        columns = self._df_omop_fields.filter(col("cdmTableName").str.to_lowercase() == omop_table).rows(named=True)
        events = self._omop_event_fields[omop_table] if omop_table in self._omop_event_fields else {}
        primary_key_column = self._get_pk(omop_table)
        # concept_columns = [
        #     column["cdmFieldName"]
        #     for column in columns
        #     if "concept_id" in column["cdmFieldName"]  # and "source_concept_id" not in column
        # ]

        template = self._template_env.get_template("etl/{omop_table}_{sql_file}_insert.sql.jinja")
        sql = template.render(
            work_database_catalog=self._work_database_catalog,
            work_database_schema=self._work_database_schema,
            upload_table=upload_table,
            ctes=ctes,
            select_query=remainder,
            columns=columns,
            omop_table=omop_table,
            primary_key_column=primary_key_column,
            events=events,
            # concept_id_columns=concept_columns,
        )
        self._db.run_query(sql)

    def _create_pk_auto_numbering_swap_table(
        self, primary_key_column: str, concept_id_columns: list[str], events: Any
    ) -> None:
        """This method created a swap table so that our source codes can be translated to auto numbering primary keys.

        Args:
            primary_key_column (str): The primary key column of the OMOP table
            concept_id_columns (list[str]): List of concept_id columns
            events (Any): Object that holds the events of the the OMOP table.
        """
        template = self._template_env.get_template("etl/{primary_key_column}_swap_create.sql.jinja")
        ddl = template.render(
            work_database_catalog=self._work_database_catalog,
            work_database_schema=self._work_database_schema,
            primary_key_column=primary_key_column,
            # foreign_key_columns=vars(foreign_key_columns),
            concept_id_columns=concept_id_columns,
            events=events,
        )
        self._db.run_query(ddl)

    def _execute_pk_auto_numbering_swap_query(
        self,
        omop_table: str,
        primary_key_column: str,
        concept_id_columns: list[str],
        events: Any,
        sql_files: list[str],
        upload_tables: list[str],
    ) -> None:
        """This method does the swapping of our source codes to an auto number that will be the primary key
        of our OMOP table.

        Args:
            omop_table (str): The OMOP table
            primary_key_column (str): Primary key column
            concept_id_columns (list[str]): List of concept_id columns
            events (Any): Object that holds the events of the the OMOP table.
            sql_files (list[str]): List of upload SQL files
            upload_tables (list[str]): List of upload tables
        """
        template = self._template_env.get_template("etl/{primary_key_column}_swap_merge.sql.jinja")
        sql = template.render(
            work_database_catalog=self._work_database_catalog,
            work_database_schema=self._work_database_schema,
            primary_key_column=primary_key_column,
            concept_id_columns=concept_id_columns,
            omop_table=omop_table,
            events=events,
            sql_files=sql_files,
            upload_tables=upload_tables,
            process_semi_approved_mappings=self._process_semi_approved_mappings,
        )
        self._db.run_query(sql)

    def _check_for_duplicate_rows(
        self,
        omop_table: str,
        columns: list[str],
        upload_tables: list[str],
        primary_key_column: Optional[str],
        concept_id_columns: list[str],
        events: Any,
    ):
        """The one shot merge of the uploaded query result from the work table, with the swapped primary and foreign keys, the mapped Usagi concept and custom concepts in the destination OMOP table.

        Args:
            omop_table (str): OMOP table.
            columns (list[str]): List of columns of the OMOP table.
            upload_tables (list[str]): List of the upload tables to execute.
            primary_key_column (str): The name of the primary key column.
            concept_id_columns (list[str]): List of concept columns.
            events (Any): Object that holds the events of the the OMOP table.
        """  # noqa: E501 # pylint: disable=line-too-long
        template = self._template_env.get_template("etl/{omop_work_table}_merge_check_for_duplicate_rows.sql.jinja")
        sql_doubles = template.render(
            omop_table=omop_table,
            work_database_catalog=self._work_database_catalog,
            work_database_schema=self._work_database_schema,
            primary_key_column=primary_key_column,
            concept_id_columns=concept_id_columns,
            columns=columns,
            upload_tables=upload_tables,
            events=events,
        )
        rows = self._db.run_query(sql_doubles)
        if rows:
            df = from_dicts(rows)
            with pl_Config(fmt_str_lengths=1000):
                logging.warning(
                    f"Duplicate rows supplied (combination of id column and concept columns must be unique)! Check ETL queries for table '{omop_table}' and run the 'clean' command!\nQuery to get the duplicates:\n{sql_doubles}\n\n{df}"
                )

    def _merge_into_omop_table(
        self,
        omop_table: str,
        columns: list[str],
        upload_tables: list[str],
        required_columns: list[str],
        primary_key_column: Optional[str],
        pk_auto_numbering: bool,
        foreign_key_columns: Any,
        concept_id_columns: list[str],
        events: Any,
    ):
        """The one shot merge of the uploaded query result from the omop table, with the swapped primary and foreign keys, the mapped Usagi concept and custom concepts in the destination OMOP table.
        If the OMOP table has event columns, the merge will happen to a work table, and when all tables are done, a seperate ETL step will merge the work table into the OMOP table, with its event columns filled in.
        This is because event columns can point to almost any OMOP table, so first all tables must be done, before we can fill in the event columns.

        Args:
            omop_table (str): OMOP table.
            columns (list[str]): List of columns of the OMOP table.
            required_columns (list[str]): List of required columns of the OMOP table.
            primary_key_column (str): The name of the primary key column.
            pk_auto_numbering (bool): Is the primary key a generated incremental number?
            foreign_key_columns (Any): List of foreign key columns.
            concept_id_columns (list[str]): List of concept columns.
            events (Any): Object that holds the events of the the OMOP table.
        """  # noqa: E501 # pylint: disable=line-too-long
        if not events:
            self._remove_constraints(omop_table)

        template = self._template_env.get_template("etl/{omop_table}_merge.sql.jinja")
        sql = template.render(
            omop_database_catalog=self._omop_database_catalog,
            omop_database_schema=self._omop_database_schema,
            omop_table=omop_table,
            work_database_catalog=self._work_database_catalog,
            work_database_schema=self._work_database_schema,
            columns=columns,
            required_columns=required_columns,
            primary_key_column=primary_key_column,
            foreign_key_columns=foreign_key_columns,
            concept_id_columns=concept_id_columns,
            pk_auto_numbering=pk_auto_numbering,
            events=events,
            process_semi_approved_mappings=self._process_semi_approved_mappings,
            upload_tables=upload_tables,
            min_custom_concept_id=Etl._CUSTOM_CONCEPT_IDS_START,
        )
        self._db.run_query(sql)

        if not events:
            self._add_constraints(omop_table)

    def _merge_event_columns(
        self,
        omop_table: str,
        columns: list[str],
        primary_key_column: Optional[str],
        events: dict[str, str],
    ):
        """The one shot merge of OMOP table (that has event columns) applying the events.

        Args:
            sql_file (str): The sql file holding the query on the raw data.
            omop_table (str): OMOP table.
            columns (list[str]): List of columns of the OMOP table.
            primary_key_column (str): The name of the primary key column.
            events (Any): Object that holds the events of the the OMOP table.
        """  # noqa: E501 # pylint: disable=line-too-long
        if not events:
            return

        logging.info(
            "Merging work table '%s' into omop table '%s'",
            omop_table,
            omop_table,
        )

        self._remove_constraints(omop_table)
        event_tables = {}
        try:
            if not self._skip_event_fks_step and len(events) > 0:  # we have event columns
                template = self._template_env.get_template("etl/{omop_table}_get_event_tables.sql.jinja")
                sql = template.render(
                    omop_table=omop_table,
                    work_database_catalog=self._work_database_catalog,
                    work_database_schema=self._work_database_schema,
                    events=events,
                )
                rows = self._db.run_query(sql)
                if rows:
                    event_tables = dict(
                        (table, self._get_pk(table)) for table in (row["event_table"] for row in rows) if table
                    )

            template = self._template_env.get_template("etl/{omop_table}_apply_event_columns.sql.jinja")
            sql = template.render(
                omop_database_catalog=self._omop_database_catalog,
                omop_database_schema=self._omop_database_schema,
                omop_table=omop_table,
                work_database_catalog=self._work_database_catalog,
                work_database_schema=self._work_database_schema,
                columns=columns,
                primary_key_column=primary_key_column,
                events=events,
                event_tables=event_tables,
            )
            self._db.run_query(sql)
        except Exception as e:
            # if isinstance(e.__cause__, NotFound):  # chained exception!!!
            logging.debug(
                "Table %s not found in work dataset, continue without merge for this table",
                omop_table,
            )
        finally:
            self._add_constraints(omop_table)

    def _create_omop_work_table(self, omop_table: str, events: Any) -> None:
        """Creates the OMOP work table (if it does'nt yet exists) based on the DDL.

        Args:
            omop_table (str): The OMOP table
            events (Any): Object that holds the events of the the OMOP table.
        """
        if not (events or omop_table == "vocabulary"):
            return

        columns = self._df_omop_fields.filter(col("cdmTableName").str.to_lowercase() == omop_table).rows(named=True)

        template = self._template_env.get_template("etl/{omop_work}_ddl.sql.jinja")
        sql = template.render(
            work_database_catalog=self._work_database_catalog,
            work_database_schema=self._work_database_schema,
            omop_table=omop_table,
            columns=columns,
            events=events,
        )
        self._db.run_query(sql)

    def _check_usagi(self, omop_table: str, concept_id_column: str, domains: list[str] | None) -> None:
        """Checks the usagi fk domain of the concept id column.

        Args:
            omop_table (str): The omop table
            concept_id_column (str): The conept id column
            domains (list[str]): The allowed domains
        """
        template = self._template_env.get_template("etl/{omop_table}__{concept_id_column}_usagi_non_standard.sql.jinja")
        sql = template.render(
            work_database_catalog=self._work_database_catalog,
            work_database_schema=self._work_database_schema,
            omop_database_catalog=self._omop_database_catalog,
            omop_database_schema=self._omop_database_schema,
            omop_table=omop_table,
            concept_id_column=concept_id_column,
            process_semi_approved_mappings=self._process_semi_approved_mappings,
        )
        rows = self._db.run_query(sql)
        if rows:
            df = from_dicts(rows)
            logging.warn(
                f"Non-standard concepts found in the Usagi CSV's for concept column '{concept_id_column}' of OMOP table '{omop_table}'!\nOnly standard concepts are allowed!\nQuery to get the invalid domains:\n{sql}\nInvalid domains:\n{df}"
            )

        if domains:
            template = self._template_env.get_template(
                "etl/{omop_table}__{concept_id_column}_usagi_fk_domain_check.sql.jinja"
            )
            sql = template.render(
                work_database_catalog=self._work_database_catalog,
                work_database_schema=self._work_database_schema,
                omop_database_catalog=self._omop_database_catalog,
                omop_database_schema=self._omop_database_schema,
                omop_table=omop_table,
                concept_id_column=concept_id_column,
                domains=domains,
                process_semi_approved_mappings=self._process_semi_approved_mappings,
            )
            rows = self._db.run_query(sql)
            if rows:
                df = from_dicts(rows)
                raise Exception(
                    f"Invalid concept domains found in the Usagi CSV's for concept column '{concept_id_column}' of OMOP table '{omop_table}'!\nOnly concept domains ({', '.join(domains)}) are allowed!\nQuery to get the invalid domains:\n{sql}\nInvalid domains:\n{df}"
                )

    def _upload_riab_version_in_metadata_table(self) -> None:
        """Upload the riab version in the metadata table."""
        from importlib import metadata

        template = self._template_env.get_template("etl/cdm_metadata_riab_version.sql.jinja")
        sql = template.render(
            cdm_version=self._omop_cdm_version,
            riab_version=metadata.version("Rabbit-in-a-Blender"),
        )

        # load the results of the query in the tempopary work table
        self._query_into_upload_table("metadata__upload__riab_version", sql, "metadata")

    def _upload_cdm_folder_git_commit_hash_in_metadata_table(self) -> None:
        """Upload the cdm folder git commit hash in the metadata table."""
        if not self._cdm_folder_path:
            return

        self._git_cdm_folder_commit_hash = self._get_git_commmit_hash(self._cdm_folder_path)
        if not self._git_cdm_folder_commit_hash:
            return

        template = self._template_env.get_template("etl/cdm_metadata_git_commit_hash.sql.jinja")
        sql = template.render(
            cdm_version=self._omop_cdm_version,
            git_commit_hash=self._git_cdm_folder_commit_hash,
        )

        # load the results of the query in the tempopary work table
        self._query_into_upload_table("metadata__upload__git_commit_hash", sql, "metadata")
