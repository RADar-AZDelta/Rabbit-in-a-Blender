# Copyright 2024 RADar-AZDelta
# SPDX-License-Identifier: gpl3+

# pylint: disable=unsubscriptable-object
"""Holds the ETL abstract class"""

import logging
import os
import platform
import tempfile
from abc import abstractmethod
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from pathlib import Path
from threading import Lock
from typing import Any, Optional, cast

import polars as pl

from .etl_base import EtlBase


class Etl(EtlBase):
    """
    ETL class that automates the extract-transfer-load process from source data to the OMOP common data model.
    """

    def __init__(
        self,
        only_omop_table: Optional[list[str]] = None,
        only_query: Optional[list[str]] = None,
        skip_usagi_and_custom_concept_upload: Optional[bool] = None,
        process_semi_approved_mappings: Optional[bool] = None,
        skip_event_fks_step: Optional[bool] = None,
        **kwargs,
    ):
        """Constructor
        The ETL will read the json with all the OMOP tables. Each OMOP table has a 'pk' (primary key), 'fks' (foreign keys) and 'events' property.

        Args:
            cdm_folder_path (str): The path to the OMOP folder structure that holds for each OMOP CDM table (folder) the ETL queries, Usagi CSV's and custom concept CSV's0
            only_omop_table (str): Only do specific ETL on this OMOP CDM table(s).
            only_query (str): Only do ETL for the specified sql file(s) in the CDM folder structure. (ex: measurement/lab_measurements.sql)
            skip_usagi_and_custom_concept_upload (bool): If no changes have been made to the Usagi and custom concept CSV's, then you can speed up the ETL process by setting this flag to True. The ETL process will skip the upload and processing of the Usagi and custom concept CSV's.
        """  # noqa: E501 # pylint: disable=line-too-long
        super().__init__(**kwargs)

        self._only_omop_table = only_omop_table
        self._only_query: Optional[list[Path]] = [Path(query) for query in only_query] if only_query else None
        self._skip_usagi_and_custom_concept_upload = skip_usagi_and_custom_concept_upload
        self._process_semi_approved_mappings = process_semi_approved_mappings
        self._skip_event_fks_step = skip_event_fks_step

        self._lock_custom_concepts = Lock()
        self._lock_source_value_to_concept_id_mapping = Lock()

        self._usagi_polars_schema: dict[str, pl.DataType] = {  # type: ignore
            "sourceCode": pl.Utf8,  # type: ignore
            "sourceName": pl.Utf8,  # type: ignore
            "mappingStatus": pl.Utf8,  # type: ignore
            "conceptId": pl.Int64,  # type: ignore
            "conceptName": pl.Utf8,  # type: ignore
            "domainId": pl.Utf8,  # type: ignore
        }

        self._custom_concepts_polars_schema: dict[str, pl.DataType] = {  # type: ignore
            "concept_id": pl.Int64,  # type: ignore
            "concept_name": pl.Utf8,  # type: ignore
            "domain_id": pl.Utf8,  # type: ignore
            "vocabulary_id": pl.Utf8,  # type: ignore
            "concept_class_id": pl.Utf8,  # type: ignore
            "standard_concept": pl.Utf8,  # type: ignore
            "concept_code": pl.Utf8,  # type: ignore
            "valid_start_date": pl.Date,  # type: ignore
            "valid_end_date": pl.Date,  # type: ignore
            "invalid_reason": pl.Utf8,  # type: ignore
        }

        self._git_cdm_folder_commit_hash = None

    def run(self):
        """
        Start the ETL process.\n
        The ETL loops all the OMOP tables.\n
        For each table it:
            - uploads and processes the custom concepts
            - uploads and processes the usagi mappings
            - merges the 'raw data' with the 'custom concepts CSV's' and 'Usagi mappings CSV's' (that are located in the omop folder) into the corresponding OMOP table. This is a one-shot operation, it either fails or succeeds as a whole.
        The omop folder has for each table a subfolder.\n
        In the table subfolder you place all the SQL queries that transforms the data from the source tables into the format of the destination OMOP table. The ETL process will automatically renumber/replace the 'primary', 'foreign keys' and 'concept id's', so you keep the source values in your query.\n
        In the table subfolder, there is per 'concept id' column a subfolder. In these 'concept id' subfolders, you place the Usagi CSV's. (filename ends with _usagi.csv)\n
        If you have 'custom concepts' for a specific 'concept id' column, than you create a 'custom' subfolder in the 'concept id' subfolder. Here the 'custom concepts' can be put into one or more CSV files (ending with _concept.csv). These CSV's have the same columns as the CONCEPT table.\n

        ex:\n
            omop/                               \n
            └ provider/-------------------------OMOP table folder\n
              ├ gender_concept_id/--------------concept folder\n
              │ ├ custom/-----------------------custom concept folder\n
              │ │ └ sex_concept.csv-------------csv with the custom concepts\n
              │ └ sex_usagi.csv-----------------Usagi csv with the mapped concepts\n
              ├ speciality_concept_id/----------concept folder\n
              │ └ speciality_usagi.csv----------Usagi csv with the mapped concepts\n
              └ doctor.sql----------------------SQL query to map raw data to omop table\n

        """  # noqa: E501 # pylint: disable=line-too-long
        etl_start = date.today()

        if self._only_query:
            d: dict[str, list[Path]] = defaultdict(list[Path])
            for k, v in [(path.parts[0], cast(Path, self._cdm_folder_path) / path) for path in self._only_query]:
                d[k].append(v)
            for omop_table, queries in d.items():
                self._process_omop_table(omop_table, queries)
                self._fill_in_event_columns_for_omop_table(omop_table)
        elif self._only_omop_table:
            for omop_table in self._only_omop_table:
                self._process_omop_table(omop_table)
                self._fill_in_event_columns_for_omop_table(omop_table)
        else:
            etl_flow = self._cdm_tables_fks_dependencies_resolved.copy()
            self._process_all_omop_tables(etl_flow)

            self._fill_in_event_columns_for_all_omop_tables()

            # cleanup old source to concept maps by setting the invalid_reason to deleted
            # (we only do this when running a full ETL = all OMOP tables)
            self._source_to_concept_map_update_invalid_reason(etl_start)
            self._source_id_to_omop_id_map_update_invalid_reason(etl_start)

    @abstractmethod
    def _pre_etl(self, etl_tables: list[str]):
        """Stuff to do before the ETL (ex remove constraints on omop tables)


        Args:
            etl_tables (list[str]): list of etl tables, eif list is empty then all tables are processed
        """
        pass

    @abstractmethod
    def _post_etl(self, etl_tables: list[str]):
        """Stuff to do after the ETL (ex add constraints on omop tables)

        Args:
            etl_tables (list[str]): list of etl tables, eif list is empty then all tables are processed
        """
        pass

    def _fill_in_event_columns_for_all_omop_tables(self):
        """Parallelize the mapping of the event columns to the correct foreign keys and fills up the final OMOP tables"""  # noqa: E501 # pylint: disable=line-too-long
        with ThreadPoolExecutor(max_workers=self._max_parallel_tables) as executor:
            futures = [
                executor.submit(self._fill_in_event_columns_for_omop_table, omop_table)
                for omop_table in self._omop_etl_tables
            ]
            # wait(futures, return_when=ALL_COMPLETED)
            for result in as_completed(futures):
                result.result()

    def _process_all_omop_tables(self, elt_flow: list[list[str]]):
        """Recursive function that parallelizes the processing of ETL tables

        Args:
            elt_flow (Any): The tree structure of the tabbel to tun in parallel and the dependent tables
        """
        tables = elt_flow.pop(0)
        with ThreadPoolExecutor(max_workers=self._max_parallel_tables) as executor:
            futures = [executor.submit(self._process_omop_table, omop_table) for omop_table in tables]
            # wait(futures, return_when=ALL_COMPLETED)
            for result in as_completed(futures):
                result.result()

        if len(elt_flow):
            self._process_all_omop_tables(elt_flow)

    def _process_omop_table(self, omop_table: str, only_queries: Optional[list[Path]] = None):
        """ETL method for one OMOP table

        Args:
            omop_table_name (str): Name of the OMOP table
            omop_table_props (Any): Object that holds, the pk (primary key), fks (foreign keys) and events of the the OMOP table.
        """  # noqa: E501 # pylint: disable=line-too-long
        omop_table_path = cast(Path, self._cdm_folder_path) / f"{omop_table}/"
        sql_files = [sql_file for suffix in ["*.sql", "*.sql.jinja"] for sql_file in omop_table_path.glob(suffix)]
        if not len(sql_files):
            logging.info(
                "No SQL files found in ETL folder '%s'",
                omop_table_path,
            )
            return

        logging.info("Processing ETL folder: %s", omop_table_path)

        events = self._omop_event_fields[omop_table] if omop_table in self._omop_event_fields else {}

        # create the OMOP work table (only if the table has event columns) based on the DDL, but with the event_id columns of type STRING
        self._create_omop_work_table(omop_table, events)

        # get all the columns from the destination OMOP table
        columns = self._get_omop_column_names(omop_table)
        concept_columns = [
            column
            for column in columns
            if "concept_id" in column  # and "source_concept_id" not in column
        ]
        required_columns = self._get_required_omop_column_names(omop_table)

        # is the primary key an auto numbering column?
        pk_auto_numbering = self._is_pk_auto_numbering(omop_table)

        if not self._skip_usagi_and_custom_concept_upload:
            with ThreadPoolExecutor(max_workers=self._max_worker_threads_per_table) as executor:
                # upload an apply the custom concept CSV's
                futures = [
                    executor.submit(
                        self._upload_custom_concepts,
                        omop_table,
                        concept_id_column.lower(),
                    )
                    for concept_id_column in concept_columns
                ]
                # wait(futures, return_when=ALL_COMPLETED)
                for result in as_completed(futures):
                    result.result()

            with ThreadPoolExecutor(max_workers=self._max_worker_threads_per_table) as executor:
                # upload and apply the Usagi CSV's
                futures = [
                    executor.submit(
                        self._apply_usagi_mapping,
                        omop_table,
                        concept_id_column.lower(),
                    )
                    for concept_id_column in concept_columns
                ]
                # wait(futures, return_when=ALL_COMPLETED)
                for result in as_completed(futures):
                    result.result()

        foreign_key_columns = self._get_fks(omop_table)
        primary_key_column = self._get_pk(omop_table)

        if only_queries:
            sql_files = only_queries

        if omop_table == "metadata":
            self._upload_riab_version_in_metadata_table()
            self._upload_cdm_folder_git_commit_hash_in_metadata_table()

        with ThreadPoolExecutor(max_workers=self._max_worker_threads_per_table) as executor:
            # upload an apply the custom concept CSV's
            futures = [
                executor.submit(
                    self._run_upload_query,
                    sql_file,
                    omop_table,
                )
                for sql_file in sql_files
            ]
            # wait(futures, return_when=ALL_COMPLETED)
            for result in as_completed(futures):
                result.result()

        upload_tables = [Path(Path(sql_file).stem).stem for sql_file in sql_files]  # remove file extensions
        if omop_table == "metadata":
            upload_tables.append("riab_version")
            if self._git_cdm_folder_commit_hash:
                upload_tables.append("git_commit_hash")

        if pk_auto_numbering:
            sql_files = [Path(sql_file).name for sql_file in sql_files]
            if omop_table == "metadata":
                sql_files.append("cdm_metadata_riab_version")
                if self._git_cdm_folder_commit_hash:
                    sql_files.append("cdm_metadata_git_commit_hash")

            # swap the primary key with an auto number
            self._swap_primary_key_auto_numbering_column(
                omop_table=omop_table,
                primary_key_column=cast(str, primary_key_column),
                concept_id_columns=concept_columns,
                events=events,
                sql_files=sql_files,
                upload_tables=upload_tables,
            )
            # store the ID swap in our 'source_id_to_omop_id_swap' table
            self._store_usagi_source_id_to_omop_id_mapping(
                omop_table=omop_table,
                primary_key_column=cast(str, primary_key_column),
            )

        # merge everything in the destination OMOP work table
        logging.info(
            "Check for duplicate rows in uploaded data for table '%s'",
            omop_table,
        )
        self._check_for_duplicate_rows(
            omop_table=omop_table,
            columns=columns,
            upload_tables=upload_tables,
            primary_key_column=primary_key_column,
            concept_id_columns=concept_columns,
            events=events,
        )

        logging.info(
            "Merging the upload queries into the omop table '%s'",
            omop_table,
        )
        self._merge_into_omop_table(
            omop_table=omop_table,
            columns=columns,
            upload_tables=upload_tables,
            required_columns=required_columns,
            primary_key_column=primary_key_column,
            pk_auto_numbering=pk_auto_numbering,
            foreign_key_columns=foreign_key_columns,
            concept_id_columns=concept_columns,
            events=events,
        )

    def _run_upload_query(
        self,
        sql_file: Path,
        omop_table: str,
    ):
        """Executes the query from the .sql file.
        The results are loaded in a temporary work table (which name will have the format {omop_table}_{sql_file_name}).
        The query must keep the source values for the primary key, foreign key(s) en concept ids.
        The ETL process will automatically replace the primary key source values with autonumbering.
        The foreign key(s) will be replaced by the ETL process with their corresponding autonumbers, that were generated by a previous ETL table. Therefor the sequence of the OMOP tables in the 'omop_tables' parameter of this class is extremely imortant!
        The source values in the concept_id columns will alse be automatically replaced by the ETL process with the mapped values from the supplied Usagi CSV's and the custom concept CSV's.

        Args:
            sql_file (str): The sql file holding the query on the raw data.
            omop_table (str): OMOP table.
        """  # noqa: E501 # pylint: disable=line-too-long
        upload_table = f"{omop_table}__upload__{Path(Path(sql_file).stem).stem}"  # remove file extension
        logging.debug(
            "Running query '%s' from raw tables into table '%s'",
            str(sql_file),
            upload_table,
        )
        select_query = self._get_query_from_sql_file(sql_file, omop_table)

        # load the results of the query in the tempopary work table
        self._query_into_upload_table(upload_table, select_query, omop_table)

    def _upload_custom_concepts(self, omop_table: str, concept_id_column: str):
        """Processes all the CSV files (ending with _concept.csv) under the 'custom' subfolder of the '{concept_id_column}' folder.
        The custom concept CSV's are loaded into one large Arrow table.
        The Arrow table is then saved to a Parquet file in a temp folder.
        The Parquet file is then loaded in a database upload table (in the work zone).
        The custom concepts are given an unique id (above 2.000.000.000), and are merged in the OMOP concept table.

        Args:
            omop_table (str): OMOP table.
            concept_id_column (str): Custom concept_id column.
        """  # noqa: E501 # pylint: disable=line-too-long

        concept_csv_files = list(
            (cast(Path, self._cdm_folder_path) / f"{omop_table}/{concept_id_column}/custom/").glob("*_concept.csv")
        )
        if not len(concept_csv_files):
            logging.info(
                "No custom concept CSV's found for column '%s' of table '%s'",
                concept_id_column,
                omop_table,
            )
            return

        logging.info(
            "Uploading custom concepts for column '%s' of table '%s'",
            concept_id_column,
            omop_table,
        )
        # clean up the custom concept upload table
        self._clear_custom_concept_upload_table(omop_table, concept_id_column)

        # create the Usagi table
        self._create_custom_concept_upload_table(omop_table, concept_id_column)

        # create the swap table
        self._create_custom_concept_id_swap_table()

        # ar_table = None
        df = pl.DataFrame()
        for concept_csv_file in concept_csv_files:  # loop the custom concept CSV's
            logging.info(
                "Creating concept_id swap from custom concept file '%s'",
                str(concept_csv_file),
            )
            # convert the custom concepts CSV to a DataFrame
            df_temp = self._convert_concept_csv_to_polars_dataframe(concept_csv_file)

            # concat the DataFrame into one large DataFrame
            df = df_temp if df.is_empty() else pl.concat([df, df_temp])

        # = ar_temp_table if not ar_table else pa.concat_tables([ar_table, ar_temp_table])
        if df.is_empty():
            return
        with tempfile.TemporaryDirectory(prefix="riab_") as temp_dir_path:
            if platform.system() == "Windows":
                import win32api

                temp_dir_path = win32api.GetLongPathName(temp_dir_path)

            parquet_file = Path(temp_dir_path) / f"{omop_table}__{concept_id_column}_concept.parquet"
            # save the one large DataFrame in a Parquet file in a temporary directory
            df.write_parquet(str(parquet_file))

            # load the Parquet file into the specific custom concept upload table
            self._load_custom_concepts_parquet_in_upload_table(parquet_file, omop_table, concept_id_column)

            # Check that the domain_id,vocabulary_id,concept_class_id of the custom concept exisits in our uploaded vocabulary
            self._validate_custom_concepts(omop_table, concept_id_column)

        logging.info(
            "Swapping the custom concept id's for for column '%s' of table '%s'",
            concept_id_column,
            omop_table,
        )

        self._lock_custom_concepts.acquire()
        try:
            # give the custom concepts an unique id (above 2.000.000.000) and store those id's in the swap table
            self._give_custom_concepts_an_unique_id_above_2bilj(omop_table, concept_id_column)

            logging.info(
                "Merging custom concept into CONCEPT table for column '%s' of table '%s'",
                concept_id_column,
                omop_table,
            )
            # merge the custom concepts with their uniquely created id's in the OMOP concept table
            self._merge_custom_concepts_with_the_omop_concepts(omop_table, concept_id_column)
        except Exception as ex:
            raise ex
        finally:
            self._lock_custom_concepts.release()

    @abstractmethod
    def _validate_custom_concepts(self, omop_table: str, concept_id_column: str) -> None:
        """Checks that the domain_id, vocabulary_id and concept_class_id columns of the custom concept contain valid values, that exists in our uploaded vocabulary."""
        pass

    def _apply_usagi_mapping(self, omop_table: str, concept_id_column: str):
        """Processes all the Usagi CSV files (ending with _usagi.csv) under the '{concept_id_column}' folder.
        The CSV's will be loaded to one large Arrow table, converted to Parquet, uploaded to an upload table.
        The source values will be swapped with their corresponding concept id's.
        The custom concepts will automatically recieve mapping status 'APPROVED'.
        All source values will be loaded in the SOURCE_TO_CONCEPT_MAP table.

        Args:
            omop_table (str): OMOP table.
            concept_id_column (str): Custom concept_id column.
        """  # noqa: E501 # pylint: disable=line-too-long

        usagi_csv_files = list(
            (cast(Path, self._cdm_folder_path) / f"{omop_table}/{concept_id_column}/").glob("*_usagi.csv")
        )

        logging.info(
            "Creating concept_id swap for column '%s' of table '%s'",
            concept_id_column,
            omop_table,
        )
        if len(usagi_csv_files):
            # clean up the usagi upload table
            self._clear_usagi_upload_table(omop_table, concept_id_column)

        # create the Usagi upload table
        self._create_usagi_upload_table(omop_table, concept_id_column)

        if not len(usagi_csv_files):
            logging.info(
                "No Usagi CSV's found for column '%s' of table '%s'",
                concept_id_column,
                omop_table,
            )
            return

        df = pl.DataFrame()
        for usagi_csv_file in usagi_csv_files:  # loop all the Usagi CSV's
            logging.info("Creating concept_id swap from Usagi file '%s'", str(usagi_csv_file))
            # convert the CSV to an Arrow table
            df_temp = self._convert_usagi_csv_to_polars_dataframe(usagi_csv_file)
            # only get the APPOVED concepts

            df_duplicates = (
                df_temp.filter(
                    pl.col("mappingStatus").is_in(
                        ["APPROVED", "SEMI-APPROVED"] if self._process_semi_approved_mappings else ["APPROVED"]
                    )
                )
                .group_by("sourceCode", "conceptId")
                .agg(count=pl.col("sourceCode").len())
                .filter(pl.col("count") > 1)
                .sort("count", descending=True)
            )
            if not df_duplicates.is_empty():
                logging.warning(
                    "Duplicates (combination of sourceCode and conceptId) in the Usagi CSV '%s'!\n%s",
                    usagi_csv_file,
                    df_duplicates,
                )

            # concat the Arrow tables into one large Arrow table
            df = df_temp if df.is_empty() else pl.concat([df, df_temp])

        if omop_table == "metadata" and concept_id_column == "metadata_concept_id":
            df_temp = pl.from_dicts(
                [
                    {
                        "sourceCode": f"RIAB_OMOPCDM{self._omop_cdm_version}",
                        "sourceName": f"OMOPCDM{self._omop_cdm_version}",
                        "mappingStatus": "APPROVED",
                        "conceptId": "756265",
                        "conceptName": "OMOP CDM Version 5.4.0",
                        "domainId": "Metadata",
                    },
                    {
                        "sourceCode": f"GIT_OMOPCDM{self._omop_cdm_version}",
                        "sourceName": f"OMOPCDM{self._omop_cdm_version}",
                        "mappingStatus": "APPROVED",
                        "conceptId": "756265",
                        "conceptName": "OMOP CDM Version 5.4.0",
                        "domainId": "Metadata",
                    },
                ],
                schema=self._usagi_polars_schema,
            )
            df = df_temp if df.is_empty() else pl.concat([df, df_temp])
        if omop_table == "metadata" and concept_id_column == "metadata_type_concept_id":
            df_temp = pl.from_dicts(
                [
                    {
                        "sourceCode": "RIAB_EHR",
                        "sourceName": "EHR",
                        "mappingStatus": "APPROVED",
                        "conceptId": "32817",
                        "conceptName": "EHR",
                        "domainId": "Type Concept",
                    },
                    {
                        "sourceCode": "GIT_EHR",
                        "sourceName": "EHR",
                        "mappingStatus": "APPROVED",
                        "conceptId": "32817",
                        "conceptName": "EHR",
                        "domainId": "Type Concept",
                    },
                ],
                schema=self._usagi_polars_schema,
            )
            df = df_temp if df.is_empty() else pl.concat([df, df_temp])

        if not df.is_empty():
            df_duplicates = (
                df.filter(
                    pl.col("mappingStatus").is_in(
                        ["APPROVED", "SEMI-APPROVED"] if self._process_semi_approved_mappings else ["APPROVED"]
                    )
                )
                .group_by("sourceCode", "conceptId")
                .agg(count=pl.col("sourceCode").len())
                .filter(pl.col("count") > 1)
                .sort("count", descending=True)
            )
            if not df_duplicates.is_empty():
                logging.warning(
                    "Duplicates (combination of sourceCode and conceptId) in the Usagi CSV's for concept column '%s' of OMOP table '%s'!",  # noqa: E501 # pylint: disable=line-too-long
                    concept_id_column,
                    omop_table,
                )

            with tempfile.TemporaryDirectory(prefix="riab_") as temp_dir_path:
                if platform.system() == "Windows":
                    import win32api

                    temp_dir_path = win32api.GetLongPathName(temp_dir_path)

                parquet_file = os.path.join(temp_dir_path, f"{omop_table}__{concept_id_column}_usagi.parquet")
                # save the one large Arrow table in a Parquet file in a temporary directory
                df.write_parquet(parquet_file)
                # load the Parquet file into the specific usagi upload table
                self._load_usagi_parquet_in_upload_table(parquet_file, omop_table, concept_id_column)

            fk_domains = self._get_fk_domains(omop_table)
            columns = self._get_omop_column_names(omop_table)
            concept_columns = [
                column
                for column in columns
                if "concept_id" in column  # and "source_concept_id" not in column
            ]
            # for column, domains in fk_domains.items():
            for concept_column in concept_columns:
                self._check_usagi(omop_table, concept_column, fk_domains.get(concept_column))

        concept_csv_files = list(
            (cast(Path, self._cdm_folder_path) / f"{omop_table}/{concept_id_column}/custom/").glob("*_concept.csv")
        )
        if len(concept_csv_files):
            logging.info(
                "Updating the custom concepts from code to assigned id in the usagi table for column '%s' of table '%s'",  # noqa: E501 # pylint: disable=line-too-long
                concept_id_column,
                omop_table,
            )
            self._update_custom_concepts_in_usagi(omop_table, concept_id_column)

        logging.info(
            "Merging mapped concepts into SOURCE_TO_CONCEPT_MAP table for column '%s' of table '%s'",
            concept_id_column,
            omop_table,
        )
        # fill up the SOURCE_TO_CONCEPT_MAP table with all approved mappings from the Usagi CSV's
        self._lock_source_value_to_concept_id_mapping.acquire()
        try:
            self._store_usagi_source_value_to_concept_id_mapping(omop_table, concept_id_column)
        except Exception as ex:
            raise ex
        finally:
            self._lock_source_value_to_concept_id_mapping.release()

    def _fill_in_event_columns_for_omop_table(self, omop_table: str):
        """Maps the event columns to the correct foreign keys and fills up the final OMOP tables

        Args:
            omop_table_name (str): OMOP table
            omop_table_props (Any): Primary key, foreign key(s) and event(s) of the OMOP table
        """
        omop_table_path = cast(Path, self._cdm_folder_path) / f"{omop_table}/"
        sql_files = [sql_file for suffix in ["*.sql", "*.sql.jinja"] for sql_file in omop_table_path.glob(suffix)]
        if not len(sql_files):
            return

        events = self._omop_event_fields.get(omop_table, {})

        # get all the columns from the destination OMOP table
        columns = self._get_omop_column_names(omop_table)

        primary_key_column = self._get_pk(omop_table)

        self._merge_event_columns(
            omop_table=omop_table,
            columns=columns,
            primary_key_column=primary_key_column,
            events=events,
        )

    def _swap_primary_key_auto_numbering_column(
        self,
        omop_table: str,
        primary_key_column: str,
        concept_id_columns: list[str],
        events: Any,
        sql_files: list[str],
        upload_tables: list[str],
    ):
        """Swap the primary key source value of the omop table with a generated incremental number.

        Args:
            omop_table (str): OMOP table.
            primary_key_column (str): The name of the primary key column.
            concept_id_columns (list[str]): List of the columns that hold concepts
            events (Any): Object that holds the events of the the OMOP table.
            sql_files (list[str]): List of the SQL files to execute.
            upload_tables (list[str]): List of the upload tables to execute.
        """  # noqa: E501 # pylint: disable=line-too-long
        logging.debug(
            "Swapping primary key column '%s' for table '%s'",
            primary_key_column,
            omop_table,
        )
        # create the swap table for the primary key
        self._create_pk_auto_numbering_swap_table(primary_key_column, concept_id_columns, events)

        # execute the swap query
        self._execute_pk_auto_numbering_swap_query(
            omop_table=omop_table,
            primary_key_column=primary_key_column,
            concept_id_columns=concept_id_columns,
            events=events,
            sql_files=sql_files,
            upload_tables=upload_tables,
        )

    @abstractmethod
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
        pass

    @abstractmethod
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
        pass

    def _convert_concept_csv_to_polars_dataframe(self, concept_csv_file: Path) -> pl.DataFrame:
        """Converts a custom concept CSV file to a Polars dataframe, containing the relevant columns.

        Args:
            concept_csv_file (Path): Concept CSV file

        Returns:
            pl.DataFrame: Polars dataframe
        """
        logging.debug("Converting Concept csv '%s' to polars dataframe", str(concept_csv_file))
        try:
            df = pl.read_csv(
                str(concept_csv_file),
                try_parse_dates=True,
                missing_utf8_is_empty_string=True,
                dtypes=self._custom_concepts_polars_schema,
            ).select(
                "concept_id",
                "concept_name",
                "domain_id",
                "vocabulary_id",
                "concept_class_id",
                "standard_concept",
                "concept_code",
                "valid_start_date",
                "valid_end_date",
                "invalid_reason",
            )
        except Exception as e:
            raise Exception(f"Failed converting concept csv '{concept_csv_file}' to parquet") from e
        return df

    def _convert_usagi_csv_to_polars_dataframe(self, usagi_csv_file: Path) -> pl.DataFrame:
        """Converts a Usagi CSV file to an Arrow table, maintaining the relevant columns.

        Args:
            usagi_csv_file (str): Usagi CSV file

        Returns:
            pa.Table: Arrow table.
        """
        logging.debug("Converting Usagi csv '%s' to polars DataFrame", str(usagi_csv_file))
        df = pl.read_csv(str(usagi_csv_file), dtypes=self._usagi_polars_schema).select(
            "sourceCode",
            "sourceName",
            "mappingStatus",
            "conceptId",
            "conceptName",
            "domainId",
        )
        return df

    @abstractmethod
    def _source_to_concept_map_update_invalid_reason(self, etl_start: date) -> None:
        """Cleanup old source to concept maps by setting the invalid_reason to deleted
        for all source to concept maps with a valid_start_date before the ETL start date.

        Args:
            etl_start (date): The start data of the ETL.
        """
        pass

    @abstractmethod
    def _source_id_to_omop_id_map_update_invalid_reason(self, etl_start: date) -> None:
        """Cleanup old source id's to omop id's maps by setting the invalid_reason to deleted
        for all maps with a valid_start_date before the ETL start date.

        Args:
            etl_start (date): The start data of the ETL.
        """
        pass

    @abstractmethod
    def _clear_custom_concept_upload_table(self, omop_table: str, concept_id_column: str) -> None:
        """Clears the custom concept upload table (holds the contents of the custom concept CSV's)

        Args:
            omop_table (str): The omop table
            concept_id_column (str): The conept id column
        """
        pass

    @abstractmethod
    def _create_custom_concept_upload_table(self, omop_table: str, concept_id_column: str) -> None:
        """Creates the custom concept upload table (holds the contents of the custom concept CSV's)

        Args:
            omop_table (str): The omop table
            concept_id_column (str): The conept id column
        """
        pass

    @abstractmethod
    def _create_custom_concept_id_swap_table(self) -> None:
        """Creates the custom concept id swap tabel (swaps between source value and the concept id)"""
        pass

    @abstractmethod
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

    @abstractmethod
    def _give_custom_concepts_an_unique_id_above_2bilj(self, omop_table: str, concept_id_column: str) -> None:
        """Give the custom concepts an unique id (above 2.000.000.000) and store those id's
        in the concept id swap table.

        Args:
            omop_table (str): The omop table
            concept_id_column (str): The conept id column
        """
        pass

    @abstractmethod
    def _merge_custom_concepts_with_the_omop_concepts(self, omop_table: str, concept_id_column: str) -> None:
        """Merges the uploaded custom concepts in the OMOP concept table.

        Args:
            omop_table (str): The omop table
            concept_id_column (str): The conept id column
        """
        pass

    @abstractmethod
    def _clear_usagi_upload_table(self, omop_table: str, concept_id_column: str) -> None:
        """Clears the usagi upload table (holds the contents of the usagi CSV's)

        Args:
            omop_table (str): The omop table
            concept_id_column (str): The conept id column
        """
        pass

    @abstractmethod
    def _create_usagi_upload_table(self, omop_table: str, concept_id_column: str) -> None:
        """Creates the Usagi upload table (holds the contents of the Usagi CSV's)

        Args:
            omop_table (str): The omop table
            concept_id_column (str): The conept id column
        """
        pass

    @abstractmethod
    def _load_usagi_parquet_in_upload_table(self, parquet_file: str, omop_table: str, concept_id_column: str) -> None:
        """The Usagi CSV's are converted to a parquet file.
        This method loads the parquet file in a upload table.

        Args:
            parquet_file (Path): The path to the parquet file
            omop_table (str): The omop table
            concept_id_column (str): The conept id column
        """
        pass

    @abstractmethod
    def _update_custom_concepts_in_usagi(self, omop_table: str, concept_id_column: str) -> None:
        """This method updates the Usagi upload table with with the generated custom concept ids (above 2.000.000.000).
        The concept_id column in the Usagi upload table is swapped by the generated custom concept_id (above 2.000.000.000).

        Args:
            omop_table (str): The omop table
            concept_id_column (str): The conept id column
        """
        pass

    @abstractmethod
    def _store_usagi_source_value_to_concept_id_mapping(self, omop_table: str, concept_id_column: str) -> None:
        """Fill up the SOURCE_TO_CONCEPT_MAP table with all approved mappings from the uploaded Usagi CSV's

        Args:
            omop_table (str): The omop table
            concept_id_column (str): The conept id column
        """
        pass

    @abstractmethod
    def _get_query_from_sql_file(self, sql_file: Path, omop_table: str) -> str:
        """Reads the query from file. If it is a Jinja template, it renders the template.

        Args:
            sql_file (Path): Path to the sql or jinja file
            omop_table (str): _description_

        Returns:
            str: The query (if it is a Jinja template, the rendered query)
        """
        pass

    @abstractmethod
    def _query_into_upload_table(self, upload_table: str, select_query: str, omop_table: str) -> None:
        """This method inserts the results from our custom SQL queries the the work OMOP upload table.

        Args:
            upload_table (str): The work upload table
            select_query (str): The query
            omop_table (str): The omop table
        """
        pass

    @abstractmethod
    def _create_pk_auto_numbering_swap_table(
        self, primary_key_column: str, concept_id_columns: list[str], events: Any
    ) -> None:
        """This method created a swap table so that our source codes can be translated to auto numbering primary keys.

        Args:
            primary_key_column (str): The primary key column of the OMOP table
            concept_id_columns (list[str]): List of concept_id columns
            events (Any): Object that holds the events of the the OMOP table.
        """
        pass

    @abstractmethod
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
        pass

    @abstractmethod
    def _create_omop_work_table(self, omop_table: str, events: Any) -> None:
        """Creates the OMOP work table (if it does'nt yet exists) based on the DDL.

        Args:
            omop_table (str): The OMOP table
            events (Any): Object that holds the events of the the OMOP table.
        """
        pass

    @abstractmethod
    def _merge_event_columns(
        self,
        omop_table: str,
        columns: list[str],
        primary_key_column: Optional[str],
        events: Any,
    ):
        """The one shot merge of OMOP table (that has event columns) applying the events.

        Args:
            sql_file (str): The sql file holding the query on the raw data.
            omop_table (str): OMOP table.
            columns (list[str]): List of columns of the OMOP table.
            primary_key_column (str): The name of the primary key column.
            events (Any): Object that holds the events of the the OMOP table.
        """  # noqa: E501 # pylint: disable=line-too-long
        pass

    @abstractmethod
    def _store_usagi_source_id_to_omop_id_mapping(self, omop_table: str, primary_key_column: str) -> None:
        """Fill up the SOURCE_ID_TO_OMOP_ID_MAP table with all the swapped source id's to omop id's

        Args:
            omop_table (str): The omop table
            primary_key_column (str): The primary key column
        """
        pass

    @abstractmethod
    def _check_usagi(self, omop_table: str, concept_id_column: str, domains: list[str] | None) -> bool:
        """Checks the usagi fk domain of the concept id column.

        Args:
            omop_table (str): The omop table
            concept_id_column (str): The conept id column
            domains (list[str]): The allowed domains
        """
        pass

    @abstractmethod
    def _upload_riab_version_in_metadata_table(self) -> None:
        """Upload the riab version in the metadata table."""
        pass

    @abstractmethod
    def _upload_cdm_folder_git_commit_hash_in_metadata_table(self) -> None:
        """Upload the cdm folder git commit hash in the metadata table."""
        pass
