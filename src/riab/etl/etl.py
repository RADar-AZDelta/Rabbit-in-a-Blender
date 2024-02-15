# Copyright 2024 RADar-AZDelta
# SPDX-License-Identifier: gpl3+

# pylint: disable=unsubscriptable-object
"""Holds the ETL abstract class"""
import json
import logging
import os
import tempfile
from abc import abstractmethod
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from pathlib import Path
from threading import Lock
from types import SimpleNamespace
from typing import Any, List, Optional, cast

import pyarrow as pa
import pyarrow.csv as csv
import pyarrow.parquet as pq

from .etl_base import EtlBase


class Etl(EtlBase):
    """
    ETL class that automates the extract-transfer-load process from source data to the OMOP common data model.
    """

    def __init__(
        self,
        only_omop_table: Optional[str] = None,
        only_query: Optional[str] = None,
        skip_usagi_and_custom_concept_upload: Optional[bool] = None,
        process_semi_approved_mappings: Optional[bool] = None,
        **kwargs,
    ):
        """Constructor
        The ETL will read the json with all the OMOP tables. Each OMOP table has a 'pk' (primary key), 'fks' (foreign keys) and 'events' property.

        Args:
            cdm_folder_path (str): The path to the OMOP folder structure that holds for each OMOP CDM table (folder) the ETL queries, Usagi CSV's and custom concept CSV's0
            only_omop_table (str): Only do ETL on this OMOP CDM table.
            only_query (str): Only do ETL for the specified sql file in the CDM folder structure. (ex: measurement/lab_measurements.sql)
            skip_usagi_and_custom_concept_upload (bool): If no changes have been made to the Usagi and custom concept CSV's, then you can speed up the ETL process by setting this flag to True. The ETL process will skip the upload and processing of the Usagi and custom concept CSV's.
        """  # noqa: E501 # pylint: disable=line-too-long
        super().__init__(**kwargs)

        self._only_omop_table = only_omop_table
        self._only_query: Optional[Path] = Path(only_query) if only_query else None
        self._skip_usagi_and_custom_concept_upload = skip_usagi_and_custom_concept_upload
        self._process_semi_approved_mappings = process_semi_approved_mappings

        with open(
            str(Path(__file__).parent.resolve() / "etl_flow.json"),
            "r",
            encoding="UTF8",
        ) as file:
            self._etl_flow = json.load(file, object_hook=lambda x: SimpleNamespace(**x))

        self._lock_custom_concepts = Lock()
        self._lock_source_value_to_concept_id_mapping = Lock()

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
            omop_table = self._only_query.parts[0]
            self._process_folder_to_work(omop_table, getattr(self._omop_tables, omop_table))
            self._process_work_to_omop(omop_table, getattr(self._omop_tables, omop_table))
            for omop_table, table_props in vars(self._omop_tables).items():
                if omop_table == self._only_omop_table:
                    continue
                events = vars(
                    getattr(
                        table_props,
                        "events",
                        json.loads("{}", object_hook=lambda d: SimpleNamespace(**d)),
                    )
                )
                if events:
                    self._process_work_to_omop(omop_table, table_props)
        elif self._only_omop_table:
            self._process_folder_to_work(self._only_omop_table, getattr(self._omop_tables, self._only_omop_table))
            self._process_work_to_omop(self._only_omop_table, getattr(self._omop_tables, self._only_omop_table))
            for omop_table, table_props in vars(self._omop_tables).items():
                if omop_table == self._only_omop_table:
                    continue
                events = vars(
                    getattr(
                        table_props,
                        "events",
                        json.loads("{}", object_hook=lambda d: SimpleNamespace(**d)),
                    )
                )
                if events:
                    self._process_work_to_omop(omop_table, table_props)
        else:
            self._do_folder_to_work_etl_flow(self._etl_flow)

            self._do_parallel_work_to_omop()

            # cleanup old source to concept maps by setting the invalid_reason to deleted
            # (we only do this when running a full ETL = all OMOP tables)
            self._source_to_concept_map_update_invalid_reason(etl_start)
            self._source_id_to_omop_id_map_update_invalid_reason(etl_start)

    def _do_parallel_work_to_omop(self):
        """Parallelize the mapping of the event columns to the correct foreign keys and fills up the final OMOP tables"""  # noqa: E501 # pylint: disable=line-too-long
        with ThreadPoolExecutor(max_workers=len(vars(self._omop_tables))) as executor:
            futures = [
                executor.submit(
                    self._process_work_to_omop,
                    omop_table,
                    table_props,
                )
                for omop_table, table_props in vars(self._omop_tables).items()
            ]
            # wait(futures, return_when=ALL_COMPLETED)
            for result in as_completed(futures):
                result.result()

    def _do_folder_to_work_etl_flow(self, elt_flow: Any):
        """Recursive function that parallelizes the processing of ETL folders

        Args:
            elt_flow (Any): The tree structure of the tabbel to tun in parallel and the dependent tables
        """
        with ThreadPoolExecutor(max_workers=len(elt_flow.tables)) as executor:
            futures = [
                executor.submit(
                    self._process_folder_to_work,
                    omop_table,
                    getattr(self._omop_tables, omop_table),
                )
                for omop_table in elt_flow.tables
            ]
            # wait(futures, return_when=ALL_COMPLETED)
            for result in as_completed(futures):
                result.result()

        if hasattr(elt_flow, "dependent"):
            self._do_folder_to_work_etl_flow(elt_flow.dependent)

    def _process_folder_to_work(self, omop_table: str, omop_table_props: Any):
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

        events = vars(
            getattr(
                omop_table_props,
                "events",
                json.loads("{}", object_hook=lambda d: SimpleNamespace(**d)),
            )
        )

        # create the OMOP work table based on the DDL, but with the event_id columns of type STRING
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
        pk_auto_numbering = self._is_pk_auto_numbering(omop_table, omop_table_props)

        if not self._skip_usagi_and_custom_concept_upload:
            with ThreadPoolExecutor(max_workers=len(concept_columns)) as executor:
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

        foreign_key_columns = getattr(
            omop_table_props,
            "fks",
            json.loads(
                "{}", object_hook=lambda d: SimpleNamespace(**d)
            ),  # create an empty SimpleNamespace object as default value
        )
        # replace foreign table with primary key of foreign table
        for foreign_key, foreign_table in vars(foreign_key_columns).items():
            setattr(
                foreign_key_columns,
                foreign_key,
                getattr(getattr(self._omop_tables, foreign_table), "pk"),
            )
        pk_swap_table_name = getattr(omop_table_props, "pk", None)

        if self._only_query:
            self._run_upload_query(cast(Path, self._cdm_folder_path) / self._only_query, omop_table)
        else:
            with ThreadPoolExecutor(max_workers=self._max_workers) as executor:
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

        if pk_auto_numbering:
            # swap the primary key with an auto number
            self._swap_primary_key_auto_numbering_column(
                omop_table=omop_table,
                pk_swap_table_name=cast(str, pk_swap_table_name),
                primary_key_column=omop_table_props.pk,
                concept_id_columns=concept_columns,
                events=events,
                sql_files=[Path(Path(sql_file).stem).stem for sql_file in sql_files],
            )
            # store the ID swap in our 'source_id_to_omop_id_swap' table
            self._store_usagi_source_id_to_omop_id_mapping(
                omop_table=omop_table,
                pk_swap_table_name=cast(str, pk_swap_table_name),
            )

        # usefull to combine all upload results into one large table
        self._combine_upload_tables(
            omop_table,
            [Path(Path(sql_file).stem).stem for sql_file in sql_files],
            columns=columns,
        )

        # merge everything in the destination OMOP work table
        logging.info(
            "Merging the upload queries into the omop work table '%s'",
            omop_table,
        )
        self._merge_into_omop_work_table(
            sql_files=[Path(Path(sql_file).stem).stem for sql_file in sql_files],
            omop_table=omop_table,
            columns=columns,
            required_columns=required_columns,
            pk_swap_table_name=pk_swap_table_name,
            primary_key_column=getattr(omop_table_props, "pk", None),
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
        logging.debug(
            "Running query '%s' from raw tables into table '%s'",
            str(sql_file),
            f"{omop_table}__upload__{Path(Path(sql_file).stem).stem}",
        )
        select_query = self._get_query_from_sql_file(sql_file, omop_table)

        # load the results of the query in the tempopary work table
        upload_table = f"{omop_table}__upload__{Path(Path(sql_file).stem).stem}"
        self._query_into_upload_table(upload_table, select_query)

    @abstractmethod
    def _combine_upload_tables(
        self,
        omop_table: str,
        sql_files: List[str],
        columns: List[str],
    ):
        """_summary_

        Args:
            omop_table_name (str): Name of the OMOP table
            sql_files (List[str]): sql_files (List[Path]): The sql files holding the query on the raw data.
        """
        pass

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

        ar_table = None
        for concept_csv_file in concept_csv_files:  # loop the custom concept CSV's
            logging.info(
                "Creating concept_id swap from custom concept file '%s'",
                str(concept_csv_file),
            )
            # convert the custom concepts CSV to an Arrow table
            ar_temp_table = self._convert_concept_csv_to_arrow_table(concept_csv_file)
            # concat the Arrow tables into one large Arrow table
            ar_table = ar_temp_table if not ar_table else pa.concat_tables([ar_table, ar_temp_table])
        if not ar_table:
            return
        with tempfile.TemporaryDirectory() as temp_dir:
            parquet_file = Path(temp_dir) / f"{omop_table}__{concept_id_column}_concept.parquet"
            # save the one large Arrow table in a Parquet file in a temporary directory
            pq.write_table(ar_table, str(parquet_file))
            # load the Parquet file into the specific custom concept upload table
            self._load_custom_concepts_parquet_in_upload_table(parquet_file, omop_table, concept_id_column)

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
            logging.debug(
                "Change type to INT64 in usagi table for column '%s' of table '%s'",
                concept_id_column,
                omop_table,
            )
            self._cast_concepts_in_usagi(omop_table, concept_id_column)
            logging.info(
                "No Usagi CSV's found for column '%s' of table '%s'",
                concept_id_column,
                omop_table,
            )
            return

        ar_table = None
        for usagi_csv_file in usagi_csv_files:  # loop all the Usagi CSV's
            logging.info("Creating concept_id swap from Usagi file '%s'", str(usagi_csv_file))
            # convert the CSV to an Arrow table
            ar_temp_table = self._convert_usagi_csv_to_arrow_table(usagi_csv_file)
            if len(ar_temp_table.group_by(["sourceCode", "conceptId"]).aggregate([("sourceCode", "count")])) != len(
                ar_temp_table
            ):
                logging.warning(
                    "Duplicates (combination of sourceCode and conceptId) in the Usagi CSV '%s'!",
                    usagi_csv_file,
                )
            # concat the Arrow tables into one large Arrow table
            ar_table = ar_temp_table if not ar_table else pa.concat_tables([ar_table, ar_temp_table])

        if ar_table:
            with tempfile.TemporaryDirectory() as temp_dir:
                parquet_file = os.path.join(temp_dir, f"{omop_table}__{concept_id_column}_usagi.parquet")
                # save the one large Arrow table in a Parquet file in a temporary directory
                pq.write_table(ar_table, parquet_file)
                # load the Parquet file into the specific usagi upload table
                self._load_usagi_parquet_in_upload_table(parquet_file, omop_table, concept_id_column)
            if len(ar_table.group_by(["sourceCode", "conceptId"]).aggregate([("sourceCode", "count")])) != len(
                ar_table
            ):
                logging.warning(
                    "Duplicates (combination of sourceCode and conceptId) in the Usagi CSV's for concept column '%s' of OMOP table '%s'!",  # noqa: E501 # pylint: disable=line-too-long
                    concept_id_column,
                    omop_table,
                )

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

        logging.debug(
            "Change type to INT64 in usagi table for column '%s' of table '%s'",
            concept_id_column,
            omop_table,
        )
        self._cast_concepts_in_usagi(omop_table, concept_id_column)

        if len(concept_csv_files):
            logging.info(
                "Adding the custom concepts to the usagi table for column '%s' of table '%s'",
                concept_id_column,
                omop_table,
            )
            # add the custom concepts with the concept id's and names using the previously filled up swap table
            # custom concepts will recieve the mapping status 'APPROVED'
            self._add_custom_concepts_to_usagi(omop_table, concept_id_column)

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

    def _process_work_to_omop(self, omop_table_name: str, omop_table_props: Any):
        """Maps the event columns to the correct foreign keys and fills up the final OMOP tables

        Args:
            omop_table_name (str): OMOP table
            omop_table_props (Any): Primary key, foreign key(s) and event(s) of the OMOP table
        """
        events = vars(
            getattr(
                omop_table_props,
                "events",
                json.loads("{}", object_hook=lambda d: SimpleNamespace(**d)),
            )
        )

        # get all the columns from the destination OMOP table
        columns = self._get_omop_column_names(omop_table_name)

        # merge everything in the destination OMOP work table
        logging.info(
            "Merging work table '%s' into omop table '%s'",
            omop_table_name,
            omop_table_name,
        )
        self._merge_into_omop_table(
            omop_table=omop_table_name,
            columns=columns,
            primary_key_column=getattr(omop_table_props, "pk", None),
            events=events,
        )

    def _swap_primary_key_auto_numbering_column(
        self,
        omop_table: str,
        pk_swap_table_name: str,
        primary_key_column: str,
        concept_id_columns: List[str],
        events: Any,
        sql_files: List[str],
    ):
        """Swap the primary key source value of the omop table with a generated incremental number.

        Args:
            omop_table (str): OMOP table.
            pk_swap_table_name (str): Name of the swap table, for generating the auto numbering.
            primary_key_column (str): The name of the primary key column.
            concept_id_columns (List[str]): List of the columns that hold concepts
            events (Any): Object that holds the events of the the OMOP table.
        """  # noqa: E501 # pylint: disable=line-too-long
        logging.debug(
            "Swapping primary key column '%s' for table '%s'",
            primary_key_column,
            omop_table,
        )
        # create the swap table for the primary key
        self._create_pk_auto_numbering_swap_table(pk_swap_table_name, concept_id_columns, events)

        # execute the swap query
        self._execute_pk_auto_numbering_swap_query(
            omop_table=omop_table,
            pk_swap_table_name=pk_swap_table_name,
            primary_key_column=primary_key_column,
            concept_id_columns=concept_id_columns,
            events=events,
            sql_files=sql_files,
        )

    @abstractmethod
    def _merge_into_omop_work_table(
        self,
        sql_files: List[str],
        omop_table: str,
        columns: List[str],
        required_columns: List[str],
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
            required_columns (List[str]): List of required columns of the OMOP table.
            pk_swap_table_name (str): The name of the swap table to convert the source value of the primary key to an auto number.
            primary_key_column (str): The name of the primary key column.
            pk_auto_numbering (bool): Is the primary key a generated incremental number?
            foreign_key_columns (Any): List of foreign key columns.
            concept_id_columns (List[str]): List of concept columns.
            events (Any): Object that holds the events of the the OMOP table.
        """  # noqa: E501 # pylint: disable=line-too-long
        pass

    def _convert_usagi_csv_to_arrow_table(self, usagi_csv_file: Path) -> pa.Table:
        """Converts a Usagi CSV file to an Arrow table, maintaining the relevant columns.

        Args:
            usagi_csv_file (str): Usagi CSV file

        Returns:
            pa.Table: Arrow table.
        """
        logging.debug("Converting Usagi csv '%s' to arrow table", str(usagi_csv_file))
        table = csv.read_csv(
            usagi_csv_file,
            parse_options=csv.ParseOptions(quote_char='"'),
            convert_options=csv.ConvertOptions(
                include_columns=[
                    "sourceCode",
                    "sourceName",
                    "mappingStatus",
                    "conceptId",
                    "conceptName",
                    "domainId",
                ],
                column_types={
                    "sourceCode": pa.string(),
                    "sourceName": pa.string(),
                    "mappingStatus": pa.string(),
                    "conceptId": pa.string(),
                    "conceptName": pa.string(),
                    "domainId": pa.string(),
                },
            ),
        )
        return table

    def _convert_concept_csv_to_arrow_table(self, concept_csv_file: Path) -> pa.Table:
        """Converts a custom concept CSV file to an Arrow table, containing the relevant columns.

        Args:
            concept_csv_file (str): Concept CSV file

        Returns:
            pa.Table: Arrow table
        """
        logging.debug("Converting Concept csv '%s' to arrow table", str(concept_csv_file))
        table = csv.read_csv(
            concept_csv_file,
            convert_options=csv.ConvertOptions(
                include_columns=[
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
                ],
                column_types={
                    "concept_id": pa.string(),
                    "concept_name": pa.string(),
                    "domain_id": pa.string(),
                    "vocabulary_id": pa.string(),
                    "concept_class_id": pa.string(),
                    "standard_concept": pa.string(),
                    "concept_code": pa.string(),
                    "valid_start_date": pa.date32(),  # can only custom parse with timestamp, not date
                    "valid_end_date": pa.date32(),  # can only custom parse with timestamp, not date
                    "invalid_reason": pa.string(),
                },
                # timestamp_parsers=[csv.ISO8601, '%Y-%m-%d', '%d/%m/%Y']
            ),
        )
        return table

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
    def _is_pk_auto_numbering(self, omop_table_name: str, omop_table_props: Any) -> bool:
        """Checks if the primary key of the OMOP table needs autonumbering.
        For example the [Person](https://ohdsi.github.io/CommonDataModel/cdm54.html#PERSON) table has an auto numbering primary key, the [Vocabulary](https://ohdsi.github.io/CommonDataModel/cdm54.html#VOCABULARY) table not.

        Args:
            omop_table_name (str): OMOP table
            omop_table_props (Any): Primary key, foreign key(s) and event(s) of the OMOP table

        Returns:
            bool: True if the PK needs autonumbering
        """  # noqa: E501 # pylint: disable=line-too-long
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
    def _add_custom_concepts_to_usagi(self, omop_table: str, concept_id_column: str) -> None:
        """The custom concepts are added to the upload Usagi table with status 'APPROVED'.

        Args:
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
    def _cast_concepts_in_usagi(self, omop_table: str, concept_id_column: str) -> None:
        """Because we swapped the concept_id column (that for custom concepts is initially loaded with
        the concept_code, and that's a string) in the Usagi upload table with the generated custom
        concept_id (above 2.000.000.000), we need to cast it from string to an integer.

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
    def _query_into_upload_table(self, upload_table: str, select_query: str) -> None:
        """This method inserts the results from our custom SQL queries the the work OMOP upload table.

        Args:
            upload_table (str): The work omop table
            select_query (str): The query
        """
        pass

    @abstractmethod
    def _create_pk_auto_numbering_swap_table(
        self, pk_swap_table_name: str, concept_id_columns: List[str], events: Any
    ) -> None:
        """This method created a swap table so that our source codes can be translated to auto numbering primary keys.

        Args:
            pk_swap_table_name (str): The name of our primary key swap table
            concept_id_columns (List[str]): List of concept_id columns
            events (Any): Object that holds the events of the the OMOP table.
        """
        pass

    @abstractmethod
    def _execute_pk_auto_numbering_swap_query(
        self,
        omop_table: str,
        pk_swap_table_name: str,
        primary_key_column: str,
        concept_id_columns: List[str],
        events: Any,
        sql_files: List[str],
    ) -> None:
        """This method does the swapping of our source codes to an auto number that will be the primary key
        of our OMOP table.

        Args:
            omop_table (str): The OMOP table
            pk_swap_table_name (str): Primary key swap table
            primary_key_column (str): Primary key column
            concept_id_columns (List[str]): List of concept_id columns
            events (Any): Object that holds the events of the the OMOP table.
            sql_files (List[str]): List of upload SQL files
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

    @abstractmethod
    def _store_usagi_source_id_to_omop_id_mapping(self, omop_table: str, pk_swap_table_name: str) -> None:
        """Fill up the SOURCE_ID_TO_OMOP_ID_MAP table with all the swapped source id's to omop id's

        Args:
            omop_table (str): The omop table
            pk_swap_table_name (str): The id swap work table
        """
        pass
