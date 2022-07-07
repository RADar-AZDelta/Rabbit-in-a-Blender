# pylint: disable=unsubscriptable-object
"""Holds the ETL class"""
import glob
import json
import logging
import math
import os
import pathlib
import re
import tempfile
import zipfile
from abc import ABC, abstractmethod
from datetime import date
from pathlib import Path
from re import Match
from types import SimpleNamespace
from typing import Any, List, Optional, cast

import polars as pl
import pyarrow as pa
import pyarrow.csv as csv
import pyarrow.parquet as pq


class Etl(ABC):
    """
    ETL class that automates the extract-transfer-load process from source data to the OMOP common data model.
    """

    _CUSTOM_CONCEPT_IDS_START = f"{2 * math.pow(1000, 3):.0f}"

    def __init__(
        self,
        cdm_folder_path: str,
        only_omop_table: Optional[str] = None,
        skip_usagi_and_custom_concept_upload: Optional[bool] = None,
    ):
        """Constructor
        The ETL will read the json with all the OMOP tables. Each OMOP table has a 'pk' (primary key), 'fks' (foreign keys) and 'concepts' property.

        Args:
            only_omop_table (str): Only do ETL on this OMOP CDM table.


        ```
        """  # noqa: E501 # pylint: disable=line-too-lon
        self._cdm_folder_path = (
            os.path.abspath(cdm_folder_path) if cdm_folder_path else None
        )
        self._only_omop_table = only_omop_table
        self._skip_usagi_and_custom_concept_upload = (
            skip_usagi_and_custom_concept_upload
        )

        with open(
            os.path.join(
                pathlib.Path(__file__).parent.absolute(),
                "cdm_5.4_schema.json",
            ),
            encoding="UTF8",
        ) as file:
            self._omop_tables = json.load(
                file, object_hook=lambda x: SimpleNamespace(**x)
            )

    @abstractmethod
    def create_omop_db(self) -> None:
        pass

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

        if self._only_omop_table:
            self._process_omop_folder(
                self._only_omop_table, getattr(self._omop_tables, self._only_omop_table)
            )
        else:
            for omop_table, table_props in vars(self._omop_tables).items():
                self._process_omop_folder(omop_table, table_props)

        self._source_to_concept_map_update_invalid_reason(etl_start)

    def _process_omop_folder(self, omop_table_name: str, omop_table_props: Any):
        """ETL method for one OMOP table

        Args:
            omop_table_name (str): Name of the OMOP table
            omop_table_props (Any): Object that holds, the pk (primary key), fks (foreign keys) and concepts of the the OMOP table.
        """  # noqa: E501 # pylint: disable=line-too-long
        logging.info("OMOP table: %s", omop_table_name)
        # get all the columns from the destination OMOP table
        columns = self._get_column_names(omop_table_name)

        # is the primary key an auto numbering column?
        pk_auto_numbering = self._is_pk_auto_numbering(
            omop_table_name, omop_table_props
        )

        if not self._skip_usagi_and_custom_concept_upload:
            for concept_id_column in getattr(
                omop_table_props, "concepts", []
            ):  # loop all concept_id columns
                # upload an apply the custom concept CSV's
                self._upload_custom_concepts(omop_table_name, concept_id_column.lower())
                # upload and apply the Usagi CSV's
                self._apply_usagi_mapping(omop_table_name, concept_id_column.lower())

        for sql_file in (
            item
            for sublist in [
                sorted(
                    glob.glob(
                        os.path.join(
                            cast(str, self._cdm_folder_path),
                            f"{omop_table_name}/{e}",
                        )
                    )
                )
                for e in ("*.sql", "*.sql.jinja")
            ]
            for item in sublist
        ):  # loop the sql files
            # execute the sql file and store the results in a temporary work table
            self._execute_query_from_sql_file_and_store_results_in_work_table(
                sql_file, omop_table_name
            )

            if pk_auto_numbering:
                # swap the primary key with an auto number
                self._swap_primary_key_auto_numbering_column(
                    sql_file,
                    omop_table_name,
                    columns,
                    omop_table_props.pk,
                    pk_auto_numbering,
                    getattr(
                        omop_table_props,
                        "fks",
                        json.loads(
                            "{}", object_hook=lambda d: SimpleNamespace(**d)
                        ),  # create an empty SimpleNamespace object as default value
                    ),
                    getattr(omop_table_props, "concepts", []),
                )

            if omop_table_name == "fact_relationship":
                match = re.search(
                    r"^.+_(?P<first>.+)_(?P<second>.+)(?:[.]sql)(?:[.]jinja)?$",
                    os.path.basename(sql_file),
                )
                foreign_key_columns = {
                    "fact_id_1": {
                        "table": cast(Match, match).groups()[0],
                        "column": f"{cast(Match, match).groups()[0]}_id",
                    },
                    "fact_id_2": {
                        "table": cast(Match, match).groups()[1],
                        "column": f"{cast(Match, match).groups()[1]}_id",
                    },
                }
            else:
                foreign_key_columns = getattr(
                    omop_table_props,
                    "fks",
                    json.loads(
                        "{}", object_hook=lambda d: SimpleNamespace(**d)
                    ),  # create an empty SimpleNamespace object as default value
                )

            # merge everything in the destination OMOP table
            self._merge_into_omop_table(
                sql_file,
                omop_table_name,
                columns,
                getattr(omop_table_props, "pk", None),
                pk_auto_numbering,
                foreign_key_columns,
                getattr(omop_table_props, "concepts", []),
            )

    def _upload_custom_concepts(self, omop_table: str, concept_id_column: str):
        """Processes all the CSV files (ending with _concept.csv) under the 'custom' subfolder of the '{concept_id_column}' folder.
        The custom concept CSV's are loaded into one large Arrow table.
        The Arrow table is then saved to a Parquet file in a temp folder, and stored in a destination bucket/folder.
        The uploaded Parquet file is then loaded in a database table in the work zone.
        The custom concepts are given an unique id (above 2.000.000.000), and are merged in the OMOP concept table.

        Args:
            omop_table (str): OMOP table.
            concept_id_column (str): Custom concept_id column.
        """  # noqa: E501 # pylint: disable=line-too-long
        logging.info(
            "Creating custom concepts for column '%s' of table '%s'",
            concept_id_column,
            omop_table,
        )
        # clean up the custom concept upload table
        self._clear_custom_concept_upload_table(omop_table, concept_id_column)

        # create the swap table
        self._create_custom_concept_id_swap_table()

        ar_table = None
        for concept_csv_file in sorted(
            glob.glob(
                os.path.join(
                    cast(str, self._cdm_folder_path),
                    f"{omop_table}/{concept_id_column}/custom/*_concept.csv",
                )
            )
        ):  # loop the custon concept CSV's
            logging.info(
                "Creating concept_id swap from Usagi file '%s'", concept_csv_file
            )
            # convert the CSV to an Arrow table
            ar_temp_table = self._convert_concept_csv_to_arrow_table(concept_csv_file)
            # concat the Arrow tables into one large Arrow table
            ar_table = (
                ar_temp_table
                if not ar_table
                else pa.concat_tables([ar_table, ar_temp_table])
            )
        if not ar_table:
            return
        with tempfile.TemporaryDirectory() as temp_dir:
            parquet_file = os.path.join(
                temp_dir, f"{omop_table}__{concept_id_column}_concept.parquet"
            )
            # save the one large Arrow table in a Parquet file in a temporary directory
            pq.write_table(ar_table, parquet_file)
            # load the Parquet file into the specific custom concept upload table
            self._load_custom_concepts_parquet_in_upload_table(
                parquet_file, omop_table, concept_id_column
            )

        logging.info(
            "Swapping the custom concept id's for for column '%s' of table '%s'",
            concept_id_column,
            omop_table,
        )
        # give the custom concepts an unique id (above 2.000.000.000) and store those id's in the swap table
        self._give_custom_concepts_an_unique_id_above_2bilj(
            omop_table, concept_id_column
        )

        logging.info(
            "Merging custom concept into CONCEPT table for column '%s' of table '%s'",
            concept_id_column,
            omop_table,
        )
        # merge the custom concepts with their uniquely created id's in the OMOP concept table
        self._merge_custom_concepts_with_the_omop_concepts(
            omop_table, concept_id_column
        )

    def _apply_usagi_mapping(self, omop_table: str, concept_id_column: str):
        """Processes all the Usagi CSV files (ending with _usagi.csv) under the '{concept_id_column}' folder.
        The CSV's will be loaded to one large Arrow table, converted to Parquet, uploaded to a Cloud Storage Bucket, and finally loaded in corresponding Big Query table in work dataset.
        The source values will be swapped with their corresponding concept id's.
        The custom concepts will automatically recieve mapping status 'APPROVED'.
        All source values will be loaded in the SOURCE_TO_CONCEPT_MAP table.

        Args:
            omop_table (str): OMOP table.
            concept_id_column (str): Custom concept_id column.
        """  # noqa: E501 # pylint: disable=line-too-long
        logging.info(
            "Creating concept_id swap for column '%s' of table '%s'",
            concept_id_column,
            omop_table,
        )
        # clean up the usagi upload table
        self._clear_usagi_upload_table(omop_table, concept_id_column)

        # create the Usagi table
        self._create_usagi_upload_table(omop_table, concept_id_column)

        ar_table = None
        for usagi_csv_file in sorted(
            glob.glob(
                os.path.join(
                    cast(str, self._cdm_folder_path),
                    f"{omop_table}/{concept_id_column}/*_usagi.csv",
                )
            )
        ):  # loop all the Usagi CSV's
            logging.info(
                "Creating concept_id swap from Usagi file '%s'", usagi_csv_file
            )
            # convert the CSV to an Arrow table
            ar_temp_table = self._convert_usagi_csv_to_arrow_table(usagi_csv_file)
            # concat the Arrow tables into one large Arrow table
            ar_table = (
                ar_temp_table
                if not ar_table
                else pa.concat_tables([ar_table, ar_temp_table])
            )
        if not ar_table:
            return
        with tempfile.TemporaryDirectory() as temp_dir:
            parquet_file = os.path.join(
                temp_dir, f"{omop_table}__{concept_id_column}_usagi.parquet"
            )
            # save the one large Arrow table in a Parquet file in a temporary directory
            pq.write_table(ar_table, parquet_file)
            # load the Parquet file into the specific usagi upload table
            self._load_usagi_parquet_in_upload_table(
                parquet_file, omop_table, concept_id_column
            )

        logging.info(
            "Swapping the source values with the concept ids in usagi table for column '%s' of table '%s'",
            concept_id_column,
            omop_table,
        )
        # replace the source values with the concept id's and names using the previously filled up swap table
        # custom concepts will recieve the mapping status 'APPROVED'
        self._swap_usagi_source_value_for_concept_id(omop_table, concept_id_column)

        logging.info(
            "Merging mapped concepts into SOURCE_TO_CONCEPT_MAP table for column '%s' of table '%s'",
            concept_id_column,
            omop_table,
        )
        # fill up the SOURCE_TO_CONCEPT_MAP table with all approvrd mappings from the Usagi CSV's
        self._store_usagi_source_value_to_concept_id_mapping(
            omop_table, concept_id_column
        )

    def _execute_query_from_sql_file_and_store_results_in_work_table(
        self, sql_file: str, omop_table: str
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
        logging.info(
            "Running query '%s' from raw tables into table '%s'",
            sql_file,
            f"{omop_table}_{Path(Path(sql_file).stem).stem}",
        )
        select_query = self._get_query_from_sql_file(sql_file, omop_table)

        # load the results of the query in the tempopary work table
        work_table = f"{omop_table}_{Path(Path(sql_file).stem).stem}"
        self._query_into_work_table(work_table, select_query)

    def _swap_primary_key_auto_numbering_column(
        self,
        sql_file: str,
        omop_table: str,
        columns: List[str],
        primary_key_column: str,
        pk_auto_numbering: bool,
        foreign_key_columns: Any,
        concept_id_columns: List[str],
    ):
        """Swap the primary key source value of the omop table with a generated incremental number.

        Args:
            sql_file (str): The sql file holding the query on the raw data.
            omop_table (str): OMOP table.
            primary_key_column (str): The name of the primary key column.
        """  # noqa: E501 # pylint: disable=line-too-long
        logging.info(
            "Swapping primary key column '%s' for query '%s'",
            primary_key_column,
            sql_file,
        )
        # create the swap table for the primary key
        self._create_pk_auto_numbering_swap_table(
            primary_key_column, concept_id_columns
        )

        # execute the swap query
        work_table = f"{omop_table}_{Path(Path(sql_file).stem).stem}"
        self._execute_pk_auto_numbering_swap_query(
            omop_table,
            work_table,
            columns,
            primary_key_column,
            pk_auto_numbering,
            foreign_key_columns,
            concept_id_columns,
        )

    @abstractmethod
    def _merge_into_omop_table(
        self,
        sql_file: str,
        omop_table: str,
        columns: List[str],
        primary_key_column: Optional[str],
        pk_auto_numbering: bool,
        foreign_key_columns: Any,
        concept_id_columns: List[str],
    ):
        """The one shot merge of the uploaded query result from the work table, with the swapped primary and foreign keys, the mapped Usagi concept and custom concepts in the destination OMOP table.

        Args:
            sql_file (str): The sql file holding the query on the raw data.
            omop_table (str): OMOP table.
            columns (List[str]): List of columns of the OMOP table.
            primary_key_column (str): The name of the primary key column.
            pk_auto_numbering (bool): Is the primary key a generated incremental number?
            foreign_key_columns (Any): List of foreign key columns.
            concept_id_columns (List[str]): List of concept columns.
        """  # noqa: E501 # pylint: disable=line-too-long
        pass

    def _convert_usagi_csv_to_arrow_table(self, usagi_csv_file: str) -> pa.Table:
        """Converts a Usagi CSV file to an Arrow table, maintaining the relevant columns.

        Args:
            usagi_csv_file (str): Usagi CSV file

        Returns:
            pa.Table: Arrow table.
        """
        logging.info("Converting Usagi csv '%s' to arrow table", usagi_csv_file)
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
                    "conceptId": pa.int64(),
                    "conceptName": pa.string(),
                    "domainId": pa.string(),
                },
            ),
        )
        return table

    def _convert_concept_csv_to_arrow_table(self, concept_csv_file: str) -> pa.Table:
        """Converts a custom concept CSV file to an Arrow table, maintaining the relevant columns.

        Args:
            concept_csv_file (str): Concept CSV file

        Returns:
            pa.Table: Arrow table
        """
        logging.info("Converting Concept csv '%s' to arrow table", concept_csv_file)
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

    def cleanup(self, cleanup_table: str = "all"):
        """
        Cleanup the ETL process:\n
        All work tables in the work dataset are deleted.\n
        All 'clinical' and 'health system' tables in the omop dataset are truncated. (the ones configured in the omop_tables variable)\n
        The 'source_to_concept_map' table in the omop dataset is truncated.\n
        All custom concepts are removed from the 'concept', 'concept_relationship' and 'concept_ancestor' tables in the omop dataset.\n
        """  # noqa: E501 # pylint: disable=line-too-long
        work_tables = self._get_work_tables()
        # custom cleanup
        if cleanup_table == "all":
            logging.info("Truncate omop table 'source_to_concept_map'")
            self._truncate_omop_table("source_to_concept_map")

            logging.info(
                "Removing custom concepts from 'concept' table",
            )
            self._remove_custom_concepts_from_concept_table()

            logging.info(
                "Removing custom concepts from 'concept_relationship' table",
            )
            self._remove_custom_concepts_from_concept_relationship_table()

            logging.info(
                "Removing custom concepts from 'concept_ancestor' table",
            )
            self._remove_custom_concepts_from_concept_ancestor_table()
        else:
            for table_name in work_tables:
                if table_name.startswith(cleanup_table) and table_name.endswith(
                    "_concept"
                ):
                    omop_table = table_name.split("__")[0]
                    concept_id_column = table_name.split("__")[1].removesuffix(
                        "_concept"
                    )
                    logging.info(
                        "Removing custom concepts from '%s' based on values from '%s' CSV",
                        "concept",
                        f"{omop_table}__{concept_id_column}_concept",
                    )
                    self._remove_custom_concepts_from_concept_table_using_usagi_table(
                        omop_table, concept_id_column
                    )

                    logging.info(
                        "Removing custom concepts from '%s' based on values from '%s' CSV",
                        "concept_relationship",
                        f"{omop_table}__{concept_id_column}_usagi",
                    )
                    self._remove_custom_concepts_from_concept_relationship_table_using_usagi_table(
                        omop_table, concept_id_column
                    )

                    logging.info(
                        "Removing custom concepts from '%s' based on values from '%s' CSV",
                        "concept_ancestor",
                        f"{omop_table}__{concept_id_column}_usagi",
                    )
                    self._remove_custom_concepts_from_concept_ancestor_table_using_usagi_table(
                        omop_table, concept_id_column
                    )
                elif table_name.startswith(cleanup_table) and table_name.endswith(
                    "_usagi"
                ):
                    omop_table = table_name.split("__")[0]
                    concept_id_column = table_name.split("__")[1].removesuffix("_usagi")
                    logging.info(
                        "Removing source to comcept maps from '%s' based on values from '%s' CSV",
                        "source_to_concept_map",
                        f"{omop_table}__{concept_id_column}_usagi",
                    )
                    self._remove_source_to_concept_map_using_usagi_table(
                        omop_table, concept_id_column
                    )

        # delete work tables
        for table_name in work_tables:
            if cleanup_table == "all" or table_name.startswith(cleanup_table):
                self._delete_work_table(table_name)
        # truncate omop tables
        omop_tables = vars(self._omop_tables).keys()
        for table_name in (x for x in omop_tables if x not in ["vocabulary"]):
            if cleanup_table == "all" or table_name == cleanup_table:
                logging.info(
                    "Truncate table '%s'",
                    table_name,
                )
                self._truncate_omop_table(table_name)

    def import_vocabularies(self, path_to_zip_file: str):
        with zipfile.ZipFile(path_to_zip_file, "r") as zip_ref:
            with tempfile.TemporaryDirectory(
                prefix="omop_vocabularies_"
            ) as temp_dir_path:
                logging.info(
                    "Extracting vocabularies zip file '%s' to temporary dir '%s'",
                    path_to_zip_file,
                    temp_dir_path,
                )
                zip_ref.extractall(temp_dir_path)

                for vocabulary_table in [
                    "CONCEPT",
                    "CONCEPT_ANCESTOR",
                    "CONCEPT_CLASS",
                    "CONCEPT_RELATIONSHIP",
                    "CONCEPT_SYNONYM",
                    "DOMAIN",
                    "DRUG_STRENGTH",
                    "RELATIONSHIP",
                    "VOCABULARY",
                ]:
                    logging.info("Converting '%s.csv' to parquet", vocabulary_table)
                    csv_file = os.path.join(temp_dir_path, f"{vocabulary_table}.csv")
                    df = self._read_vocabulary_csv(vocabulary_table, csv_file)
                    parquet_file = os.path.join(
                        temp_dir_path, f"{vocabulary_table}.parquet"
                    )
                    df.write_parquet(
                        parquet_file  # , compression="snappy", statistics=True
                    )
                    logging.info(
                        "Loading '%s.parquet' into work table", vocabulary_table
                    )
                    self._clear_vocabulary_upload_table(vocabulary_table.lower())
                    self._load_vocabulary_parquet_in_upload_table(
                        parquet_file, vocabulary_table.lower()
                    )
                    self._merge_uploaded_vocabulary_table(vocabulary_table.lower())

    def _read_vocabulary_csv(
        self, vocabulary_table: str, csv_file: str
    ) -> pl.DataFrame:
        df = pl.read_csv(csv_file, has_header=True, sep="\t")
        match vocabulary_table:
            case "CONCEPT" | "CONCEPT_RELATIONSHIP":
                df["valid_start_date"] = (
                    df["valid_start_date"]
                    .cast(pl.Utf8)
                    .str.strptime(pl.Date, "%Y%m%d", strict=False)
                    .cast(pl.Date)
                )
                df["valid_end_date"] = (
                    df["valid_end_date"]
                    .cast(pl.Utf8)
                    .str.strptime(pl.Date, "%Y%m%d", strict=False)
                    .cast(pl.Date)
                )
        return df

    @abstractmethod
    def _source_to_concept_map_update_invalid_reason(self, etl_start: date) -> None:
        pass

    @abstractmethod
    def _get_column_names(self, omop_table_name: str) -> List[str]:
        pass

    @abstractmethod
    def _is_pk_auto_numbering(
        self, omop_table_name: str, omop_table_props: Any
    ) -> bool:
        pass

    @abstractmethod
    def _clear_custom_concept_upload_table(
        self, omop_table: str, concept_id_column: str
    ) -> None:
        pass

    @abstractmethod
    def _create_custom_concept_id_swap_table(self) -> None:
        pass

    @abstractmethod
    def _load_custom_concepts_parquet_in_upload_table(
        self, parquet_file: str, omop_table: str, concept_id_column: str
    ) -> None:
        pass

    @abstractmethod
    def _give_custom_concepts_an_unique_id_above_2bilj(
        self, omop_table: str, concept_id_column: str
    ) -> None:
        pass

    @abstractmethod
    def _merge_custom_concepts_with_the_omop_concepts(
        self, omop_table: str, concept_id_column: str
    ) -> None:
        pass

    @abstractmethod
    def _clear_usagi_upload_table(
        self, omop_table: str, concept_id_column: str
    ) -> None:
        pass

    @abstractmethod
    def _create_usagi_upload_table(
        self, omop_table: str, concept_id_column: str
    ) -> None:
        pass

    @abstractmethod
    def _load_usagi_parquet_in_upload_table(
        self, parquet_file: str, omop_table: str, concept_id_column: str
    ) -> None:
        pass

    @abstractmethod
    def _swap_usagi_source_value_for_concept_id(
        self, omop_table: str, concept_id_column: str
    ) -> None:
        pass

    @abstractmethod
    def _store_usagi_source_value_to_concept_id_mapping(
        self, omop_table: str, concept_id_column: str
    ) -> None:
        pass

    @abstractmethod
    def _get_query_from_sql_file(self, sql_file: str, omop_table: str) -> str:
        pass

    @abstractmethod
    def _query_into_work_table(self, work_table: str, select_query: str) -> None:
        pass

    @abstractmethod
    def _create_pk_auto_numbering_swap_table(
        self, primary_key_column: str, concept_id_columns: List[str]
    ) -> None:
        pass

    @abstractmethod
    def _execute_pk_auto_numbering_swap_query(
        self,
        omop_table: str,
        work_table: str,
        columns: List[str],
        primary_key_column: str,
        pk_auto_numbering: bool,
        foreign_key_columns: Any,
        concept_id_columns: List[str],
    ) -> None:
        pass

    @abstractmethod
    def _get_work_tables(self) -> List[str]:
        pass

    @abstractmethod
    def _truncate_omop_table(self, table_name: str) -> None:
        pass

    @abstractmethod
    def _remove_custom_concepts_from_concept_table(self) -> None:
        pass

    @abstractmethod
    def _remove_custom_concepts_from_concept_relationship_table(self) -> None:
        pass

    @abstractmethod
    def _remove_custom_concepts_from_concept_ancestor_table(self) -> None:
        pass

    @abstractmethod
    def _remove_custom_concepts_from_concept_table_using_usagi_table(
        self, omop_table: str, concept_id_column: str
    ) -> None:
        pass

    @abstractmethod
    def _remove_custom_concepts_from_concept_relationship_table_using_usagi_table(
        self, omop_table: str, concept_id_column: str
    ) -> None:
        pass

    @abstractmethod
    def _remove_custom_concepts_from_concept_ancestor_table_using_usagi_table(
        self, omop_table: str, concept_id_column: str
    ) -> None:
        pass

    @abstractmethod
    def _remove_source_to_concept_map_using_usagi_table(
        self, omop_table: str, concept_id_column: str
    ) -> None:
        pass

    @abstractmethod
    def _delete_work_table(self, work_table: str) -> None:
        pass

    @abstractmethod
    def _load_vocabulary_parquet_in_upload_table(
        self, parquet_file: str, vocabulary_table: str
    ) -> None:
        pass

    @abstractmethod
    def _clear_vocabulary_upload_table(self, vocabulary_table: str) -> None:
        pass

    @abstractmethod
    def _merge_uploaded_vocabulary_table(self, vocabulary_table: str) -> None:
        pass
