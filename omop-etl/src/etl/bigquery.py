# pylint: disable=unsubscriptable-object
"""Holds the ETL class"""
import glob
import json
import logging
import math
import os
import sys
import tempfile
from datetime import date
from pathlib import Path
from types import SimpleNamespace
from typing import Any, List

import google.cloud.bigquery as bq
import jinja2 as jj
import pyarrow as pa
import pyarrow.csv as csv
import pyarrow.parquet as pq
from jinja2.utils import select_autoescape

from .utils.gcp import Gcp


class Etl:
    """
    ETL class that automates the extract-transfer-load process from source data to the OMOP common data model.
    """

    _CUSTOM_CONCEPT_IDS_START = f"{2 * math.pow(1000, 3):.0f}"

    def __init__(
        self,
        gcp: Gcp,
        project_id: str,
        dataset_id_raw: str,
        dataset_id_work: str,
        dataset_id_omop: str,
        bucket_name: str,
        bucket_path: str,
        omop_tables: Any,
    ):
        """Constructor

        Args:
            gcp (Gcp): Google Cloud Provider object
            project_id (str): project ID in gcp
            dataset_id_raw (str): Big Query dataset ID that holds the raw tables
            dataset_id_work (str): Big Query dataset ID that holds the work tables
            dataset_id_omop (str): Big Query dataset ID that holds the omop tables
            bucket_name (str): Name of the Cloud Storage bucket
            bucket_path (str): The path in the bucket (directory) to store the Parquet file(s). These parquet files will be the converted and uploaded 'custom concept' CSV's and the Usagi CSV's.
            omop_tables (Any): The dictionary of the OMOP tables. Each OMOP table has a 'pk' (primary key), 'fks' (foreign keys) and 'concepts' property.


        ex::

            "PROVIDER": {\n
                "pk": "PROVIDER_ID",\n
                "fks": {\n
                    "CARE_SITE_ID": {\n
                        "table": "CARE_SITE",\n
                        "column": "CARE_SITE_ID"\n
                    }\n
                },\n
                "concepts": ["SPECIALTY_CONCEPT_ID", "GENDER_CONCEPT_ID"]\n
            }\n
        ```
        """  # noqa: E501 # pylint: disable=line-too-long
        self._gcp = gcp
        self._project_id = project_id
        self._dataset_id_raw = dataset_id_raw
        self._dataset_id_work = dataset_id_work
        self._dataset_id_omop = dataset_id_omop
        self._bucket_name = bucket_name
        self._bucket_path = bucket_path
        self._omop_tables = omop_tables

        template_dir = os.path.join(
            os.path.dirname(os.path.realpath(__file__)), "templates"
        )
        template_loader = jj.FileSystemLoader(searchpath=template_dir)
        self._template_env = jj.Environment(
            autoescape=select_autoescape(["sql"]), loader=template_loader
        )

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
            └ PROVIDER/-------------------------OMOP table folder\n
              ├ gender_concept_id/--------------concept folder\n
              │ ├ custom/-----------------------custom concept folder\n
              │ │ └ sex_concept.csv-------------csv with the custom concepts\n
              │ └ sex_usagi.csv-----------------Usagi csv with the mapped concepts\n
              ├ speciality_concept_id/----------concept folder\n
              │ └ speciality_usagi.csv----------Usagi csv with the mapped concepts\n
              └ doctor.sql----------------------SQL query to map raw data to omop table\n

        """  # noqa: E501 # pylint: disable=line-too-long
        self._gcp.create_dataset(self._project_id, self._dataset_id_work)

        etl_start = date.today()

        for omop_table, table_props in vars(self._omop_tables).items():
            self._process_omop_folder(omop_table, table_props)

        template = self._template_env.get_template(
            "SOURCE_TO_CONCEPT_MAP_update_invalid_reason.sql.jinja"
        )
        sql = template.render(
            project_id=self._project_id,
            dataset_id_omop=self._dataset_id_omop,
        )
        self._gcp.run_query_job(
            sql,
            query_parameters=[
                bq.ScalarQueryParameter("etl_start", "DATE", etl_start)
            ],
        )

    def _process_omop_folder(self, omop_table_name: str, omop_table_props: Any):
        """ETL method for one OMOP table

        Args:
            omop_table_name (str): Name of the OMOP table
            omop_table_props (Any): Object that holds, the pk (primary key), fks (foreign keys) and concepts of the the OMOP table.
        """  # noqa: E501 # pylint: disable=line-too-long
        logging.info("OMOP table: %s", omop_table_name)
        # get all the columns from the destination OMOP table
        columns = self._gcp.get_column_names(
            self._project_id, self._dataset_id_omop, omop_table_name
        )
        # get the primary key meta data from the the destination OMOP table
        pk_column_metadata = self._gcp.get_column_metadata(
            self._project_id,
            self._dataset_id_omop,
            omop_table_name,
            omop_table_props.pk,
        )
        # is the primary key an auto numbering column?
        pk_auto_numbering = pk_column_metadata.get("data_type") == "INT64"
        for concept_id_column in getattr(
            omop_table_props, "concepts", []
        ):  # loop all concept_id columns
            # upload an apply the custom concept CSV's
            self._upload_custom_concepts(omop_table_name, concept_id_column.lower())
            # upload and apply the Usagi CSV's
            self._swap_concept_id_column(omop_table_name, concept_id_column.lower())
        for sql_file in (
            item
            for sublist in [
                sorted(
                    glob.glob(
                        os.path.join(
                            os.path.abspath(os.path.dirname(sys.argv[0])),
                            f"../omop/{omop_table_name}/{e}",
                        )
                    )
                    # os.path.join(
                    #     os.path.abspath(os.path.dirname(__file__)),
                    #     f"../omop/{omop_table_name}/{e}",
                    # ))
                )
                for e in ("*.sql", "*.sql.jinja")
            ]
            for item in sublist
        ):  # loop the sql files
            # execute the sql file and store the results in a work table
            self._raw_to_work_query(sql_file, omop_table_name)
            if pk_auto_numbering:
                # swap the primary key with an auto number
                self._swap_primary_key_column(
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
            # merge everything in the destination OMOP table
            self._merge_into_omop_table(
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

    def _upload_custom_concepts(self, omop_table: str, concept_id_column: str):
        """Processes all the CSV files (ending with _concept.csv) under the 'custom' subfolder of the '{concept_id_column}' folder.
        The custom concept CSV's are loaded into one large Arrow table.
        The Arrow table is then saved to a Parquet file in a temp folder, and uploaded to a Google Cloud Storage bucket.
        The uploaded Parquet file is then loaded in a Big Query table in the work dataset.
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
        # clean up the work table
        self._gcp.delete_table(
            f"{self._project_id}.{self._dataset_id_work}.{omop_table}__{concept_id_column}_concept"
        )
        # create the swap table
        template = self._template_env.get_template("CONCEPT_ID_swap_create.sql.jinja")
        ddl = template.render(
            project_id=self._project_id,
            dataset_id_work=self._dataset_id_work,
        )
        self._gcp.run_query_job(ddl)

        ar_table = None
        for concept_csv_file in sorted(
            glob.glob(
                os.path.join(
                    os.path.abspath(os.path.dirname(sys.argv[0])),
                    f"../omop/{omop_table}/{concept_id_column}/custom/*_concept.csv",
                    # os.path.abspath(os.path.dirname(__file__)),
                    # f"../omop/{omop_table}/{concept_id_column}/custom/*_concept.csv",
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
            # upload the Parquet file to the Cloud Storage Bucket
            self._gcp.upload_file_to_bucket(
                parquet_file, self._bucket_name, self._bucket_path
            )
            # load the uploaded Parquet file from the bucket into the specific custom concept table in the work dataset
            self._gcp.batch_load_from_bucket_into_bigquery_table(
                f"gs://{self._bucket_name}/{self._bucket_path}/{omop_table}__{concept_id_column}_concept.parquet",
                f"{self._project_id}.{self._dataset_id_work}.{omop_table}__{concept_id_column}_concept",
            )

        logging.info(
            "Swapping the custom concept id's for for column '%s' of table '%s'",
            concept_id_column,
            omop_table,
        )
        # give the custom concepts an unique id (above 2.000.000.000) and store those id's in the swap table
        template = self._template_env.get_template("CONCEPT_ID_swap_merge.sql.jinja")
        sql = template.render(
            project_id=self._project_id,
            dataset_id_work=self._dataset_id_work,
            omop_table=omop_table,
            concept_id_column=concept_id_column,
            min_custom_concept_id=Etl._CUSTOM_CONCEPT_IDS_START,
        )
        self._gcp.run_query_job(sql)

        logging.info(
            "Merging custom concept into CONCEPT table for column '%s' of table '%s'",
            concept_id_column,
            omop_table,
        )
        # merge the custom concepts with their uniquely created id's in the OMOP concept table
        template = self._template_env.get_template("CONCEPT_merge.sql.jinja")
        sql = template.render(
            project_id=self._project_id,
            dataset_id_omop=self._dataset_id_omop,
            dataset_id_work=self._dataset_id_work,
            omop_table=omop_table,
            concept_id_column=concept_id_column,
        )
        self._gcp.run_query_job(sql)

    def _swap_concept_id_column(self, omop_table: str, concept_id_column: str):
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
        # clean up the work table
        self._gcp.delete_table(
            f"{self._project_id}.{self._dataset_id_work}.{omop_table}__{concept_id_column}_usagi"
        )
        # create the Usagi table
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
        ar_table = None
        for usagi_csv_file in sorted(
            glob.glob(
                os.path.join(
                    os.path.abspath(os.path.dirname(sys.argv[0])),
                    f"../omop/{omop_table}/{concept_id_column}/*_usagi.csv",
                )
            )
            # os.path.join(
            #     os.path.abspath(os.path.dirname(__file__)),
            #     f"../omop/{omop_table}/{concept_id_column}/*_usagi.csv",
            # ))
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
            # upload the Parquet file to the Cloud Storage Bucket
            self._gcp.upload_file_to_bucket(
                parquet_file, self._bucket_name, self._bucket_path
            )
            # load the uploaded Parquet file from the bucket into the specific usagi table in the work dataset
            self._gcp.batch_load_from_bucket_into_bigquery_table(
                f"gs://{self._bucket_name}/{self._bucket_path}/{omop_table}__{concept_id_column}_usagi.parquet",
                f"{self._project_id}.{self._dataset_id_work}.{omop_table}__{concept_id_column}_usagi",
            )

        logging.info(
            "Swapping the source values with the concept ids in usagi table for column '%s' of table '%s'",
            concept_id_column,
            omop_table,
        )
        # replace the source values with the concept id's and names using the previously filled up swap table
        # custom concepts will recieve the mapping status 'APPROVED'
        template = self._template_env.get_template(
            "{omop_table}__{concept_id_column}_usagi_merge.sql.jinja"
        )
        sql = template.render(
            project_id=self._project_id,
            dataset_id_work=self._dataset_id_work,
            omop_table=omop_table,
            concept_id_column=concept_id_column,
            dataset_id_omop=self._dataset_id_omop,
            min_custom_concept_id=Etl._CUSTOM_CONCEPT_IDS_START,
        )
        self._gcp.run_query_job(sql)

        logging.info(
            "Merging mapped concepts into SOURCE_TO_CONCEPT_MAP table for column '%s' of table '%s'",
            concept_id_column,
            omop_table,
        )
        # fill up the SOURCE_TO_CONCEPT_MAP table with all approvrd mappings from the Usagi CSV's
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

    def _raw_to_work_query(self, sql_file: str, omop_table: str):
        """Executes the query from the .sql file.
        The results are loaded in a table (which name will have the format {omop_table}_{sql_file_name}) of the work dataset.
        The query must keep the source values for the primary key, foreign key(s) en concept ids.
        The ETL process will automatically replace the primary key source values with autonumbering.
        The foreign key(s) will be replaced by the ETL process with their corresponding autonumbers, that were generated by a previous ETL table. Therefor the sequence of the OMOP tables in the 'omop_tables' parameter of this class is extremely imortant!
        The source values in the concept_id columns will alse be automatically replaced by the ETL process with the mapped values from the supplied Usagi CSV's and the custom concept CSV's.

        Args:
            sql_file (str): The sql file holding the query on the raw data.
            omop_table (str): OMOP table.
        """  # noqa: E501 # pylint: disable=line-too-long
        logging.info(
            "Running filter query '%s' from raw tables into table '%s'",
            sql_file,
            f"{self._dataset_id_work}.{omop_table}_{Path(Path(sql_file).stem).stem}",
        )
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

        # load the results of the query in the work dataset table
        template = self._template_env.get_template(
            "{omop_table}_{sql_file}_insert.sql.jinja"
        )
        ddl = template.render(
            project_id=self._project_id,
            dataset_id_work=self._dataset_id_work,
            omop_table=omop_table,
            sql_file=Path(Path(sql_file).stem).stem,
            select_query=select_query,
        )
        self._gcp.run_query_job(ddl)

    def _swap_primary_key_column(
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
        template = self._template_env.get_template(
            "{primary_key_column}_swap_create.sql.jinja"
        )
        ddl = template.render(
            project_id=self._project_id,
            dataset_id_work=self._dataset_id_work,
            primary_key_column=primary_key_column,
            foreign_key_columns=foreign_key_columns.__dict__,
            concept_id_columns=concept_id_columns,
        )
        self._gcp.run_query_job(ddl)

        # execute the swap merge query
        template = self._template_env.get_template(
            "{primary_key_column}_swap_merge.sql.jinja"
        )
        sql = template.render(
            project_id=self._project_id,
            dataset_id_work=self._dataset_id_work,
            columns=columns,
            primary_key_column=primary_key_column,
            foreign_key_columns=foreign_key_columns.__dict__,
            concept_id_columns=concept_id_columns,
            pk_auto_numbering=pk_auto_numbering,
            omop_table=omop_table,
            sql_file=Path(Path(sql_file).stem).stem,
        )
        self._gcp.run_query_job(sql)

    def _merge_into_omop_table(
        self,
        sql_file: str,
        omop_table: str,
        columns: List[str],
        primary_key_column: str,
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
        logging.info("Merging query '%s' int omop table '%s'", sql_file, omop_table)
        template = self._template_env.get_template("{omop_table}_merge.sql.jinja")
        sql = template.render(
            project_id=self._project_id,
            dataset_id_omop=self._dataset_id_omop,
            omop_table=omop_table,
            dataset_id_work=self._dataset_id_work,
            sql_file=Path(Path(sql_file).stem).stem,
            columns=columns,
            primary_key_column=primary_key_column,
            foreign_key_columns=foreign_key_columns.__dict__,
            concept_id_columns=concept_id_columns,
            pk_auto_numbering=pk_auto_numbering,
        )
        self._gcp.run_query_job(sql)

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
                    "CONCEPT_ID",
                    "CONCEPT_NAME",
                    "DOMAIN_ID",
                    "VOCABULARY_ID",
                    "CONCEPT_CLASS_ID",
                    "STANDARD_CONCEPT",
                    "CONCEPT_CODE",
                    "VALID_START_DATE",
                    "VALID_END_DATE",
                    "INVALID_REASON",
                ],
                column_types={
                    "CONCEPT_ID": pa.string(),
                    "CONCEPT_NAME": pa.string(),
                    "DOMAIN_ID": pa.string(),
                    "VOCABULARY_ID": pa.string(),
                    "CONCEPT_CLASS_ID": pa.string(),
                    "STANDARD_CONCEPT": pa.string(),
                    "CONCEPT_CODE": pa.string(),
                    "VALID_START_DATE": pa.date32(),  # can only custom parse with timestamp, not date
                    "VALID_END_DATE": pa.date32(),  # can only custom parse with timestamp, not date
                    "INVALID_REASON": pa.string(),
                },
                # timestamp_parsers=[csv.ISO8601, '%Y-%m-%d', '%d/%m/%Y']
            ),
        )
        return table

    def cleanup(self, cleanup_table: str = "ALL"):
        """
        Cleanup the ETL process:\n
        All work tables in the work dataset are deleted.\n
        All 'clinical' and 'health system' tables in the omop dataset are truncated. (the ones configured in the omop_tables variable)\n
        The SOURCE_TO_CONCEPT_MAP table in the omop dataset is truncated.\n
        All custom concepts are removed from the CONCEPT, CONCEPT_RELATIONSHIP and CONCEPT_ANCESTOR tables in the omop dataset.\n
        """  # noqa: E501 # pylint: disable=line-too-long
        work_tables = self._gcp.get_table_names(self._project_id, self._dataset_id_work)
        # custom cleanup
        if cleanup_table == "ALL":
            logging.info(
                "Truncate table '%s'",
                f"{self._project_id}.{self._dataset_id_omop}.SOURCE_TO_CONCEPT_MAP",
            )
            template = self._template_env.get_template("cleanup/truncate.sql.jinja")
            sql = template.render(
                project_id=self._project_id,
                dataset_id_omop=self._dataset_id_omop,
                table_name="SOURCE_TO_CONCEPT_MAP",
            )
            self._gcp.run_query_job(sql)

            logging.info(
                "Removing custom concepts from '%s'",
                f"{self._project_id}.{self._dataset_id_omop}.CONCEPT",
            )
            template = self._template_env.get_template(
                "cleanup/CONCEPT_remove_custom_concepts.sql.jinja"
            )
            sql = template.render(
                project_id=self._project_id,
                dataset_id_omop=self._dataset_id_omop,
                min_custom_concept_id=Etl._CUSTOM_CONCEPT_IDS_START,
            )
            self._gcp.run_query_job(sql)

            logging.info(
                "Removing custom concepts from '%s'",
                f"{self._project_id}.{self._dataset_id_omop}.CONCEPT_RELATIONSHIP",
            )
            template = self._template_env.get_template(
                "cleanup/CONCEPT_RELATIONSHIP_remove_custom_concepts.sql.jinja"
            )
            sql = template.render(
                project_id=self._project_id,
                dataset_id_omop=self._dataset_id_omop,
                min_custom_concept_id=Etl._CUSTOM_CONCEPT_IDS_START,
            )
            self._gcp.run_query_job(sql)

            logging.info(
                "Removing custom concepts from '%s'",
                f"{self._project_id}.{self._dataset_id_omop}.CONCEPT_ANCESTOR",
            )
            template = self._template_env.get_template(
                "cleanup/CONCEPT_ANCESTOR_remove_custom_concepts.sql.jinja"
            )
            sql = template.render(
                project_id=self._project_id,
                dataset_id_omop=self._dataset_id_omop,
                min_custom_concept_id=Etl._CUSTOM_CONCEPT_IDS_START,
            )
            self._gcp.run_query_job(sql)
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
                        f"{self._project_id}.{self._dataset_id_omop}.CONCEPT",
                        f"{self._project_id}.{self._dataset_id_work}.{omop_table}__{concept_id_column}_concept",
                    )
                    template = self._template_env.get_template(
                        "cleanup/CONCEPT_remove_custom_concepts_by_{omop_table}__{concept_id_column}_concept_table.sql.jinja"
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

                    logging.info(
                        "Removing custom concepts from '%s' based on values from '%s' CSV",
                        f"{self._project_id}.{self._dataset_id_omop}.CONCEPT_RELATIONSHIP",
                        f"{self._project_id}.{self._dataset_id_work}.{omop_table}__{concept_id_column}_usagi",
                    )
                    template = self._template_env.get_template(
                        "cleanup/CONCEPT_RELATIONSHIP_remove_custom_concepts_by_{omop_table}__{concept_id_column}_usagi_table.sql.jinja"
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

                    logging.info(
                        "Removing custom concepts from '%s' based on values from '%s' CSV",
                        f"{self._project_id}.{self._dataset_id_omop}.CONCEPT_ANCESTOR",
                        f"{self._project_id}.{self._dataset_id_work}.{omop_table}__{concept_id_column}_usagi",
                    )
                    template = self._template_env.get_template(
                        "cleanup/CONCEPT_ANCESTOR_remove_custom_concepts_by_{omop_table}__{concept_id_column}_usagi_table.sql.jinja"
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
                elif table_name.startswith(cleanup_table) and table_name.endswith(
                    "_usagi"
                ):
                    omop_table = table_name.split("__")[0]
                    concept_id_column = table_name.split("__")[1].removesuffix("_usagi")
                    logging.info(
                        "Removing source to comcept maps from '%s' based on values from '%s' CSV",
                        f"{self._project_id}.{self._dataset_id_omop}.SOURCE_TO_CONCEPT_MAP",
                        f"{self._project_id}.{self._dataset_id_work}.{omop_table}__{concept_id_column}_usagi",
                    )
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

        # delete work tables
        for table_name in work_tables:
            if cleanup_table == "ALL" or table_name.startswith(cleanup_table):
                table_id = f"{self._project_id}.{self._dataset_id_work}.{table_name}"
                logging.info("Deleting table '%s'", table_id)
                self._gcp.delete_table(table_id)
        # truncate omop tables
        omop_tables = vars(self._omop_tables).keys()
        for table_name in (x for x in omop_tables if x not in ["VOCABULARY"]):
            if cleanup_table == "ALL" or table_name == cleanup_table:
                logging.info(
                    "Truncate table '%s'",
                    f"{self._project_id}.{self._dataset_id_omop}.{table_name}",
                )
                template = self._template_env.get_template("cleanup/truncate.sql.jinja")
                sql = template.render(
                    project_id=self._project_id,
                    dataset_id_omop=self._dataset_id_omop,
                    table_name=table_name,
                )
                self._gcp.run_query_job(sql)
