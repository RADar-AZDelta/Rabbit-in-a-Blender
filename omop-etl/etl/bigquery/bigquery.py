# pylint: disable=unsubscriptable-object
"""Holds the BigQuery ETL class"""
import json
import logging
import re
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, cast

import google.auth
import google.cloud.bigquery as bq
import jinja2 as jj
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pcsv
import pyarrow.parquet as pq
from jinja2.utils import select_autoescape

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

    def create_omop_db(self) -> None:
        """Create OMOP tables in the omop-dataset in BigQuery and apply clustering"""
        self._gcp.run_query_job(self._ddl)

        for table, fields in self._clustering_fields.items():
            self._gcp.set_clustering_fields_on_table(
                self._project_id, self._dataset_id_omop, table, fields
            )

    @property
    def _clustering_fields(self) -> Dict[str, List[str]]:
        if not self.__clustering_fields:
            with open(
                str(
                    Path(__file__).parent.absolute()
                    / "templates/OMOPCDM_bigquery_5.4_clustering_fields.json"
                ),
                "r",
                encoding="UTF8",
            ) as file:
                self.__clustering_fields = json.load(file)
        return self.__clustering_fields

    def _source_to_concept_map_update_invalid_reason(self, etl_start: date) -> None:
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

    def _get_column_names(self, omop_table_name: str) -> List[str]:
        columns = self._gcp.get_column_names(
            self._project_id, self._dataset_id_omop, omop_table_name
        )
        return columns

    @property
    def _ddl(self):
        with open(
            str(
                Path(__file__).parent.absolute()
                / "templates/OMOPCDM_bigquery_5.4_ddl.sql"
            ),
            "r",
            encoding="UTF8",
        ) as file:
            ddl = file.read()

        ddl = re.sub(
            r"(?:create table @cdmDatabaseSchema)(\S*)",
            rf"create table if not exists `{self._project_id}.{self._dataset_id_omop}\1`",
            ddl,
        )
        ddl = re.sub(r".(?<!not )null", r"", ddl)
        ddl = re.sub(r"\"", r"", ddl)
        ddl = re.sub(r"domain_concept_id_", r"field_concept_id_", ddl)
        ddl = re.sub(r"cost_domain_id STRING", r"cost_field_concept_id INT64", ddl)
        return ddl

    def _is_pk_auto_numbering(
        self, omop_table_name: str, omop_table_props: Any
    ) -> bool:
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
        pk_auto_numbering = pk_column_metadata.get("data_type") == "INT64"
        return pk_auto_numbering

    def _clear_custom_concept_upload_table(
        self, omop_table: str, concept_id_column: str
    ) -> None:
        self._gcp.delete_table(
            self._project_id,
            self._dataset_id_work,
            f"{omop_table}__{concept_id_column}_concept",
        )

    def _create_custom_concept_upload_table(
        self, omop_table: str, concept_id_column: str
    ) -> None:
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
        template = self._template_env.get_template("CONCEPT_ID_swap_create.sql.jinja")
        ddl = template.render(
            project_id=self._project_id,
            dataset_id_work=self._dataset_id_work,
        )
        self._gcp.run_query_job(ddl)

    def _load_custom_concepts_parquet_in_upload_table(
        self, parquet_file: Path, omop_table: str, concept_id_column: str
    ) -> None:
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
        self._gcp.delete_table(
            self._project_id,
            self._dataset_id_work,
            f"{omop_table}__{concept_id_column}_usagi",
        )

    def _create_usagi_upload_table(
        self, omop_table: str, concept_id_column: str
    ) -> None:
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

    def _get_query_from_sql_file(self, sql_file: Path, omop_table: str) -> str:
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
        self, pk_swap_table_name: str, concept_id_columns: List[str]
    ) -> None:
        template = self._template_env.get_template(
            "{primary_key_column}_swap_create.sql.jinja"
        )
        ddl = template.render(
            project_id=self._project_id,
            dataset_id_work=self._dataset_id_work,
            pk_swap_table_name=pk_swap_table_name,
            # foreign_key_columns=vars(foreign_key_columns),
            concept_id_columns=concept_id_columns,
        )
        self._gcp.run_query_job(ddl)

    def _execute_pk_auto_numbering_swap_query(
        self,
        omop_table: str,
        work_table: str,
        pk_swap_table_name: str,
        primary_key_column: str,
        concept_id_columns: List[str],
    ) -> None:
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
            work_table=work_table,
        )
        self._gcp.run_query_job(sql)

    def _merge_into_omop_table(
        self,
        sql_file: Path,
        omop_table: str,
        columns: List[str],
        pk_swap_table_name: Optional[str],
        primary_key_column: Optional[str],
        pk_auto_numbering: bool,
        foreign_key_columns: Any,
        concept_id_columns: List[str],
    ):
        logging.info(
            "Merging query '%s' into omop table '%s'", str(sql_file), omop_table
        )
        template = self._template_env.get_template("{omop_table}_merge.sql.jinja")
        sql = template.render(
            project_id=self._project_id,
            dataset_id_omop=self._dataset_id_omop,
            omop_table=omop_table,
            dataset_id_work=self._dataset_id_work,
            sql_file=Path(Path(sql_file).stem).stem,
            columns=columns,
            pk_swap_table_name=pk_swap_table_name,
            primary_key_column=primary_key_column,
            foreign_key_columns=vars(foreign_key_columns)
            if hasattr(foreign_key_columns, "__dict__")
            else foreign_key_columns,
            concept_id_columns=concept_id_columns,
            pk_auto_numbering=pk_auto_numbering,
        )
        self._gcp.run_query_job(sql)

    def _get_work_tables(self) -> List[str]:
        work_tables = self._gcp.get_table_names(self._project_id, self._dataset_id_work)
        return work_tables

    def _truncate_omop_table(self, table_name: str) -> None:
        template = self._template_env.get_template("cleanup/truncate.sql.jinja")
        sql = template.render(
            project_id=self._project_id,
            dataset_id_omop=self._dataset_id_omop,
            table_name=table_name,
        )
        self._gcp.run_query_job(sql)

    def _remove_custom_concepts_from_concept_table(self) -> None:
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
        self._gcp.run_query_job(sql)

    def _remove_custom_concepts_from_concept_relationship_table_using_usagi_table(
        self, omop_table: str, concept_id_column: str
    ) -> None:
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

    def _remove_custom_concepts_from_concept_ancestor_table_using_usagi_table(
        self, omop_table: str, concept_id_column: str
    ) -> None:
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

    def _remove_custom_concepts_from_vocabulary_table_using_usagi_table(
        self, omop_table: str, concept_id_column: str
    ) -> None:
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
        self._gcp.run_query_job(sql)

    def _remove_source_to_concept_map_using_usagi_table(
        self, omop_table: str, concept_id_column: str
    ) -> None:
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
        table_id = f"{self._project_id}.{self._dataset_id_work}.{work_table}"
        logging.info("Deleting table '%s'", table_id)
        self._gcp.delete_table(self._project_id, self._dataset_id_work, work_table)

    def _load_vocabulary_in_upload_table(
        self, csv_file: Path, vocabulary_table: str
    ) -> None:
        logging.info("Converting '%s.csv' to parquet", vocabulary_table)
        tab = self._read_vocabulary_csv(vocabulary_table, csv_file)
        parquet_file = csv_file.parent / f"{vocabulary_table}.parquet"
        pq.write_table(tab, where=parquet_file)

        logging.info("Loading '%s.parquet' into work table", vocabulary_table)
        # upload the Parquet file to the Cloud Storage Bucket
        uri = self._gcp.upload_file_to_bucket(parquet_file, self._bucket_uri)
        # load the uploaded Parquet file from the bucket into the specific custom concept table in the work dataset
        self._gcp.batch_load_from_bucket_into_bigquery_table(
            uri,
            self._project_id,
            self._dataset_id_work,
            vocabulary_table,
            write_disposition=bq.WriteDisposition.WRITE_EMPTY,  # , schema
        )

    def _clear_vocabulary_upload_table(self, vocabulary_table: str) -> None:
        self._gcp.delete_table(
            self._project_id, self._dataset_id_work, vocabulary_table
        )

    def _read_vocabulary_csv(self, vocabulary_table: str, csv_file: Path) -> pa.Table:
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

        table_ddl = re.search(
            rf"create\s+table\s+if\s+not\s+exists\s+`?\S*?\.{vocabulary_table}`?\s*\(\s*(.*?)\s*\)\s*(?:;|$)",
            self._ddl,
            flags=re.DOTALL,
        )
        if table_ddl:
            table_ddl = table_ddl.group(1)
        else:
            raise Exception(
                "No definition found for {vocabulary_table} in ddl-file (with current regex)."
            )
        fields = re.findall(
            r"(?:,|^)\s*(\w+\s+?\w+).*?(?=,|$|;)", table_ddl, flags=re.DOTALL
        )
        for idx, field in enumerate(fields):
            splits = field.split(" ")
            schema.append((splits[0], _to_pa(splits[1])))
            if splits[1].lower() == "date":
                date_columns.append((idx, splits[0]))

        schema = pa.schema(schema)

        return (schema, date_columns)

    def _recreate_vocabulary_table(self, vocabulary_table: str) -> None:
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
