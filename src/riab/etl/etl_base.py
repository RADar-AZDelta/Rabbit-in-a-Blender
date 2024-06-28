# Copyright 2024 RADar-AZDelta
# SPDX-License-Identifier: gpl3+

"""Holds the ETL abstract base class"""

import json
import logging
import time
from abc import ABC, abstractmethod
from pathlib import Path

from polars import DataFrame, DataType, Datetime, Float64, Int64, Utf8, col, element, lit, read_csv, when


class EtlBase(ABC):
    """
    Base class for the ETL commands
    """

    _CUSTOM_CONCEPT_IDS_START = (
        2_000_000_000  # Concepts reserved for site-specific codes and mappings start from 2 billion
    )

    def __init__(
        self,
        db_engine: str,
        cdm_folder_path: str | None = None,
        omop_cdm_version: str = "5.4",
        max_parallel_tables: int = 9,
        max_worker_threads_per_table: int = 16,
    ):
        """Constructor
        Base class constructor for the ETL commands

        Args:
            cdm_folder_path (str): The path to the OMOP folder structure that holds for each OMOP CDM table (folder) the ETL queries, Usagi CSV's and custom concept CSV's0
        """  # noqa: E501 # pylint: disable=line-too-long

        self._cdm_folder_path = Path(cdm_folder_path).resolve() if cdm_folder_path else None
        self._db_engine = db_engine
        self._omop_cdm_version = omop_cdm_version
        self._max_parallel_tables = max_parallel_tables
        self._max_worker_threads_per_table = max_worker_threads_per_table

        self._cdm_tables_fks_dependencies_resolved: list[list[str]] = []

        logging.debug("Loading Jinja environment")
        import jinja2 as jj
        from jinja2.utils import select_autoescape

        template_dir = Path(__file__).resolve().parent / self._db_engine / "templates"
        template_loader = jj.FileSystemLoader(searchpath=template_dir)
        self._template_env = jj.Environment(autoescape=select_autoescape(["sql"]), loader=template_loader)

        logging.debug(f"Processing OMOP_CDMv{omop_cdm_version}_Table_Level.csv")
        self._df_omop_tables: DataFrame = read_csv(
            str(
                Path(__file__).parent.parent.resolve()
                / "libs"
                / "CommonDataModel"
                / "inst"
                / "csv"
                / f"OMOP_CDMv{omop_cdm_version}_Table_Level.csv"
            )
        )
        # ctx = SQLContext(omop_tables=self._df_omop_tables, eager_execution=True)
        # self._omop_cdm_tables = ctx.execute("SELECT lower(cdmTableName) FROM omop_tables WHERE schema = 'CDM'")["cdmTableName"].to_list()
        self._omop_cdm_tables: list[str] = (
            self._df_omop_tables.filter(col("schema") == "CDM")
            .select(cdmTableName=(col("cdmTableName").str.to_lowercase()))["cdmTableName"]
            .to_list()
        )

        self._omop_etl_tables: list[str] = (
            self._df_omop_tables.filter(
                (col("schema") == "CDM") | (col("cdmTableName").str.to_lowercase() == "vocabulary")
            )
            .select(cdmTableName=(col("cdmTableName").str.to_lowercase()))["cdmTableName"]
            .to_list()
        )

        logging.debug(f"Processing OMOP_CDMv{omop_cdm_version}_Field_Level.csv")
        self._df_omop_fields: DataFrame = read_csv(
            str(
                Path(__file__).parent.parent.resolve()
                / "libs"
                / "CommonDataModel"
                / "inst"
                / "csv"
                / f"OMOP_CDMv{omop_cdm_version}_Field_Level.csv"
            )
        ).with_row_count(name="row_nr")
        match db_engine:
            case "sql_server":
                # change the datatype for all _source_value columns from varchar(50) to varchar(255)
                self._df_omop_fields = self._df_omop_fields.with_columns(
                    when(col("cdmFieldName").str.ends_with("_source_value") & (col("cdmDatatype") == "varchar(50)"))
                    .then(lit("varchar(255)"))
                    .otherwise(col("cdmDatatype"))
                    .alias("cdmDatatype")
                )

        # the NOTE_NLP has a FK to NOTES see issue https://github.com/OHDSI/CommonDataModel/issues/539
        row_nr = self._df_omop_fields.filter(
            (col("cdmTableName").str.to_uppercase() == "NOTE_NLP")
            & (col("cdmFieldName").str.to_uppercase() == "NOTE_ID")
        ).select("row_nr")["row_nr"][0]
        self._df_omop_fields[row_nr, "isForeignKey"] = "Yes"
        self._df_omop_fields[row_nr, "fkTableName"] = "NOTE"
        self._df_omop_fields[row_nr, "fkFieldName"] = "NOTE_ID"

        self._resolve_cdm_tables_fks_dependencies()

        logging.debug(f"Processing cdm_{omop_cdm_version}_events.json")
        with open(
            str(Path(__file__).parent.resolve() / f"cdm_{omop_cdm_version}_events.json"),
            "r",
            encoding="UTF8",
        ) as file:
            self._omop_event_fields: dict[str, dict[str, str]] = json.load(file)

    def __enter__(self):
        self._start_time = time.time()
        return self

    def __exit__(self, exception_type, exception_value, exception_traceback):
        end_time = time.time()
        hours, rem = divmod(end_time - self._start_time, 3600)
        minutes, seconds = divmod(rem, 60)
        elapsted_time = "{:0>2}:{:0>2}:{:05.2f}".format(int(hours), int(minutes), seconds)
        logging.info("RiaB took: %s", elapsted_time)

    def _resolve_cdm_tables_fks_dependencies(self):
        """Resolves the ETL dependency"""
        tables = (
            self._df_omop_tables.filter(
                (col("schema") == "CDM") | (col("cdmTableName").is_in(["CDM_SOURCE", "VOCABULARY"]))
            )
            .select("cdmTableName")["cdmTableName"]
            .to_list()
        )

        self._cdm_tables_fks_dependencies_resolved = self._build_fk_dependency_tree_of_tables(tables)

        logging.debug(
            "Resolved ETL tables foreign keys dependency graph: \n%s",
            self.print_cdm_tables_fks_dependencies_tree(),
        )

    def _build_fk_dependency_tree_of_tables(self, tables: list[str]):
        """Builds the foreign key dependency tree of the tables"""
        fk_dependency_tree: list[list[str]] = []

        tables_with_fks = dict(
            self._df_omop_fields.filter(
                (col("cdmTableName").is_in(tables))
                & ((col("fkTableName").is_null()) | (col("fkTableName").is_in(tables)))
            )
            .group_by("cdmTableName")
            .agg(col("fkTableName"))
            .with_columns(col("fkTableName").list.drop_nulls().alias("fkTableName"))
            .iter_rows()
        )

        # remove circular references
        tables_with_fks = dict(
            ((k.lower(), set(v.lower() for v in v) - set([k.lower()])) for k, v in tables_with_fks.items())
        )

        tables_with_no_fks = set(k for k, v in tables_with_fks.items() if not v)

        fk_dependency_tree.append(["vocabulary"])
        fk_dependency_tree.append([t for t in tables_with_no_fks if t != "vocabulary"])

        tables_with_fks = dict(
            ((k, set(v) - tables_with_no_fks) for k, v in tables_with_fks.items() if k not in tables_with_no_fks)
        )

        while tables_with_fks:
            # values not in keys (tables without FK's)
            t = set(fk for fks in tables_with_fks.values() for fk in fks) - set(tables_with_fks.keys())
            # and keys without value (tables without FK's)
            t.update(k for k, v in tables_with_fks.items() if not v)
            if not t:  # circular reference
                raise Exception("Circular reference in FKs dependency graph")
            # can be done right away
            fk_dependency_tree.append(sorted(t))
            # and cleaned up
            tables_with_fks = dict(((k, set(v) - t) for k, v in tables_with_fks.items() if v))

        return fk_dependency_tree

    def print_cdm_tables_fks_dependencies_tree(self) -> str:
        """Prints the ETL dependency tree"""
        spacer = 0
        depency_tree_text_representation = []
        for level in self._cdm_tables_fks_dependencies_resolved:
            for idx, table in enumerate(level):
                depency_tree_text_representation.append(
                    f"{' ' * spacer}{('└──' if idx == (len(level) - 1) else '├──') }{table.lower()}"
                )
            spacer += 2
        return "\n".join(depency_tree_text_representation)

    def _get_omop_column_names(self, omop_table_name: str) -> list[str]:
        """Get list of column names of a omop table.

        Args:
            omop_table_name (str): OMOP table

        Returns:
            list[str]: list of column names
        """
        fields = self._df_omop_fields.filter((col("cdmTableName").str.to_lowercase() == omop_table_name))[
            "cdmFieldName"
        ].to_list()
        return fields

    def _get_required_omop_column_names(self, omop_table_name: str) -> list[str]:
        """Get list of required column names of a omop table.

        Args:
            omop_table_name (str): OMOP table

        Returns:
            list[str]: list of column names
        """
        fields = self._df_omop_fields.filter(
            (col("cdmTableName").str.to_lowercase() == omop_table_name) & (col("isRequired") == "Yes")
        )["cdmFieldName"].to_list()
        return fields

    def _is_pk_auto_numbering(self, omop_table_name: str) -> bool:
        """Checks if the primary key of the OMOP table needs autonumbering.
        For example the [Person](https://ohdsi.github.io/CommonDataModel/cdm54.html#PERSON) table has an auto numbering primary key, the [Vocabulary](https://ohdsi.github.io/CommonDataModel/cdm54.html#VOCABULARY) table not.

        Args:
            omop_table_name (str): OMOP table
            omop_table_props (Any): Primary key, foreign key(s) and event(s) of the OMOP table

        Returns:
            bool: True if the PK needs autonumbering
        """  # noqa: E501 # pylint: disable=line-too-long
        pk_auto_numbering = (
            len(
                self._df_omop_fields.filter(
                    (col("cdmTableName").str.to_lowercase() == omop_table_name)
                    & (col("isPrimaryKey") == "Yes")
                    & (col("cdmDatatype") == "integer")
                )
            )
            > 0
        )
        return pk_auto_numbering

    def _get_pk(self, omop_table_name: str) -> str | None:
        """Get primary key column of a omop table.

        Args:
            omop_table_name (str): OMOP table

        Returns:
            str: primary key column name
        """
        fks = (
            self._df_omop_fields.filter(
                (col("cdmTableName").str.to_lowercase() == omop_table_name) & (col("isPrimaryKey") == "Yes")
            )
            .select("cdmFieldName")["cdmFieldName"]
            .to_list()
        )

        return len(fks) > 0 and fks[0] or None

    def _get_fks(self, omop_table_name: str) -> dict[str, str]:
        """Get list of foreign key columns of a omop table. (without foreign keys to the CONCEPT table)

        Args:
            omop_table_name (str): OMOP table

        Returns:
            dict[str, str]: dict with he column name and the foreign key table name
        """
        fks = dict(
            self._df_omop_fields.filter(
                (col("cdmTableName").str.to_lowercase() == omop_table_name)
                & (col("isForeignKey") == "Yes")
                & (col("fkTableName").str.to_lowercase() != "concept")
            )
            .select("cdmFieldName", col("fkTableName").str.to_lowercase())
            .iter_rows()
        )

        return fks

    def _get_fk_domains(self, omop_table_name: str) -> dict[str, list[str]]:
        """Get list of domains of the foreign key columns of a omop table.

        Args:
            omop_table_name (str): OMOP table

        Returns:
            dict[str, list[str]]: dict with he column name and the list of foreign key domain names
        """
        fk_domains = dict(
            self._df_omop_fields.filter(
                (col("cdmTableName").str.to_lowercase() == omop_table_name) & (col("fkDomain").is_not_null())
            )
            .with_columns(
                col("fkDomain").str.to_lowercase().str.split(",").list.eval(element().str.strip()).alias("fkDomain")
            )
            .select("cdmFieldName", "fkDomain")
            .iter_rows()
        )

        return fk_domains

    def _get_polars_type(self, cdmDatatype: str) -> DataType:
        match cdmDatatype:
            case "integer":
                return Int64  # type: ignore
            case "datetime":
                return Datetime  # type: ignore
            case "varchar(50)":
                return Utf8  # type: ignore
            case "date":
                return Utf8  # type: ignore
            # WARNING: Date # Data not parsed well --> will do it manually
            case "Integer":
                return Int64  # type: ignore
            case "varchar(20)":
                return Utf8  # type: ignore
            case "float":
                return Float64  # type: ignore
            case "varchar(MAX)":
                return Utf8  # type: ignore
            case "varchar(255)":
                return Utf8  # type: ignore
            case "varchar(10)":
                return Utf8  # type: ignore
            case "varchar(60)":
                return Utf8  # type: ignore
            case "varchar(250)":
                return Utf8  # type: ignore
            case "varchar(1)":
                return Utf8  # type: ignore
            case "varchar(2000)":
                return Utf8  # type: ignore
            case "varchar(2)":
                return Utf8  # type: ignore
            case "varchar(9)":
                return Utf8  # type: ignore
            case "varchar(80)":
                return Utf8  # type: ignore
            case "varchar(3)":
                return Utf8  # type: ignore
            case "varchar(25)":
                return Utf8  # type: ignore
            case "varchar(1000)":
                return Utf8  # type: ignore
            case _:
                raise ValueError(f"Unknown cdmDatatype: {cdmDatatype}")

    def _get_polars_schema_for_cdm_table(self, vocabulary_table: str) -> dict[str, DataType]:
        df_table_fields = self._df_omop_fields.filter(
            col("cdmTableName").str.to_lowercase() == vocabulary_table
        ).select(["cdmFieldName", "cdmDatatype"])
        polars_schema: dict[str, DataType] = {}
        cdmFieldName: str
        cdmDatatype: str
        for cdmFieldName, cdmDatatype in df_table_fields.iter_rows():
            polars_schema[cdmFieldName] = self._get_polars_type(cdmDatatype)
        return polars_schema

    @abstractmethod
    def _test_db_connection(self):
        """Test the connection to the database."""
        pass

    def _get_git_commmit_hash(self, base_path: Path):
        git_dir = base_path / ".git"

        if not git_dir.exists():
            return None

        try:
            with (git_dir / "HEAD").open("r") as head:
                ref = head.readline().split(" ")[-1].strip()

            with (git_dir / ref).open("r") as git_hash:
                return git_hash.readline().strip()
        except Exception:
            return None
