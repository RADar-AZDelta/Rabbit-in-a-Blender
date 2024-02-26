# Copyright 2024 RADar-AZDelta
# SPDX-License-Identifier: gpl3+

"""Holds the ETL abstract base class"""
import json
import logging
import time
from abc import ABC
from pathlib import Path
from typing import List

import jinja2 as jj
import polars as pl
from jinja2.utils import select_autoescape


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
        max_workers=16,
    ):
        """Constructor
        Base class constructor for the ETL commands

        Args:
            cdm_folder_path (str): The path to the OMOP folder structure that holds for each OMOP CDM table (folder) the ETL queries, Usagi CSV's and custom concept CSV's0
        """  # noqa: E501 # pylint: disable=line-too-long
        self._start_time = time.time()

        self._cdm_folder_path = Path(cdm_folder_path).resolve() if cdm_folder_path else None
        self._db_engine = db_engine
        self._omop_cdm_version = omop_cdm_version
        self._max_workers = max_workers

        self._cdm_tables_fks_dependencies_resolved: list[list[str]] = []

        template_dir = Path(__file__).resolve().parent / self._db_engine / "templates"
        template_loader = jj.FileSystemLoader(searchpath=template_dir)
        self._template_env = jj.Environment(autoescape=select_autoescape(["sql"]), loader=template_loader)

        self._df_omop_tables: pl.DataFrame = pl.read_csv(
            str(
                Path(__file__).parent.parent.resolve()
                / "libs"
                / "CommonDataModel"
                / "inst"
                / "csv"
                / f"OMOP_CDMv{omop_cdm_version}_Table_Level.csv"
            )
        )
        # ctx = pl.SQLContext(omop_tables=self._df_omop_tables, eager_execution=True)
        # self._omop_cdm_tables = ctx.execute("SELECT lower(cdmTableName) FROM omop_tables WHERE schema = 'CDM'")["cdmTableName"].to_list()
        self._omop_cdm_tables: List[str] = (
            self._df_omop_tables.filter(pl.col("schema") == "CDM")
            .select(cdmTableName=(pl.col("cdmTableName").str.to_lowercase()))["cdmTableName"]
            .to_list()
        )

        self._omop_etl_tables: List[str] = (
            self._df_omop_tables.filter(
                (pl.col("schema") == "CDM") | (pl.col("cdmTableName").str.to_lowercase() == "vocabulary")
            )
            .select(cdmTableName=(pl.col("cdmTableName").str.to_lowercase()))["cdmTableName"]
            .to_list()
        )

        self._df_omop_fields: pl.DataFrame = pl.read_csv(
            str(
                Path(__file__).parent.parent.resolve()
                / "libs"
                / "CommonDataModel"
                / "inst"
                / "csv"
                / f"OMOP_CDMv{omop_cdm_version}_Field_Level.csv"
            )
        ).with_row_count(name="row_nr")

        # the NOTE_NLP has a FK to NOTES see issue https://github.com/OHDSI/CommonDataModel/issues/539
        row_nr = self._df_omop_fields.filter(
            (pl.col("cdmTableName").str.to_uppercase() == "NOTE_NLP")
            & (pl.col("cdmFieldName").str.to_uppercase() == "NOTE_ID")
        ).select("row_nr")["row_nr"][0]
        self._df_omop_fields[row_nr, "isForeignKey"] = "Yes"
        self._df_omop_fields[row_nr, "fkTableName"] = "NOTE"
        self._df_omop_fields[row_nr, "fkFieldName"] = "NOTE_ID"

        self._resolve_cdm_tables_fks_dependencies()

        with open(
            str(Path(__file__).parent.resolve() / f"cdm_{omop_cdm_version}_events.json"),
            "r",
            encoding="UTF8",
        ) as file:
            self._omop_event_fields: dict[str, dict[str, str]] = json.load(file)

    def __del__(self):
        end_time = time.time()
        hours, rem = divmod(end_time - self._start_time, 3600)
        minutes, seconds = divmod(rem, 60)
        elapsted_time = "{:0>2}:{:0>2}:{:05.2f}".format(int(hours), int(minutes), seconds)
        logging.info("ETL took: %s", elapsted_time)

    def _resolve_cdm_tables_fks_dependencies(self):
        """Resolves the ETL dependency"""
        df_cdm_tables = (
            self._df_omop_tables.filter(
                (pl.col("schema") == "CDM") | (pl.col("cdmTableName").is_in(["CDM_SOURCE", "VOCABULARY"]))
            )
            .select("cdmTableName")["cdmTableName"]
            .to_list()
        )
        tables = dict(
            self._df_omop_fields.filter(
                (pl.col("cdmTableName").is_in(df_cdm_tables))
                & ((pl.col("fkTableName").is_null()) | (pl.col("fkTableName").is_in(df_cdm_tables)))
            )
            .group_by("cdmTableName")
            .agg(pl.col("fkTableName"))
            .with_columns(pl.col("fkTableName").list.drop_nulls().alias("fkTableName"))
            .iter_rows()
        )

        # remove circular references
        tables = dict(((k.lower(), set(v.lower() for v in v) - set([k.lower()])) for k, v in tables.items()))

        tables_with_no_fks = set(k for k, v in tables.items() if not v)
        self._cdm_tables_fks_dependencies_resolved.append(sorted(tables_with_no_fks))
        tables_with_fks = dict(
            ((k, set(v) - tables_with_no_fks) for k, v in tables.items() if k not in tables_with_no_fks)
        )

        while tables_with_fks:
            # values not in keys (tables without FK's)
            t = set(fk for fks in tables_with_fks.values() for fk in fks) - set(tables_with_fks.keys())
            # and keys without value (tables without FK's)
            t.update(k for k, v in tables_with_fks.items() if not v)
            # can be done right away
            self._cdm_tables_fks_dependencies_resolved.append(sorted(t))
            # and cleaned up
            tables_with_fks = dict(((k, set(v) - t) for k, v in tables_with_fks.items() if v))

        logging.debug(
            "Resolved ETL tables foreign keys dependency graph: \n%s",
            self.print_cdm_tables_fks_dependencies_tree(),
        )

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

    def _get_omop_column_names(self, omop_table_name: str) -> List[str]:
        """Get list of column names of a omop table.

        Args:
            omop_table_name (str): OMOP table

        Returns:
            List[str]: list of column names
        """
        fields = self._df_omop_fields.filter((pl.col("cdmTableName").str.to_lowercase() == omop_table_name))[
            "cdmFieldName"
        ].to_list()
        return fields

    def _get_required_omop_column_names(self, omop_table_name: str) -> List[str]:
        """Get list of required column names of a omop table.

        Args:
            omop_table_name (str): OMOP table

        Returns:
            List[str]: list of column names
        """
        fields = self._df_omop_fields.filter(
            (pl.col("cdmTableName").str.to_lowercase() == omop_table_name) & (pl.col("isRequired") == "Yes")
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
                    (pl.col("cdmTableName").str.to_lowercase() == omop_table_name)
                    & (pl.col("isPrimaryKey") == "Yes")
                    & (pl.col("cdmDatatype") == "integer")
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
                (pl.col("cdmTableName").str.to_lowercase() == omop_table_name) & (pl.col("isPrimaryKey") == "Yes")
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
                (pl.col("cdmTableName").str.to_lowercase() == omop_table_name)
                & (pl.col("isForeignKey") == "Yes")
                & (pl.col("fkTableName").str.to_lowercase() != "concept")
            )
            .select("cdmFieldName", pl.col("fkTableName").str.to_lowercase())
            .iter_rows()
        )

        return fks

    def _get_fk_domains(self, omop_table_name: str) -> dict[str, List[str]]:
        """Get list of domains of the foreign key columns of a omop table.

        Args:
            omop_table_name (str): OMOP table

        Returns:
            dict[str, List[str]]: dict with he column name and the list of foreign key domain names
        """
        fk_domains = dict(
            self._df_omop_fields.filter(
                (pl.col("cdmTableName").str.to_lowercase() == omop_table_name) & (pl.col("fkDomain").is_not_null())
            )
            .with_columns(
                pl.col("fkDomain")
                .str.to_lowercase()
                .str.split(",")
                .list.eval(pl.element().str.strip())
                .alias("fkDomain")
            )
            .select("cdmFieldName", "fkDomain")
            .iter_rows()
        )

        return fk_domains
