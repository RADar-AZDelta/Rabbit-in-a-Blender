# Copyright 2024 RADar-AZDelta
# SPDX-License-Identifier: gpl3+

"""Holds the ETL abstract base class"""
import json
import logging
import time
from abc import ABC, abstractmethod
from pathlib import Path
from types import SimpleNamespace
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

        self._cdm_tables_fks_dependencies_resolved: List[set[str]] = []

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
        # for table in ctx.execute("SELECT lower(cdmTableName) FROM omop_tables WHERE schema = 'CDM'")["cdmTableName"]:
        #     print(table)

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
            str(Path(__file__).parent.resolve() / f"cdm_{omop_cdm_version}_schema.json"),
            "r",
            encoding="UTF8",
        ) as file:
            self._omop_tables = json.load(file, object_hook=lambda x: SimpleNamespace(**x))

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
        tables = dict(((k, set(v) - set([k])) for k, v in tables.items()))

        tables_with_no_fks = set(k for k, v in tables.items() if not v)
        self._cdm_tables_fks_dependencies_resolved.append(tables_with_no_fks)
        tables_with_fks = dict(
            ((k, set(v) - tables_with_no_fks) for k, v in tables.items() if k not in tables_with_no_fks)
        )

        while tables_with_fks:
            # values not in keys (tables without FK's)
            t = set(fk for fks in tables_with_fks.values() for fk in fks) - set(tables_with_fks.keys())
            # and keys without value (tables without FK's)
            t.update(k for k, v in tables_with_fks.items() if not v)
            # can be done right away
            self._cdm_tables_fks_dependencies_resolved.append(t)
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
                    f"{' ' * spacer}{('└──' if idx == (len(level) - 1) else '├──') }{table}"
                )
            spacer += 2
        return "\n".join(depency_tree_text_representation)

    @abstractmethod
    def _get_omop_column_names(self, omop_table_name: str) -> List[str]:
        """Get list of column names of a omop table.

        Args:
            omop_table_name (str): OMOP table

        Returns:
            List[str]: list of column names
        """
        pass

    @abstractmethod
    def _get_required_omop_column_names(self, omop_table_name: str) -> List[str]:
        """Get list of required column names of a omop table.

        Args:
            omop_table_name (str): OMOP table

        Returns:
            List[str]: list of column names
        """
        pass
