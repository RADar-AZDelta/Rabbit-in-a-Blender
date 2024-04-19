# Copyright 2024 RADar-AZDelta
# SPDX-License-Identifier: gpl3+

import logging
import os
import re
import subprocess
from abc import ABC
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Optional, cast

import polars as pl
from sqlalchemy import engine

from ..db import Db
from ..etl_base import EtlBase


class SqlServerEtlBase(EtlBase, ABC):
    def __init__(
        self,
        server: str,
        user: str,
        password: str,
        port: int,
        raw_database_catalog: Optional[str],
        omop_database_catalog: str,
        work_database_catalog: str,
        dqd_database_catalog: str,
        achilles_database_catalog: str,
        raw_database_schema: Optional[str],
        omop_database_schema: str,
        work_database_schema: str,
        dqd_database_schema: str,
        achilles_database_schema: str,
        disable_fk_constraints: bool = True,
        bcp_code_page: str = "ACP",
        **kwargs,
    ):
        """This class holds the SQL Server specific methods of the ETL process

        Args:

        """
        super().__init__(**kwargs)

        self._server = server
        self._user = user
        self._password = password
        self._port = port
        self._raw_database_catalog = cast(str, raw_database_catalog)
        self._omop_database_catalog = omop_database_catalog
        self._work_database_catalog = work_database_catalog
        self._dqd_database_catalog = dqd_database_catalog
        self._achilles_database_catalog = achilles_database_catalog
        self._raw_database_schema = cast(str, raw_database_schema)
        self._omop_database_schema = omop_database_schema
        self._work_database_schema = work_database_schema
        self._dqd_database_schema = dqd_database_schema
        self._achilles_database_schema = achilles_database_schema
        self._disable_fk_constraints = disable_fk_constraints
        self._bcp_code_page = bcp_code_page

        url = engine.URL.create(
            drivername="mssql+pymssql",
            username=self._user,
            password=self._password,
            host=self._server,
            port=self._port,
            database=self._work_database_catalog,  # required for Azure SQL
        )

        self._db = Db(url)

    def _upload_dataframe(self, catalog: str, schema: str, table: str, df: pl.DataFrame) -> None:
        with TemporaryDirectory(prefix="riab_") as temp_dir_path:
            upload_file = str(Path(temp_dir_path) / f"{table}.csv")

            df.write_csv(
                upload_file,
                separator="\t",
                line_terminator="\n",
                include_header=True,
                datetime_format="%F %T",
                date_format="%F",
                time_format="%T",
                quote_style="never",
                # include_bom=True,
            )

            bcp_error_file = f"bcp_{table}.err"

            logging.debug("Loading '%s' into table [%s].[%s].[%s]", upload_file, catalog, schema, table)
            args = [
                "bcp" + (".exe" if os.name == "nt" else ""),
                f"[{schema}].[{table}]",
                "in",
                upload_file,
                "-d",
                f"{catalog}",
                "-S",
                f"{self._server},{self._port}",
                "-U",
                self._user,
                "-P",
                self._password,
                "-c",
                "-C",
                self._bcp_code_page,
                "-t",
                "\t",
                "-r",
                "\n",
                "-F2",
                "-k",
                "-b",
                "10000",
                "-e",
                bcp_error_file
            ]
            logging.info(f"Bulk copy command: {re.sub(
                r"-P.*-c",
                r"-P******* -c",
                " ".join([arg.encode("unicode_escape").decode("utf-8") if (arg == "\n" or arg == "\t") else arg for arg in args]),
            )}")
            process = subprocess.Popen(args)  # , shell=True, stdout=subprocess.PIPE)
            exit_code = process.wait()
            if os.path.isfile(bcp_error_file) and os.path.getsize(bcp_error_file) == 0:
                os.remove(bcp_error_file)  # remove the BCP error file
            else:
                raise Exception(f"BCP failed! See {bcp_error_file} for errors.")

    def _upload_parquet(self, catalog: str, schema: str, table: str, parquet_file: Path) -> None:
        """Loads the CSV file in the specific standardised vocabulary table

        Args:
            vocabulary_table (str): The standardised vocabulary table
            parquet_file (Path): Path to the CSV file
        """
        logging.debug(
            "Converting parquet file %s to BCP input file for table [%s].[%s].[%s]",
            parquet_file,
            catalog,
            schema,
            table,
        )
        df = pl.read_parquet(parquet_file)

        self._upload_dataframe(catalog, schema, table, df)

    def _remove_constraints(self, table_name: str) -> None:
        """Remove the foreign key constraints pointing to this table

        Args:
            table_name (str): Omop table
        """
        with open(
            str(
                Path(__file__).parent.resolve()
                / "templates"
                / "ddl"
                / f"OMOPCDM_{self._db_engine}_{self._omop_cdm_version}_constraints.sql.jinja"
            ),
            "r",
            encoding="UTF8",
        ) as file:
            ddl = file.read()
        matches = list(
            re.finditer(
                rf"(ALTER TABLE \[{{{{omop_database_catalog}}}}\]\.\[{{{{omop_database_schema}}}}\]\.)(.*)( ADD CONSTRAINT )(.*) (FOREIGN KEY \()(.*)( REFERENCES \[{{{{omop_database_catalog}}}}\]\.\[{{{{omop_database_schema}}}}\]\.{table_name.upper()} \()(.*)(\);)",
                ddl,
            )
        )
        constraints_to_drop = [
            f"IF EXISTS (SELECT 1 FROM sys.foreign_keys fk INNER JOIN sys.schemas s ON s.schema_id = fk.schema_id WHERE fk.name = '{match.group(4)}' and s.name = '{self._omop_database_schema}')\n{match.group(1)}{match.group(2)} DROP CONSTRAINT {match.group(4)};"
            for match in matches
        ]

        if len(constraints_to_drop):
            logging.debug("Remove the table contraints from omop table %s", table_name)
            modified_ddl = "\n".join(constraints_to_drop)
            template = self._template_env.from_string(modified_ddl)
            sql = template.render(
                omop_database_catalog=self._omop_database_catalog,
                omop_database_schema=self._omop_database_schema,
            )
            self._db.run_query(f"use [{self._omop_database_catalog}];\n" + sql)

    def _add_constraints(self, table_name: str) -> None:
        """Add the foreign key constraints pointing to this table

        Args:
            table_name (str): Omop table
        """
        with open(
            str(
                Path(__file__).parent.resolve()
                / "templates"
                / "ddl"
                / f"OMOPCDM_{self._db_engine}_{self._omop_cdm_version}_constraints.sql.jinja"
            ),
            "r",
            encoding="UTF8",
        ) as file:
            ddl = file.read()
        matches = list(
            re.finditer(
                rf"(ALTER TABLE \[{{{{omop_database_catalog}}}}\]\.\[{{{{omop_database_schema}}}}\]\.)(.*)( ADD CONSTRAINT )(.*) (FOREIGN KEY \()(.*)( REFERENCES \[{{{{omop_database_catalog}}}}\]\.\[{{{{omop_database_schema}}}}\]\.{table_name.upper()} \()(.*)(\);)",
                ddl,
            )
        )

        constraint_ddls = {}
        for match in matches:
            if not match.group(2) in constraint_ddls.keys():
                constraint_ddls[match.group(2)] = []
            constraint_ddls[match.group(2)].append(
                self._template_env.from_string(
                    f"{match.group(1)}{match.group(2)}{match.group(3)}{match.group(4)} {match.group(5)}{match.group(6)}{match.group(7)}{match.group(8)}{match.group(9)}"
                ).render(
                    omop_database_catalog=self._omop_database_catalog,
                    omop_database_schema=self._omop_database_schema,
                )
            )

        constraint_ddls = [
            self._template_env.from_string(
                f"{match.group(1)}{match.group(2)}{match.group(3)}{match.group(4)} {match.group(5)}{match.group(6)}{match.group(7)}{match.group(8)}{match.group(9)}"
            ).render(
                omop_database_catalog=self._omop_database_catalog,
                omop_database_schema=self._omop_database_schema,
            )
            for match in matches
        ]

        logging.debug("Adding the table contraints to the omop tables")
        with ThreadPoolExecutor(max_workers=self._max_worker_threads_per_table) as executor:
            futures = [executor.submit(self._db.run_query, f"use [{self._omop_database_catalog}];\n" + ddl) for ddl in constraint_ddls]
            # wait(futures, return_when=ALL_COMPLETED)
            for result in as_completed(futures):
                result.result()

    def _remove_all_constraints(self) -> None:
        """Remove all the foreign key constraints from the omop tables"""
        with open(
            str(
                Path(__file__).parent.resolve()
                / "templates"
                / "ddl"
                / f"OMOPCDM_{self._db_engine}_{self._omop_cdm_version}_constraints.sql.jinja"
            ),
            "r",
            encoding="UTF8",
        ) as file:
            ddl = file.read()
        matches = list(
            re.finditer(
                r"(ALTER TABLE \[{{omop_database_catalog}}\]\.\[{{omop_database_schema}}\]\.)(.*)( ADD CONSTRAINT )(.*) (FOREIGN KEY \()(.*)( REFERENCES \[{{omop_database_catalog}}\]\.\[{{omop_database_schema}}\]\.(.*) \()(.*)(\);)",
                ddl,
            )
        )
        constraints_to_drop = [
            f"IF EXISTS (SELECT 1 FROM sys.foreign_keys fk INNER JOIN sys.schemas s ON s.schema_id = fk.schema_id WHERE fk.name = '{match.group(4)}' and s.name = '{self._omop_database_schema}')\n{match.group(1)}{match.group(2)} DROP CONSTRAINT {match.group(4)};"
            for match in matches
        ]
        logging.debug("Remove the table contraints from the omop tables")
        modified_ddl = "\n".join(constraints_to_drop)
        template = self._template_env.from_string(modified_ddl)
        sql = template.render(
            omop_database_catalog=self._omop_database_catalog,
            omop_database_schema=self._omop_database_schema,
        )
        self._db.run_query(f"use [{self._omop_database_catalog}];\n" + sql)

    def _add_all_constraints(self) -> None:
        """Add all the foreign key constraints to the omop tables"""
        with open(
            str(
                Path(__file__).parent.resolve()
                / "templates"
                / "ddl"
                / f"OMOPCDM_{self._db_engine}_{self._omop_cdm_version}_constraints.sql.jinja"
            ),
            "r",
            encoding="UTF8",
        ) as file:
            ddl = file.read()
        matches = list(
            re.finditer(
                r"(ALTER TABLE \[{{omop_database_catalog}}\]\.\[{{omop_database_schema}}\]\.)(.*)( ADD CONSTRAINT )(.*) (FOREIGN KEY \()(.*)( REFERENCES \[{{omop_database_catalog}}\]\.\[{{omop_database_schema}}\]\.(.*) \()(.*)(\);)",
                ddl,
            )
        )

        constraint_ddls = {}
        for match in matches:
            table_name = match.group(2)
            if not table_name in constraint_ddls.keys():
                constraint_ddls[table_name] = []
            constraint_ddls[table_name].append(
                self._template_env.from_string(
                    f"{match.group(1)}{match.group(2)}{match.group(3)}{match.group(4)} {match.group(5)}{match.group(6)}{match.group(7)}{match.group(9)});"
                ).render(
                    omop_database_catalog=self._omop_database_catalog,
                    omop_database_schema=self._omop_database_schema,
                )
            )

        tables = (
            self._df_omop_tables.filter(
                ~(pl.col("cdmTableName").is_in(["CONCEPT"]))
            )  # CONCEPT has a circular FK reference with DOMAIN
            .select("cdmTableName")["cdmTableName"]
            .to_list()
        )

        fk_dependency_tree = self._build_fk_dependency_tree_of_tables(tables)
        fk_dependency_tree.insert(0, ["concept"])
        fk_dependency_tree.reverse()

        logging.debug("Adding the table contraints to the omop tables")
        with ThreadPoolExecutor(max_workers=self._max_worker_threads_per_table) as executor:
            for tree_level in fk_dependency_tree:
                dlls = [
                    item for row in [constraint_ddls.get(table.upper(), []) for table in tree_level] for item in row
                ]
                futures = [executor.submit(self._run_constraint_ddl, ddl) for ddl in dlls]
                # wait(futures, return_when=ALL_COMPLETED)
                for result in as_completed(futures):
                    result.result()

    def _run_constraint_ddl(self, ddl: str):
        try:
            self._db.run_query(ddl)
        except Exception as ex:
            logging.warn(
                f"Failed to run constraint ddl: '{ddl}'.\nThis usually means you have some inconsistent data in your tables.\n{ex}"
            )

    def _test_db_connection(self):
        """Test the connection to the database."""
        self._db.run_query("select 1")
        logging.info("Successfully connected to the database.")
