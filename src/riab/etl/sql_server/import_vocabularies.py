# Copyright 2024 RADar-AZDelta
# SPDX-License-Identifier: gpl3+

import logging
import os
import re
import subprocess
from pathlib import Path
from threading import Lock

import polars as pl
from sqlalchemy import text

from ..import_vocabularies import ImportVocabularies
from .etl_base import SqlServerEtlBase


class SqlServerImportVocabularies(ImportVocabularies, SqlServerEtlBase):
    """
    Class that imports the downloaded vocabulary zip from the Athena website.
    """

    def __init__(
        self,
        **kwargs,
    ):
        super().__init__(**kwargs)

        self._lock_upload_table = Lock()

    def _load_vocabulary_parquet_in_upload_table(self, vocabulary_table: str, parquet_file: Path) -> None:
        """Loads the CSV file in the specific standardised vocabulary table

        Args:
            vocabulary_table (str): The standardised vocabulary table
            parquet_file (Path): Path to the CSV file
        """
        csv_file = str(parquet_file.parent / (parquet_file.stem.upper() + ".csv"))

        logging.debug(
            "Converting parquet file %s to BCP input file for vocabulary table %s", parquet_file, vocabulary_table
        )
        # format the date fields in the CSV! bcp uses the ODBC date format (yyyy-mm-dd hh:mm:ss[. f...])
        df = pl.read_parquet(parquet_file)

        # df.with_columns(
        #     pl.col("concept_synonym_name").str.len_bytes().alias("n_bytes"),
        #     pl.col("concept_synonym_name").str.len_chars().alias("n_chars"),
        # ).select(pl.col("n_bytes").max().alias("max_n_bytes"), pl.col("n_chars").max().alias("max_n_chars"))

        df.write_csv(
            csv_file + ".bak",
            separator="\t",
            line_terminator="\n",
            include_header=True,
            datetime_format="%F %T",
            date_format="%F",
            time_format="%T",
            # quote_style="never",
            # include_bom=True,
        )

        logging.debug("Loading '%s' into vocabulary table %s", csv_file + ".bak", vocabulary_table)
        args = [
            "bcp" + (".exe" if os.name == "nt" else ""),
            f"{self._omop_database_schema}.{vocabulary_table}",
            "in",
            csv_file + ".bak",
            f"-d{self._omop_database_catalog}",
            f"-S{self._server},{self._port}",
            f"-U{self._user}",
            f"-P{self._password}",
            "-c",
            "-C1252",
            "-t\t",
            "-r\n",
            "-F2",
            "-k",
            "-b10000",
            f"-ebcp_{vocabulary_table}.err",
        ]
        process = subprocess.Popen(args)  # , shell=True, stdout=subprocess.PIPE)
        exit_code = process.wait()
        if exit_code != 0:
            raise Exception(f"BCP failed with exit code {exit_code}")

    def _clear_vocabulary_upload_table(self, vocabulary_table: str) -> None:
        """Removes a specific standardised vocabulary table

        Args:
            vocabulary_table (str): The standardised vocabulary table
        """
        logging.debug("Remove the table contraints from vocabulary table %s", vocabulary_table)
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
        matches = re.finditer(
            rf"(ALTER TABLE {{{{omop_database_catalog}}}}\.{{{{omop_database_schema}}}}\.)(.*)( ADD CONSTRAINT )(.*)(FOREIGN KEY \()(.*)( REFERENCES {{{{omop_database_catalog}}}}\.{{{{omop_database_schema}}}}\.{vocabulary_table.upper()} \()(.*)(\);)",
            ddl,
        )
        modified_ddl = "\n".join(
            [f"{match.group(1)}{match.group(2)} DROP CONSTRAINT {match.group(4)};" for match in matches]
        )
        template = self._template_env.from_string(modified_ddl)
        sql = template.render(
            omop_database_catalog=self._omop_database_catalog,
            omop_database_schema=self._omop_database_schema,
        )

        self._lock_upload_table.acquire()
        try:
            with self._engine.connect() as conn:
                conn.execute(text(sql))
                conn.commit()
        except Exception as ex:
            raise ex
        finally:
            self._lock_upload_table.release()

        logging.debug("Truncate vocabulary table %s", vocabulary_table)
        template = self._template_env.get_template("vocabulary/vocabulary_table_truncate.sql.jinja")
        sql = template.render(
            omop_database_catalog=self._omop_database_catalog,
            omop_database_schema=self._omop_database_schema,
            vocabulary_table=vocabulary_table,
        )
        with self._engine.connect() as conn:
            conn.execute(text(sql))
            conn.commit()

    def _refill_vocabulary_table(self, vocabulary_table: str) -> None:
        """Recreates a specific standardised vocabulary table from the upload table

        Args:
            vocabulary_table (str): The standardised vocabulary table
        """
        logging.debug("Recreating constraints in vocabulary table %s", vocabulary_table)
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

        matches = re.finditer(
            rf"(ALTER TABLE {{{{omop_database_catalog}}}}\.{{{{omop_database_schema}}}}\.)(.*)( ADD CONSTRAINT )(.*)(FOREIGN KEY \()(.*)( REFERENCES {{{{omop_database_catalog}}}}\.{{{{omop_database_schema}}}}\.{vocabulary_table.upper()} \()(.*)(\);)",
            ddl,
        )
        modified_ddl = "\n".join(
            [
                f"{match.group(1)}{match.group(2)}{match.group(3)}{match.group(4)}{match.group(5)}{match.group(6)}{match.group(7)}{match.group(8)}{match.group(9)}"
                for match in matches
            ]
        )
        template = self._template_env.from_string(modified_ddl)
        sql = template.render(
            omop_database_catalog=self._omop_database_catalog,
            omop_database_schema=self._omop_database_schema,
        )
        self._lock_upload_table.acquire()
        try:
            with self._engine.connect() as conn:
                conn.execute(text(sql))
                conn.commit()
        except Exception as ex:
            raise ex
        finally:
            self._lock_upload_table.release()
