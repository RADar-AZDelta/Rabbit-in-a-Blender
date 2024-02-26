# Copyright 2024 RADar-AZDelta
# SPDX-License-Identifier: gpl3+

import logging
import tempfile
import zipfile
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import polars as pl

from .etl_base import EtlBase


class ImportVocabularies(EtlBase, ABC):
    """
    Class that creates the CDM folder structure that holds the raw queries, Usagi CSV's and custom concept CSV's.
    """

    def __init__(
        self,
        **kwargs,
    ):
        super().__init__(**kwargs)

    def run(self, path_to_zip_file: str):
        """import vocabularies, as zip-file downloaded from athena.ohdsi.org, into"""
        with zipfile.ZipFile(path_to_zip_file, "r") as zip_ref:
            with tempfile.TemporaryDirectory(prefix="omop_vocabularies_") as temp_dir_path:
                logging.info(
                    "Extracting vocabularies zip file '%s' to temporary dir '%s'",
                    path_to_zip_file,
                    temp_dir_path,
                )
                zip_ref.extractall(temp_dir_path)

                with ThreadPoolExecutor(max_workers=self._max_workers) as executor:
                    vocabulary_tables = [
                        "concept",
                        "concept_ancestor",
                        "concept_class",
                        "concept_relationship",
                        "concept_synonym",
                        "domain",
                        "drug_strength",
                        "relationship",
                        "vocabulary",
                    ]

                    logging.info("Deleting vocabulary upload tables.")
                    futures = [
                        executor.submit(
                            self._clear_vocabulary_upload_table,
                            vocabulary_table,
                        )
                        for vocabulary_table in vocabulary_tables
                    ]
                    # wait(futures, return_when=ALL_COMPLETED)
                    for result in as_completed(futures):
                        result.result()

                    logging.info("Uploading vocabulary CSV's")
                    futures = [
                        executor.submit(
                            self._convert_csv_to_parquet_and_upload,
                            vocabulary_table,
                            Path(temp_dir_path)
                            / f"{vocabulary_table.upper()}.csv",  # Uppercase because files in zip-file are still in uppercase, against the CDM 5.4 convention
                        )
                        for vocabulary_table in vocabulary_tables
                    ]
                    # wait(futures, return_when=ALL_COMPLETED)
                    for result in as_completed(futures):
                        result.result()

                    logging.info("Creating vocabulary tables.")
                    futures = [
                        executor.submit(
                            self._refill_vocabulary_table,
                            vocabulary_table,
                        )
                        for vocabulary_table in vocabulary_tables
                    ]
                    # wait(futures, return_when=ALL_COMPLETED)
                    for result in as_completed(futures):
                        result.result()

    def _convert_csv_to_parquet_and_upload(self, vocabulary_table: str, csv_file: Path):
        """
        Convert a CSV file to parquet and upload it to the vocabulary upload table.

        Args:
            vocabulary_table (str): The standardised vocabulary table
            csv_file (Path): Path to the CSV file
        """
        parquet_file = self._convert_csv_to_parquet(vocabulary_table, csv_file)
        self._load_vocabulary_parquet_in_upload_table(vocabulary_table, parquet_file)

    def _get_polars_type(self, cdmDatatype: str) -> pl.DataType:
        match cdmDatatype:
            case "integer":
                return pl.Int64  # type: ignore
            case "datetime":
                return pl.Datetime  # type: ignore
            case "varchar(50)":
                return pl.Utf8  # type: ignore
            case "date":
                return pl.Utf8  # type: ignore
            # WARNING: pl.Date # Data not parsed well --> will do it manually
            case "Integer":
                return pl.Int64  # type: ignore
            case "varchar(20)":
                return pl.Utf8  # type: ignore
            case "float":
                return pl.Float64  # type: ignore
            case "varchar(MAX)":
                return pl.Utf8  # type: ignore
            case "varchar(255)":
                return pl.Utf8  # type: ignore
            case "varchar(10)":
                return pl.Utf8  # type: ignore
            case "varchar(60)":
                return pl.Utf8  # type: ignore
            case "varchar(250)":
                return pl.Utf8  # type: ignore
            case "varchar(1)":
                return pl.Utf8  # type: ignore
            case "varchar(2000)":
                return pl.Utf8  # type: ignore
            case "varchar(2)":
                return pl.Utf8  # type: ignore
            case "varchar(9)":
                return pl.Utf8  # type: ignore
            case "varchar(80)":
                return pl.Utf8  # type: ignore
            case "varchar(3)":
                return pl.Utf8  # type: ignore
            case "varchar(25)":
                return pl.Utf8  # type: ignore
            case "varchar(1000)":
                return pl.Utf8  # type: ignore
            case _:
                raise ValueError(f"Unknown cdmDatatype: {cdmDatatype}")

    def _get_polars_schema_for_cdm_table(self, vocabulary_table: str) -> dict[str, pl.DataType]:
        df_table_fields = self._df_omop_fields.filter(
            pl.col("cdmTableName").str.to_lowercase() == vocabulary_table
        ).select(["cdmFieldName", "cdmDatatype"])
        polars_schema: dict[str, pl.DataType] = {}
        cdmFieldName: str
        cdmDatatype: str
        for cdmFieldName, cdmDatatype in df_table_fields.iter_rows():
            polars_schema[cdmFieldName] = self._get_polars_type(cdmDatatype)
        return polars_schema

    def _convert_csv_to_parquet(self, vocabulary_table: str, csv_file: Path) -> Path:
        """Converts a dictionary CSV file to a parquet file

        Args:
            vocabulary_table (str): The standardised vocabulary table
            csv_file (Path): Path to the CSV file

        Returns:
            Path: Path to the parquet file
        """
        logging.debug("Converting '%s.csv' to parquet", vocabulary_table)
        df_vocabulary_table = self._read_vocabulary_csv(vocabulary_table, csv_file)

        parquet_file = csv_file.parent / f"{vocabulary_table}.parquet"
        df_vocabulary_table.write_parquet(parquet_file)
        return parquet_file

    def _read_vocabulary_csv(self, vocabulary_table: str, csv_file: Path) -> pl.DataFrame:
        """Reads a specific standardised vocabulary table CSV file and converts it into an Polars DataFrame

        Args:
            vocabulary_table (str): The standardised vocabulary table
            csv_file (Path): Path to the CSV file

        Returns:
            pl.DataFrame: The CSV converted in an data frame
        """
        polars_schema = self._get_polars_schema_for_cdm_table(vocabulary_table)
        df_vocabulary_table = pl.read_csv(csv_file, separator="\t", try_parse_dates=True, schema=polars_schema)

        date_columns = self._df_omop_fields.filter(
            (pl.col("cdmTableName").str.to_lowercase() == vocabulary_table) & (pl.col("cdmDatatype") == "date")
        ).select("cdmFieldName")
        for date_column in date_columns:
            df_vocabulary_table = df_vocabulary_table.with_columns(pl.col(date_column).str.to_date(format="%Y%m%d"))

        return df_vocabulary_table

    @abstractmethod
    def _load_vocabulary_parquet_in_upload_table(
        self,
        vocabulary_table: str,
        parquet_file: Path,
    ) -> None:
        """Loads the CSV file in the specific standardised vocabulary table

        Args:
            vocabulary_table (str): The standardised vocabulary table
            parquet_file (Path): Path to the CSV file
        """
        pass

    @abstractmethod
    def _clear_vocabulary_upload_table(self, vocabulary_table: str) -> None:
        """Removes a specific standardised vocabulary table

        Args:
            vocabulary_table (str): The standardised vocabulary table
        """
        pass

    @abstractmethod
    def _refill_vocabulary_table(self, vocabulary_table: str) -> None:
        """Refills a specific standardised vocabulary table table from the upload table

        Args:
            vocabulary_table (str): The standardised vocabulary table
        """
        pass
