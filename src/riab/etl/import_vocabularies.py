# Copyright 2024 RADar-AZDelta
# SPDX-License-Identifier: gpl3+

import logging
import platform
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

        self._vocabulary_tables = [
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

    def run(self, path_to_zip_file: str):
        """import vocabularies, as zip-file downloaded from athena.ohdsi.org, into"""



        self._pre_load()

        with ThreadPoolExecutor(max_workers=self._max_parallel_tables) as executor:
            logging.info("Deleting vocabulary upload tables.")
            futures = [
                executor.submit(
                    self._clear_vocabulary_upload_table,
                    vocabulary_table,
                )
                for vocabulary_table in self._vocabulary_tables
            ]
            # wait(futures, return_when=ALL_COMPLETED)
            for result in as_completed(futures):
                result.result()

            with (
                zipfile.ZipFile(path_to_zip_file, "r") as zip_ref,
                tempfile.TemporaryDirectory(prefix="riab_") as temp_dir_path,
            ):
                logging.info(
                    "Extracting vocabularies zip file '%s' to temporary dir '%s'",
                    path_to_zip_file,
                    temp_dir_path,
                )
                if platform.system() == "Windows":
                    import win32api

                    temp_dir_path = win32api.GetLongPathName(temp_dir_path)

                # unzip each file from the archive in parallel
                # futures = [
                #     executor.submit(zip_ref.extract, file, temp_dir_path)
                #     for file in zip_ref.namelist()
                #     if file.endswith(".csv")
                # ]
                # # wait(futures, return_when=ALL_COMPLETED)
                # for result in as_completed(futures):
                #     result.result()
                zip_ref.extractall(temp_dir_path)

                for csv_file in Path(temp_dir_path).glob("*.csv"):
                    def blocks(files, size=65536):
                        while True:
                            b = files.read(size)
                            if not b: break
                            yield b
                    with open(csv_file, "r",encoding="utf-8",errors='ignore') as f:
                        number_of_lines = sum(bl.count("\n") for bl in blocks(f))
                    logging.info(f"Vocabulary '{csv_file.name}' holds {number_of_lines} records")

                logging.info("Uploading vocabulary CSV's")
                futures = [
                    executor.submit(
                        self._convert_csv_to_parquet_and_upload,
                        vocabulary_table,
                        Path(temp_dir_path)
                        / f"{vocabulary_table.upper()}.csv",  # Uppercase because files in zip-file are still in uppercase, against the CDM 5.4 convention
                    )
                    for vocabulary_table in self._vocabulary_tables
                ]
                # wait(futures, return_when=ALL_COMPLETED)
                for result in as_completed(futures):
                    result.result()

            logging.info("Refill vocabulary tables.")
            futures = [
                executor.submit(
                    self._refill_vocabulary_table,
                    vocabulary_table,
                )
                for vocabulary_table in self._vocabulary_tables
            ]
            # wait(futures, return_when=ALL_COMPLETED)
            for result in as_completed(futures):
                result.result()

        self._post_load()

    @abstractmethod
    def _pre_load(self):
        """Stuff to do before the load (ex remove constraints from omop tables)"""
        pass

    @abstractmethod
    def _post_load(self):
        """Stuff to do after the load (ex re-add constraints to omop tables)"""
        pass

    def _convert_csv_to_parquet_and_upload(self, vocabulary_table: str, csv_file: Path):
        """
        Convert a CSV file to parquet and upload it to the vocabulary upload table.

        Args:
            vocabulary_table (str): The standardised vocabulary table
            csv_file (Path): Path to the CSV file
        """
        parquet_file = self._convert_csv_to_parquet(vocabulary_table, csv_file)
        self._load_vocabulary_parquet_in_upload_table(vocabulary_table, parquet_file)

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

        # sort the dataframe on the first column (the _id column), this will speed up the import
        df_vocabulary_table = df_vocabulary_table.sort(df_vocabulary_table.columns[0]) 

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
        df_vocabulary_table = pl.read_csv(
            csv_file, separator="\t", try_parse_dates=True, schema=polars_schema, encoding="utf-8"
        )

        date_columns = self._df_omop_fields.filter(
            (pl.col("cdmTableName").str.to_lowercase() == vocabulary_table) & (pl.col("cdmDatatype") == "date")
        ).select("cdmFieldName")
        for date_column in date_columns:
            df_vocabulary_table = df_vocabulary_table.with_columns(pl.col(date_column).str.to_date(format="%Y%m%d"))

        return df_vocabulary_table

    @abstractmethod
    def _clear_vocabulary_upload_table(self, vocabulary_table: str) -> None:
        """Removes a specific standardised vocabulary table

        Args:
            vocabulary_table (str): The standardised vocabulary table
        """
        pass

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
    def _refill_vocabulary_table(self, vocabulary_table: str) -> None:
        """Refills a specific standardised vocabulary table table from the upload table

        Args:
            vocabulary_table (str): The standardised vocabulary table
        """
        pass
