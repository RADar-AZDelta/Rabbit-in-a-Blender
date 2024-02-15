# Copyright 2024 RADar-AZDelta
# SPDX-License-Identifier: gpl3+

import logging
import tempfile
import zipfile
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

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

                    logging.info("Deleting vocabulary tables.")
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
                            self._load_vocabulary_in_upload_table,
                            Path(temp_dir_path)
                            / f"{vocabulary_table.upper()}.csv",  # Uppercase because files in zip-file are still in uppercase, against the CDM 5.4 convention
                            vocabulary_table,
                        )
                        for vocabulary_table in vocabulary_tables
                    ]
                    # wait(futures, return_when=ALL_COMPLETED)
                    for result in as_completed(futures):
                        result.result()

                    logging.info("Creating vocabulary tables.")
                    futures = [
                        executor.submit(
                            self._recreate_vocabulary_table,
                            vocabulary_table,
                        )
                        for vocabulary_table in vocabulary_tables
                    ]
                    # wait(futures, return_when=ALL_COMPLETED)
                    for result in as_completed(futures):
                        result.result()

    @abstractmethod
    def _load_vocabulary_in_upload_table(self, csv_file: Path, vocabulary_table: str) -> None:
        """Loads the CSV file in the specific standardised vocabulary table

        Args:
            csv_file (Path): Path to the CSV file
            vocabulary_table (str): The standardised vocabulary table
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
    def _recreate_vocabulary_table(self, vocabulary_table: str) -> None:
        """Recreates a specific standardised vocabulary table

        Args:
            vocabulary_table (str): The standardised vocabulary table
        """
        pass
