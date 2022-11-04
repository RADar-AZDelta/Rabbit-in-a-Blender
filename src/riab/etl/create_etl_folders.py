# Copyright 2022 RADar-AZDelta
# SPDX-License-Identifier: gpl3+

import logging
from abc import ABC, abstractmethod
from pathlib import Path
from typing import cast

from .etl_base import EtlBase


class CreateEtlFolders(EtlBase, ABC):
    """
    Class that creates the CDM folder structure that holds the raw queries, Usagi CSV's and custom concept CSV's.
    """

    def __init__(
        self,
        **kwargs,
    ):
        super().__init__(**kwargs)

    def run(self) -> None:
        """
        Create the ETL folder structure that will hold your queries, Usagi CSV's an custom concept CSV's.
        Based on the OMOP CDM database tables
        """

        Path.mkdir(cast(Path, self._cdm_folder_path), exist_ok=True)

        for omop_table, table_props in vars(self._omop_tables).items():
            folder = cast(Path, self._cdm_folder_path) / omop_table
            Path.mkdir(folder, exist_ok=True)
            logging.info("Creating folder %s", folder)

            sql = self._generate_sample_raw_query(omop_table)
            query_path = (
                cast(Path, self._cdm_folder_path) / omop_table / "example_query.sql"
            )
            with open(query_path, "w", encoding="UTF8") as f:
                f.write(sql)
            logging.info("Creating example query %s", query_path)

            columns = self._get_column_names(omop_table)
            for concept_column in (
                column for column in columns if "concept_id" in column
            ):
                folder = cast(Path, self._cdm_folder_path) / omop_table / concept_column
                Path.mkdir(
                    folder,
                    exist_ok=True,
                )
                logging.info("Creating folder %s", folder)

                usagi_csv_path = (
                    cast(Path, self._cdm_folder_path)
                    / omop_table
                    / concept_column
                    / "example_usagi.csv"
                )
                with open(usagi_csv_path, "w", encoding="UTF8") as f:
                    f.write(
                        "sourceCode,sourceName,sourceFrequency,sourceAutoAssignedConceptIds,ADD_INFO:additionalInfo,matchScore,mappingStatus,equivalence,statusSetBy,statusSetOn,conceptId,conceptName,domainId,mappingType,comment,createdBy,createdOn,assignedReviewer"
                    )
                logging.info("Creating example usagi CSV %s", query_path)

                folder = (
                    cast(Path, self._cdm_folder_path)
                    / omop_table
                    / concept_column
                    / "custom"
                )
                Path.mkdir(
                    folder,
                    exist_ok=True,
                )
                logging.info("Creating folder %s", folder)

                custom_concepts_csv_path = (
                    cast(Path, self._cdm_folder_path)
                    / omop_table
                    / concept_column
                    / "custom"
                    / "example_custom_concept.csv"
                )
                with open(custom_concepts_csv_path, "w", encoding="UTF8") as f:
                    f.write(
                        "concept_id,concept_name,domain_id,vocabulary_id,concept_class_id,standard_concept,concept_code,valid_start_date,valid_end_date,invalid_reason"
                    )
                logging.info("Creating example usagi CSV %s", query_path)

    @abstractmethod
    def _generate_sample_raw_query(self, omop_table: str) -> str:
        """Generates an example SQL query to query the raw data.

        Args:
            vocabulary_table (str): The standardised vocabulary table
        """
        pass
