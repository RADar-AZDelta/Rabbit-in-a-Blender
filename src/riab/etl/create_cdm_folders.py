# Copyright 2024 RADar-AZDelta
# SPDX-License-Identifier: gpl3+

import logging
from abc import ABC, abstractmethod
from pathlib import Path
from typing import cast

import polars as pl

from .etl_base import EtlBase


class CreateCdmFolders(EtlBase, ABC):
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

        for omop_table in self._omop_etl_tables:
            omop_fields = self._df_omop_fields.filter(pl.col("cdmTableName").str.to_lowercase() == omop_table)

            folder = cast(Path, self._cdm_folder_path) / omop_table
            Path.mkdir(folder, exist_ok=True)
            logging.info("Creating folder %s", folder)

            sql = self._generate_sample_etl_query(omop_table, omop_fields)
            query_path = cast(Path, self._cdm_folder_path) / omop_table / "example.sql._jinja"
            with open(query_path, "w", encoding="UTF8") as f:
                f.write(sql)
            logging.info("Creating example RTL query %s", query_path)
            concept_columns = omop_fields.filter(pl.col("fkTableName").str.to_lowercase() == "concept").rows(named=True)
            for concept_column in concept_columns:
                folder = cast(Path, self._cdm_folder_path) / omop_table / concept_column["cdmFieldName"]
                Path.mkdir(
                    folder,
                    exist_ok=True,
                )
                logging.info("Creating folder %s", folder)

                sql = self._generate_sample_usagi_query(omop_table, concept_column)
                query_path = (
                    cast(Path, self._cdm_folder_path)
                    / omop_table
                    / concept_column["cdmFieldName"]
                    / "example.sql._jinja"
                )
                with open(query_path, "w", encoding="UTF8") as f:
                    f.write(sql)
                logging.info("Creating example Usagi query %s", query_path)

                usagi_csv_path = (
                    cast(Path, self._cdm_folder_path) / omop_table / concept_column["cdmFieldName"] / "example._csv"
                )
                with open(usagi_csv_path, "w", encoding="UTF8") as f:
                    f.write("sourceCode,sourceName,sourceFrequency")
                logging.info("Creating example usagi source CSV %s", query_path)

                usagi_csv_path = (
                    cast(Path, self._cdm_folder_path)
                    / omop_table
                    / concept_column["cdmFieldName"]
                    / "example_usagi._csv"
                )
                with open(usagi_csv_path, "w", encoding="UTF8") as f:
                    f.write(
                        "sourceCode,sourceName,sourceFrequency,sourceAutoAssignedConceptIds,ADD_INFO:additionalInfo,matchScore,mappingStatus,equivalence,statusSetBy,statusSetOn,conceptId,conceptName,domainId,mappingType,comment,createdBy,createdOn,assignedReviewer"
                    )
                logging.info("Creating example usagi CSV %s", query_path)

                folder = cast(Path, self._cdm_folder_path) / omop_table / concept_column["cdmFieldName"] / "custom"
                Path.mkdir(
                    folder,
                    exist_ok=True,
                )
                logging.info("Creating folder %s", folder)

                custom_concepts_csv_path = (
                    cast(Path, self._cdm_folder_path)
                    / omop_table
                    / concept_column["cdmFieldName"]
                    / "custom"
                    / "example._csv"
                )
                with open(custom_concepts_csv_path, "w", encoding="UTF8") as f:
                    f.write(
                        "concept_id,concept_name,domain_id,vocabulary_id,concept_class_id,standard_concept,concept_code,valid_start_date,valid_end_date,invalid_reason"
                    )
                logging.info("Creating example custom concept CSV %s", query_path)

    @abstractmethod
    def _generate_sample_etl_query(self, omop_table: str, omop_fields: pl.DataFrame) -> str:
        """Generates an example SQL query to query the raw data.

        Args:
            vocabulary_table (str): The standardised vocabulary table
        """
        pass

    @abstractmethod
    def _generate_sample_usagi_query(self, omop_table: str, concept_column: dict[str, str]) -> str:
        """Generates an example SQL query to generate the Usagi source CSV.

        Args:
            omop_table (str): The OMOP table
            concept_column 'str): The concept column
        """
