# Copyright 2022 RADar-AZDelta
# SPDX-License-Identifier: gpl3+

"""Holds the ETL abstract base class"""
import json
from abc import ABC, abstractmethod
from pathlib import Path
from types import SimpleNamespace
from typing import List


class EtlBase(ABC):
    """
    Base class for the ETL commands
    """

    _CUSTOM_CONCEPT_IDS_START = 2_000_000_000  # Concepts reserved for site-specific codes and mappings start from 2 billion

    def __init__(
        self,
        cdm_folder_path: str | None = None,
        omop_cdm_version: str = "v5.4",
        max_workers=16,
    ):
        """Constructor
        Base class constructor for the ETL commands

        Args:
            cdm_folder_path (str): The path to the OMOP folder structure that holds for each OMOP CDM table (folder) the ETL queries, Usagi CSV's and custom concept CSV's0
        """  # noqa: E501 # pylint: disable=line-too-long
        self._cdm_folder_path = (
            Path(cdm_folder_path).resolve() if cdm_folder_path else None
        )
        self._omop_cdm_version = omop_cdm_version
        self._max_workers = max_workers

        with open(
            str(Path(__file__).parent.resolve() / "cdm_5.4_schema.json"),
            "r",
            encoding="UTF8",
        ) as file:
            self._omop_tables = json.load(
                file, object_hook=lambda x: SimpleNamespace(**x)
            )

    @abstractmethod
    def _get_column_names(self, omop_table_name: str) -> List[str]:
        """Get list of column names of a omop table.

        Args:
            omop_table_name (str): OMOP table

        Returns:
            List[str]: list of column names
        """
        pass

    @abstractmethod
    def _get_required_column_names(self, omop_table_name: str) -> List[str]:
        """Get list of required column names of a omop table.

        Args:
            omop_table_name (str): OMOP table

        Returns:
            List[str]: list of column names
        """
        pass
