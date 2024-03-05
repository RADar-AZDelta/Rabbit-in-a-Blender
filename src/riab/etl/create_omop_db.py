# Copyright 2024 RADar-AZDelta
# SPDX-License-Identifier: gpl3+

from abc import ABC, abstractmethod

from .etl_base import EtlBase


class CreateOmopDb(EtlBase, ABC):
    """
    Class that creates the CDM folder structure that holds the raw queries, Usagi CSV's and custom concept CSV's.
    """

    def __init__(
        self,
        **kwargs,
    ):
        super().__init__(**kwargs)

    def run(self) -> None:
        """Create OMOP tables in the database and define indexes/partitions/clusterings"""
        for ddl_part in ["ddl", "primary_keys", "constraints", "indices"]:
            self._run_cdm_ddl_query(ddl_part)

        self._run_source_id_to_omop_id_map_table_ddl_query()
        self._run_dqd_ddl_query()

    @abstractmethod
    def _run_cdm_ddl_query(self, ddl_part: str) -> None:
        """Runs a specific ddl query"""
        pass

    @abstractmethod
    def _run_source_id_to_omop_id_map_table_ddl_query(self) -> None:
        """Creates the source_id_to_omop_id_map table"""
        pass

    @abstractmethod
    def _run_dqd_ddl_query(self) -> None:
        """Creates the Data Quality Dashboard tables"""
        pass
