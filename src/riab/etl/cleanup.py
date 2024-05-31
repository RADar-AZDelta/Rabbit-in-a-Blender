# Copyright 2024 RADar-AZDelta
# SPDX-License-Identifier: gpl3+

import logging
from abc import ABC, abstractmethod
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed

from .etl_base import EtlBase


class Cleanup(EtlBase, ABC):
    """
    Class that creates the CDM folder structure that holds the raw queries, Usagi CSV's and custom concept CSV's.
    """

    def __init__(
        self,
        clear_auto_generated_custom_concept_ids: bool = False,
        **kwargs,
    ):
        super().__init__(**kwargs)

        self.clear_auto_generated_custom_concept_ids = clear_auto_generated_custom_concept_ids

    def run(self, cleanup_table: str = "all"):
        """
        Cleanup the ETL process:\n
        All work tables in the work dataset are deleted.\n
        All 'clinical' and 'health system' tables in the omop dataset are truncated. (the ones configured in the omop_tables variable)\n
        The 'source_to_concept_map' table in the omop dataset is truncated.\n
        All custom concepts are removed from the 'concept', 'concept_relationship' and 'concept_ancestor' tables in the omop dataset.\n
        All local vocabularies are removed from the 'vocabulary' table in the omop dataset.\n
        """  # noqa: E501 # pylint: disable=line-too-long
        self._pre_cleanup(cleanup_table)

        # custom cleanup
        if cleanup_table == "all":
            self._cleanup_all()
        else:
            etl_flow = deque(self._cdm_tables_fks_dependencies_resolved)

            while len(etl_flow) and (cleanup_table not in etl_flow.popleft()):
                continue

            cleanup_tables = [table for tables in etl_flow for table in tables]
            cleanup_tables.insert(0, cleanup_table)

            self._cleanup_tables(cleanup_tables)

        self._post_cleanup(cleanup_table)

    def _cleanup_tables(self, tables: list[str]):
        work_tables = self._get_work_tables()

        with ThreadPoolExecutor(max_workers=self._max_worker_threads_per_table) as executor:
            logging.info(
                "Removing mapped source id's to omop id's from SOURCE_ID_TO_OMOP_ID_MAP for OMOP tables %s",
                ",".join(tables),
            )
            self._remove_omop_ids_from_map_table(omop_tables=tables)

            usagi_tables = [
                table_name
                for table_name in work_tables
                if table_name.startswith(tuple(tables)) and table_name.endswith("_usagi")
            ]
            futures = [
                executor.submit(
                    self._cleanup_usagi_tables,
                    table_name,
                )
                for table_name in usagi_tables
            ]
            # wait(futures, return_when=ALL_COMPLETED)
            for result in as_completed(futures):
                result.result()

            concept_tables = [
                table_name
                for table_name in work_tables
                if table_name.startswith(tuple(tables)) and table_name.endswith("_concept")
            ]
            futures = [
                executor.submit(self._cleanup_custom_concept_tables, table_name) for table_name in concept_tables
            ]
            # wait(futures, return_when=ALL_COMPLETED)
            for result in as_completed(futures):
                result.result()

            futures = [executor.submit(self._custom_db_engine_cleanup, table_name) for table_name in tables]
            # wait(futures, return_when=ALL_COMPLETED)
            for result in as_completed(futures):
                result.result()

            # delete work tables
            tables_to_delete = [table_name for table_name in work_tables if table_name.startswith(tuple(tables))]
            if not self.clear_auto_generated_custom_concept_ids and "concept_id_swap" in tables_to_delete:
                tables_to_delete.remove("concept_id_swap")
            futures = [
                executor.submit(
                    self._delete_work_table,
                    table_name,
                )
                for table_name in tables_to_delete
            ]
            # wait(futures, return_when=ALL_COMPLETED)
            for result in as_completed(futures):
                result.result()

            # truncate omop tables
            omop_tables_to_truncate = [table_name for table_name in self._omop_cdm_tables if table_name in tables]
            if "vocabulary" in tables:
                omop_tables_to_truncate.append("vocabulary")

            futures = [
                executor.submit(
                    self._truncate_omop_table,
                    table_name,
                )
                for table_name in omop_tables_to_truncate
            ]
            # wait(futures, return_when=ALL_COMPLETED)
            for result in as_completed(futures):
                result.result()

    def _cleanup_all(self):
        work_tables = self._get_work_tables()

        logging.info("Truncate omop table 'source_to_concept_map'")
        self._truncate_omop_table("source_to_concept_map")

        logging.info("Truncate omop table 'source_id_to_omop_id_map'")
        self._truncate_omop_table("source_id_to_omop_id_map")

        logging.info(
            "Removing custom concepts from 'concept' table",
        )
        self._remove_custom_concepts_from_concept_table()

        self._custom_db_engine_cleanup("all")

        # delete work tables
        with ThreadPoolExecutor(max_workers=self._max_worker_threads_per_table) as executor:
            tables_to_delete = [table_name for table_name in work_tables]
            if not self.clear_auto_generated_custom_concept_ids and "concept_id_swap" in tables_to_delete:
                tables_to_delete.remove("concept_id_swap")
            futures = [
                executor.submit(
                    self._delete_work_table,
                    table_name,
                )
                for table_name in tables_to_delete
            ]
            # wait(futures, return_when=ALL_COMPLETED)
            for result in as_completed(futures):
                result.result()

            # truncate omop tables
            omop_tables_to_truncate = [table_name for table_name in self._omop_cdm_tables]
            omop_tables_to_truncate.append("vocabulary")

            futures = [
                executor.submit(
                    self._truncate_omop_table,
                    table_name,
                )
                for table_name in omop_tables_to_truncate
            ]
            # wait(futures, return_when=ALL_COMPLETED)
            for result in as_completed(futures):
                result.result()

    @abstractmethod
    def _pre_cleanup(self, cleanup_table: str = "all"):
        """Stuff to do before the cleanup (ex remove constraints from omop tables)

        Args:
            cleanup_table (str, optional): _description_. Defaults to "all".
        """
        pass

    @abstractmethod
    def _post_cleanup(self, cleanup_table: str = "all"):
        """Stuff to do after the cleanup (ex re-add constraints to omop tables)

        Args:
            cleanup_table (str, optional): Defaults to "all".
        """
        pass

    def _cleanup_usagi_tables(self, table_name: str):
        omop_table = table_name.split("__")[0]
        concept_id_column = table_name.split("__")[1].removesuffix("_usagi")
        logging.info(
            "Removing source to comcept maps from '%s' based on values from '%s' CSV",
            "source_to_concept_map",
            f"{omop_table}__{concept_id_column}_usagi",
        )
        self._remove_source_to_concept_map_using_usagi_table(omop_table, concept_id_column)

    def _cleanup_custom_concept_tables(self, table_name: str):
        try:
            omop_table = table_name.split("__")[0]
            concept_id_column = table_name.split("__")[1].removesuffix("_concept")
            logging.info(
                "Removing custom concepts from '%s' based on values from '%s' CSV",
                "concept",
                f"{omop_table}__{concept_id_column}_concept",
            )
            self._remove_custom_concepts_from_concept_table_using_usagi_table(omop_table, concept_id_column)
        except Exception as e:
            logging.warn(e)

    @abstractmethod
    def _custom_db_engine_cleanup(self, table: str) -> None:
        """Custom cleanup method for specific database engine implementation

        Args:
            table (str): Table name (all for all tables)
        """
        pass

    @abstractmethod
    def _get_work_tables(self) -> list[str]:
        """Returns a list of all our work tables (Usagi upload, custom concept upload, swap and query upload tables)

        Returns:
            list[str]: List of all the work tables
        """
        pass

    @abstractmethod
    def _truncate_omop_table(self, table_name: str) -> None:
        """Remove all rows from an OMOP table

        Args:
            table_name (str): Omop table to truncate
        """
        pass

    @abstractmethod
    def _remove_custom_concepts_from_concept_table(self) -> None:
        """Remove the custom concepts from the OMOP concept table"""
        pass

    @abstractmethod
    def _remove_custom_concepts_from_concept_table_using_usagi_table(
        self, omop_table: str, concept_id_column: str
    ) -> None:
        """Remove the custom concepts of a specific concept column of a specific OMOP table from the OMOP concept table

        Args:
            omop_table (str): The omop table
            concept_id_column (str): The conept id column
        """
        pass

    @abstractmethod
    def _remove_omop_ids_from_map_table(self, omop_tables: list[str]) -> None:
        """Remove the mapping of source to omop id's from the SOURCE_ID_TO_OMOP_ID_MAP for a specific OMOP tables.

        Args:
            omop_tables (list[str]): The omop tables
        """
        pass

    @abstractmethod
    def _remove_source_to_concept_map_using_usagi_table(self, omop_table: str, concept_id_column: str) -> None:
        """Remove the concepts of a specific concept column of a specific OMOP table from the OMOP source_to_concept_map table

        Args:
            omop_table (str): The omop table
            concept_id_column (str): The conept id column
        """
        pass

    @abstractmethod
    def _delete_work_table(self, work_table: str) -> None:
        """Remove  work table

        Args:
            work_table (str): The work table
        """
        pass
