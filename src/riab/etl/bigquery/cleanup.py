# Copyright 2024 RADar-AZDelta
# SPDX-License-Identifier: gpl3+

import logging
from threading import Lock

from google.cloud.exceptions import NotFound

from ..cleanup import Cleanup
from ..etl_base import EtlBase
from .etl_base import BigQueryEtlBase


class BigQueryCleanup(Cleanup, BigQueryEtlBase):
    def __init__(
        self,
        **kwargs,
    ):
        super().__init__(**kwargs)

        self._lock_source_to_concept_map_cleanup = Lock()

    def _pre_cleanup(self, cleanup_table: str = "all"):
        """Stuff to do before the cleanup (ex remove constraints from omop tables)

        Args:
            cleanup_table (str, optional): _description_. Defaults to "all".
        """
        pass

    def _post_cleanup(self, cleanup_table: str = "all"):
        """Stuff to do after the cleanup (ex re-add constraints to omop tables)

        Args:
            cleanup_table (str, optional): Defaults to "all".
        """
        pass

    def _get_work_tables(self) -> list[str]:
        """Returns a list of all our work tables (Usagi upload, custom concept upload, swap and query upload tables)

        Returns:
            list[str]: List of all the work tables
        """
        template = self._template_env.get_template("cleanup/all_work_table_names.sql.jinja")
        sql = template.render(dataset=self._dataset_work)
        rows = self._gcp.run_query_job(sql)
        return [row.table_name for row in rows]

    def _truncate_omop_table(self, table_name: str) -> None:
        """Remove all rows from an OMOP table

        Args:
            table_name (str): Omop table to truncate
        """
        template = self._template_env.get_template("cleanup/truncate.sql.jinja")
        sql = template.render(
            dataset_omop=self._dataset_omop,
            table_name=table_name,
            min_custom_concept_id=EtlBase._CUSTOM_CONCEPT_IDS_START,
        )
        self._gcp.run_query_job(sql)

    def _remove_custom_concepts_from_concept_table(self) -> None:
        """Remove the custom concepts from the OMOP concept table"""
        template = self._template_env.get_template("cleanup/CONCEPT_remove_custom_concepts.sql.jinja")
        sql = template.render(
            dataset_omop=self._dataset_omop,
            min_custom_concept_id=EtlBase._CUSTOM_CONCEPT_IDS_START,
        )
        self._gcp.run_query_job(sql)

    def _remove_custom_concepts_from_concept_table_using_usagi_table(
        self, omop_table: str, concept_id_column: str
    ) -> None:
        """Remove the custom concepts of a specific concept column of a specific OMOP table from the OMOP concept table

        Args:
            omop_table (str): The omop table
            concept_id_column (str): The conept id column
        """  # noqa: E501 # pylint: disable=line-too-long
        template = self._template_env.get_template(
            "cleanup/CONCEPT_remove_custom_concepts_by_{omop_table}__{concept_id_column}_usagi_table.sql.jinja"
        )
        sql = template.render(
            dataset_omop=self._dataset_omop,
            dataset_work=self._dataset_work,
            min_custom_concept_id=EtlBase._CUSTOM_CONCEPT_IDS_START,
            omop_table=omop_table,
            concept_id_column=concept_id_column,
        )
        try:
            self._gcp.run_query_job(sql)
        except NotFound:
            logging.debug(
                "Table %s__%s_usagi_table not found in work dataset",
                omop_table,
                concept_id_column,
            )

    def _remove_omop_ids_from_map_table(self, omop_tables: list[str]) -> None:
        """Remove the mapping of source to omop id's from the SOURCE_ID_TO_OMOP_ID_MAP for a specific OMOP tables.

        Args:
            omop_tables (list[str]): The omop tables
        """
        template = self._template_env.get_template(
            "cleanup/SOURCE_ID_TO_OMOP_ID_MAP_remove_ids_by_omop_table.sql.jinja"
        )
        sql = template.render(
            dataset_omop=self._dataset_omop,
            omop_tables=omop_tables,
        )
        self._gcp.run_query_job(sql)

    def _remove_source_to_concept_map_using_usagi_table(self, omop_table: str, concept_id_column: str) -> None:
        """Remove the concepts of a specific concept column of a specific OMOP table from the OMOP source_to_concept_map table

        Args:
            omop_table (str): The omop table
            concept_id_column (str): The conept id column
        """  # noqa: E501 # pylint: disable=line-too-long
        template = self._template_env.get_template(
            "cleanup/SOURCE_TO_CONCEPT_MAP_remove_concepts_by_{omop_table}__{concept_id_column}_usagi_table.sql.jinja"
        )
        sql = template.render(
            dataset_omop=self._dataset_omop,
            dataset_work=self._dataset_work,
            min_custom_concept_id=EtlBase._CUSTOM_CONCEPT_IDS_START,
            omop_table=omop_table,
            concept_id_column=concept_id_column,
        )

        self._lock_source_to_concept_map_cleanup.acquire()
        try:
            self._gcp.run_query_job(sql)
        except Exception:
            logging.warn(
                f"Cannot cleanup source_to_concept_map table with the concepts from the usagi concepts of {omop_table}.{concept_id_column}"
            )
        finally:
            self._lock_source_to_concept_map_cleanup.release()

    def _delete_work_table(self, work_table: str) -> None:
        """Remove  work table

        Args:
            work_table (str): The work table
        """
        table_id = f"{self._dataset_work}.{work_table}"
        logging.info("Deleting table '%s'", table_id)
        self._gcp.delete_table(self._dataset_work, work_table)

    def _custom_db_engine_cleanup(self, table: str) -> None:
        """Custom cleanup method for specific database engine implementation

        Args:
            table (str): Table name (all for all tables)
        """
        if table == "all":
            self._gcp.delete_from_bucket(f"{self._bucket_uri}")
        else:
            self._gcp.delete_from_bucket(f"{self._bucket_uri}/{table}")
