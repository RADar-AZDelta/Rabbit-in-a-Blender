# Copyright 2024 RADar-AZDelta
# SPDX-License-Identifier: gpl3+

import logging
from threading import Lock
from typing import List

from google.cloud.exceptions import NotFound

from ..cleanup import Cleanup
from ..etl_base import EtlBase
from .bigquery_etl_base import BigQueryEtlBase


class BigQueryCleanup(Cleanup, BigQueryEtlBase):
    def __init__(
        self,
        **kwargs,
    ):
        super().__init__(**kwargs)

        self._lock_source_to_concept_map_cleanup = Lock()

    def _get_work_tables(self) -> List[str]:
        """Returns a list of all our work tables (Usagi upload, custom concept upload, swap and query upload tables)

        Returns:
            List[str]: List of all the work tables
        """
        work_tables = self._get_all_table_names(self._dataset_work)
        return work_tables

    def _truncate_omop_table(self, table_name: str) -> None:
        """Remove all rows from an OMOP table

        Args:
            table_name (str): Omop table to truncate
        """
        logging.info("Truncate OMOP table '%s'", table_name)
        template = self._template_env.get_template("cleanup/truncate.sql.jinja")
        sql = template.render(
            dataset_omop=self._dataset_omop,
            table_name=table_name,
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

    def _remove_custom_concepts_from_concept_relationship_table(self) -> None:
        """Remove the custom concepts from the OMOP concept_relationship table"""
        template = self._template_env.get_template("cleanup/CONCEPT_RELATIONSHIP_remove_custom_concepts.sql.jinja")
        sql = template.render(
            dataset_omop=self._dataset_omop,
            min_custom_concept_id=EtlBase._CUSTOM_CONCEPT_IDS_START,
        )
        self._gcp.run_query_job(sql)

    def _remove_custom_concepts_from_concept_ancestor_table(self) -> None:
        """Remove the custom concepts from the OMOP concept_ancestor table"""
        template = self._template_env.get_template("cleanup/CONCEPT_ANCESTOR_remove_custom_concepts.sql.jinja")
        sql = template.render(
            dataset_omop=self._dataset_omop,
            min_custom_concept_id=EtlBase._CUSTOM_CONCEPT_IDS_START,
        )
        self._gcp.run_query_job(sql)

    def _remove_custom_concepts_from_vocabulary_table(self) -> None:
        """Remove the custom concepts from the OMOP vocabulary table"""
        template = self._template_env.get_template("cleanup/VOCABULARY_remove_custom_concepts.sql.jinja")
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

    def _remove_omop_ids_from_map_table(self, omop_table: str) -> None:
        """Remove the mapping of source to omop id's from the SOURCE_ID_TO_OMOP_ID_MAP for a specific OMOP table.

        Args:
            omop_table (str): The omop table
        """  # noqa: E501 # pylint: disable=line-too-long
        template = self._template_env.get_template(
            "cleanup/SOURCE_ID_TO_OMOP_ID_MAP_remove_ids_by_omop_table.sql.jinja"
        )
        sql = template.render(
            dataset_omop=self._dataset_omop,
            omop_table=omop_table,
        )
        self._gcp.run_query_job(sql)

    def _remove_custom_concepts_from_concept_relationship_table_using_usagi_table(
        self, omop_table: str, concept_id_column: str
    ) -> None:
        """Remove the custom concepts of a specific concept column of a specific OMOP table from the OMOP concept_relationship table

        Args:
            omop_table (str): The omop table
            concept_id_column (str): The conept id column
        """  # noqa: E501 # pylint: disable=line-too-long
        template = self._template_env.get_template(
            "cleanup/CONCEPT_RELATIONSHIP_remove_custom_concepts_by_{omop_table}__{concept_id_column}_usagi_table.sql.jinja"  # noqa: E501 # pylint: disable=line-too-long
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

    def _remove_custom_concepts_from_concept_ancestor_table_using_usagi_table(
        self, omop_table: str, concept_id_column: str
    ) -> None:
        """Remove the custom concepts of a specific concept column of a specific OMOP table from the OMOP concept_ancestor table

        Args:
            omop_table (str): The omop table
            concept_id_column (str): The conept id column
        """  # noqa: E501 # pylint: disable=line-too-long
        template = self._template_env.get_template(
            "cleanup/CONCEPT_ANCESTOR_remove_custom_concepts_by_{omop_table}__{concept_id_column}_usagi_table.sql.jinja"  # noqa: E501 # pylint: disable=line-too-long
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

    def _remove_custom_concepts_from_vocabulary_table_using_usagi_table(
        self, omop_table: str, concept_id_column: str
    ) -> None:
        """Remove the custom concepts of a specific concept column of a specific OMOP table from the OMOP vocabulary table

        Args:
            omop_table (str): The omop table
            concept_id_column (str): The conept id column
        """  # noqa: E501 # pylint: disable=line-too-long
        template = self._template_env.get_template(
            "cleanup/VOCABULARY_remove_custom_concepts_by_{omop_table}__{concept_id_column}_usagi_table.sql.jinja"
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
        except Exception as ex:
            raise ex
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
