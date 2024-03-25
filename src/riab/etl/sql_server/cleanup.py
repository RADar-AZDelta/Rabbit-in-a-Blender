# Copyright 2024 RADar-AZDelta
# SPDX-License-Identifier: gpl3+

import logging
import re
from pathlib import Path
from threading import Lock
from typing import List, Sequence, cast

from ..cleanup import Cleanup
from ..etl_base import EtlBase
from .etl_base import SqlServerEtlBase


class SqlServerCleanup(Cleanup, SqlServerEtlBase):
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
        template = self._template_env.get_template("cleanup/all_work_table_names.sql.jinja")
        sql = template.render(
            work_database_catalog=self._work_database_catalog, work_database_schema=self._work_database_schema
        )
        rows: Sequence = cast(Sequence, self._run_query(sql))
        return [row.table_name for row in rows]

    def _truncate_omop_table(self, table_name: str) -> None:
        """Remove all rows from an OMOP table

        Args:
            table_name (str): Omop table to truncate
        """
        logging.debug("Remove the table contraints from omop table %s", table_name)
        with open(
            str(
                Path(__file__).parent.resolve()
                / "templates"
                / "ddl"
                / f"OMOPCDM_{self._db_engine}_{self._omop_cdm_version}_constraints.sql.jinja"
            ),
            "r",
            encoding="UTF8",
        ) as file:
            ddl = file.read()
        matches = list(
            re.finditer(
                rf"(ALTER TABLE {{{{omop_database_catalog}}}}\.{{{{omop_database_schema}}}}\.)(.*)( ADD CONSTRAINT )(.*)(FOREIGN KEY \()(.*)( REFERENCES {{{{omop_database_catalog}}}}\.{{{{omop_database_schema}}}}\.{table_name.upper()} \()(.*)(\);)",
                ddl,
            )
        )
        constraints_to_drop = [
            f"{match.group(1)}{match.group(2)} DROP CONSTRAINT {match.group(4)};" for match in matches
        ]
        constraints_to_add = [
            f"{match.group(1)} {match.group(2)} {match.group(3)} {match.group(4)} {match.group(5)} {match.group(6)} {match.group(7)} {match.group(8)} {match.group(9)}"
            for match in matches
        ]

        if len(constraints_to_drop):
            modified_ddl = "\n".join(constraints_to_drop)
            template = self._template_env.from_string(modified_ddl)
            sql = template.render(
                omop_database_catalog=self._omop_database_catalog,
                omop_database_schema=self._omop_database_schema,
            )
            self._run_query(sql)

        logging.debug("Truncate omop table %s", table_name)
        template = self._template_env.get_template("cleanup/truncate.sql.jinja")
        sql = template.render(
            omop_database_catalog=self._omop_database_catalog,
            omop_database_schema=self._omop_database_schema,
            table_name=table_name,
        )
        self._run_query(sql)

        if len(constraints_to_add):
            modified_ddl = "\n".join(constraints_to_add)
            template = self._template_env.from_string(modified_ddl)
            sql = template.render(
                omop_database_catalog=self._omop_database_catalog,
                omop_database_schema=self._omop_database_schema,
            )
            self._run_query(sql)

    def _remove_custom_concepts_from_concept_table(self) -> None:
        """Remove the custom concepts from the OMOP concept table"""
        template = self._template_env.get_template("cleanup/CONCEPT_remove_custom_concepts.sql.jinja")
        sql = template.render(
            omop_database_catalog=self._omop_database_catalog,
            omop_database_schema=self._omop_database_schema,
            min_custom_concept_id=EtlBase._CUSTOM_CONCEPT_IDS_START,
        )
        self._run_query(sql)

    def _remove_custom_concepts_from_concept_relationship_table(self) -> None:
        """Remove the custom concepts from the OMOP concept_relationship table"""
        template = self._template_env.get_template("cleanup/CONCEPT_RELATIONSHIP_remove_custom_concepts.sql.jinja")
        sql = template.render(
            omop_database_catalog=self._omop_database_catalog,
            omop_database_schema=self._omop_database_schema,
            min_custom_concept_id=EtlBase._CUSTOM_CONCEPT_IDS_START,
        )
        self._run_query(sql)

    def _remove_custom_concepts_from_concept_ancestor_table(self) -> None:
        """Remove the custom concepts from the OMOP concept_ancestor table"""
        template = self._template_env.get_template("cleanup/CONCEPT_ANCESTOR_remove_custom_concepts.sql.jinja")
        sql = template.render(
            omop_database_catalog=self._omop_database_catalog,
            omop_database_schema=self._omop_database_schema,
            min_custom_concept_id=EtlBase._CUSTOM_CONCEPT_IDS_START,
        )
        self._run_query(sql)

    def _remove_custom_concepts_from_vocabulary_table(self) -> None:
        """Remove the custom concepts from the OMOP vocabulary table"""
        template = self._template_env.get_template("cleanup/VOCABULARY_remove_custom_concepts.sql.jinja")
        sql = template.render(
            omop_database_catalog=self._omop_database_catalog,
            omop_database_schema=self._omop_database_schema,
            min_custom_concept_id=EtlBase._CUSTOM_CONCEPT_IDS_START,
        )
        self._run_query(sql)

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
            omop_database_catalog=self._omop_database_catalog,
            omop_database_schema=self._omop_database_schema,
            work_database_catalog=self._work_database_catalog,
            work_database_schema=self._work_database_schema,
            min_custom_concept_id=EtlBase._CUSTOM_CONCEPT_IDS_START,
            omop_table=omop_table,
            concept_id_column=concept_id_column,
        )
        try:
            self._run_query(sql)
        except Exception:
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
            omop_database_catalog=self._omop_database_catalog,
            omop_database_schema=self._omop_database_schema,
            omop_table=omop_table,
        )
        self._run_query(sql)

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
            omop_database_catalog=self._omop_database_catalog,
            omop_database_schema=self._omop_database_schema,
            work_database_catalog=self._work_database_catalog,
            work_database_schema=self._work_database_schema,
            min_custom_concept_id=EtlBase._CUSTOM_CONCEPT_IDS_START,
            omop_table=omop_table,
            concept_id_column=concept_id_column,
        )
        try:
            self._run_query(sql)
        except Exception:
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
            omop_database_catalog=self._omop_database_catalog,
            omop_database_schema=self._omop_database_schema,
            work_database_catalog=self._work_database_catalog,
            work_database_schema=self._work_database_schema,
            min_custom_concept_id=EtlBase._CUSTOM_CONCEPT_IDS_START,
            omop_table=omop_table,
            concept_id_column=concept_id_column,
        )
        try:
            self._run_query(sql)
        except Exception:
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
            omop_database_catalog=self._omop_database_catalog,
            omop_database_schema=self._omop_database_schema,
            work_database_catalog=self._work_database_catalog,
            work_database_schema=self._work_database_schema,
            min_custom_concept_id=EtlBase._CUSTOM_CONCEPT_IDS_START,
            omop_table=omop_table,
            concept_id_column=concept_id_column,
        )
        try:
            self._run_query(sql)
        except Exception:
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
            omop_database_catalog=self._omop_database_catalog,
            omop_database_schema=self._omop_database_schema,
            work_database_catalog=self._work_database_catalog,
            work_database_schema=self._work_database_schema,
            min_custom_concept_id=EtlBase._CUSTOM_CONCEPT_IDS_START,
            omop_table=omop_table,
            concept_id_column=concept_id_column,
        )

        self._lock_source_to_concept_map_cleanup.acquire()
        try:
            self._run_query(sql)
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
        logging.debug("Deleting work table %s", work_table)
        template = self._template_env.get_template("cleanup/drop.sql.jinja")
        sql = template.render(
            work_database_catalog=self._work_database_catalog,
            work_database_schema=self._work_database_schema,
            table_name=work_table,
        )
        self._run_query(sql)

    def _custom_db_engine_cleanup(self, table: str) -> None:
        """Custom cleanup method for specific database engine implementation

        Args:
            table (str): Table name (all for all tables)
        """
        pass
