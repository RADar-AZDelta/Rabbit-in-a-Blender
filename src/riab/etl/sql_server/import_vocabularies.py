# Copyright 2024 RADar-AZDelta
# SPDX-License-Identifier: gpl3+

import logging
from pathlib import Path

from ..import_vocabularies import ImportVocabularies
from .etl_base import SqlServerEtlBase


class SqlServerImportVocabularies(ImportVocabularies, SqlServerEtlBase):
    """
    Class that imports the downloaded vocabulary zip from the Athena website.
    """

    def __init__(
        self,
        **kwargs,
    ):
        super().__init__(**kwargs)

    def _pre_load(self):
        """Stuff to do before the load (ex remove constraints from omop tables)"""
        if not self._disable_fk_constraints:
            self._remove_all_constraints()

    def _post_load(self):
        """Stuff to do after the load (ex re-add constraints to omop tables)"""
        if not self._disable_fk_constraints:
            self._add_all_constraints()

    def _clear_vocabulary_upload_table(self, vocabulary_table: str) -> None:
        """Removes a specific standardised vocabulary table

        Args:
            vocabulary_table (str): The standardised vocabulary table
        """
        logging.debug("Truncate vocabulary table %s", vocabulary_table)
        template = self._template_env.get_template("vocabulary/vocabulary_table_truncate.sql.jinja")
        sql = template.render(
            omop_database_catalog=self._omop_database_catalog,
            omop_database_schema=self._omop_database_schema,
            vocabulary_table=vocabulary_table,
        )
        self._db.run_query(sql)

    def _load_vocabulary_parquet_in_upload_table(self, vocabulary_table: str, parquet_file: Path) -> None:
        """Loads the CSV file in the specific standardised vocabulary table

        Args:
            vocabulary_table (str): The standardised vocabulary table
            parquet_file (Path): Path to the CSV file
        """
        self._upload_parquet(self._omop_database_catalog, self._omop_database_schema, vocabulary_table, parquet_file)

    def _refill_vocabulary_table(self, vocabulary_table: str) -> None:
        """Recreates a specific standardised vocabulary table from the upload table

        Args:
            vocabulary_table (str): The standardised vocabulary table
        """
        pass
