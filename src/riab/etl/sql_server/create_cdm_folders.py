# Copyright 2024 RADar-AZDelta
# SPDX-License-Identifier: gpl3+

import polars as pl

from ..create_cdm_folders import CreateCdmFolders
from .etl_base import SqlServerEtlBase


class SqlServerCreateCdmFolders(CreateCdmFolders, SqlServerEtlBase):
    def __init__(
        self,
        **kwargs,
    ):
        super().__init__(**kwargs)

    def _generate_sample_etl_query(self, omop_table: str, omop_fields: pl.DataFrame) -> str:
        """Generates an example SQL query to query the raw data.

        Args:
            omop_table (str): The OMOP table
        """
        template = self._template_env.get_template("cdm_folders/sample_etl_query.sql.jinja")

        columns = omop_fields.rows(named=True)

        sql = template.render(
            project_raw="{{project_raw}}",  # self._project_raw,
            omop_table="omop_table",
            columns=columns,
        )
        return sql

    def _generate_sample_usagi_query(self, omop_table: str, concept_column: dict[str, str]) -> str:
        """Generates an example SQL query to generate the Usagi source CSV.

        Args:
            omop_table (str): The OMOP table
            concept_column 'str): The concept column
        """
        template = self._template_env.get_template("cdm_folders/sample_usagi_query.sql.jinja")

        sql = template.render(
            project_raw="{{project_raw}}",  # self._project_raw,
            omop_table="omop_table",
            concept_column=concept_column,
        )
        return sql
