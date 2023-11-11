# Copyright 2022 RADar-AZDelta
# SPDX-License-Identifier: gpl3+

from ..create_cdm_folders import CreateCdmFolders
from .bigquery_etl_base import BigQueryEtlBase


class BigQueryCreateCdmFolders(CreateCdmFolders, BigQueryEtlBase):
    def __init__(
        self,
        **kwargs,
    ):
        super().__init__(**kwargs)

    def _generate_sample_raw_query(self, omop_table: str) -> str:
        """Generates an example SQL query to query the raw data.

        Args:
            vocabulary_table (str): The standardised vocabulary table
        """
        template = self._template_env.get_template("cdm_folders/sample_source_query.sql.jinja")

        columns = self._get_omop_column_names(omop_table)
        sql = template.render(
            project_raw=self._project_raw,
            omop_table=omop_table,
            columns=columns,
        )
        return sql
