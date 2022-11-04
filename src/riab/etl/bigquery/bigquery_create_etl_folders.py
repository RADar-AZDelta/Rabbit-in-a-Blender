# Copyright 2022 RADar-AZDelta
# SPDX-License-Identifier: gpl3+

from ..create_etl_folders import CreateEtlFolders
from .bigquery_etl_base import BigQueryEtlBase


class BigQueryCreateEtlFolders(CreateEtlFolders, BigQueryEtlBase):
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
        template = self._template_env.get_template("sample_source_query.sql.jinja")

        columns = self._get_column_names(omop_table)
        sql = template.render(
            project_id=self._project_id,
            dataset_id_raw=self._dataset_id_raw,
            omop_table=omop_table,
            columns=columns,
        )
        return sql
