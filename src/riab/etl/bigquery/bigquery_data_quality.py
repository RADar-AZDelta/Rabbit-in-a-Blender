# Copyright 2022 RADar-AZDelta
# SPDX-License-Identifier: gpl3+

import logging
import traceback
from pathlib import Path
from tempfile import NamedTemporaryFile, TemporaryDirectory
from time import time
from typing import Any, List

import google.cloud.bigquery as bq
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from ..data_quality import DataQuality
from .bigquery_etl_base import BigQueryEtlBase


class BigQueryDataQuality(DataQuality, BigQueryEtlBase):
    def __init__(
        self,
        **kwargs,
    ):
        super().__init__(target_dialect="bigquery", **kwargs)

    def _run_check_query(self, check: Any, row: str, item: Any) -> Any:
        sql = None
        result = None
        exception: str | None = None
        execution_time = -1
        try:
            parameters = item.to_dict()

            parameters["cohort"] = (
                "TRUE" if hasattr(parameters, "cohortDefinitionId") else "FALSE"
            )
            parameters[
                "cdmDatabaseSchema"
            ] = f"{self._project_id}.{self._dataset_id_omop}"
            parameters[
                "vocabDatabaseSchema"
            ] = f"{self._project_id}.{self._dataset_id_omop}"

            sql = self._render_sqlfile(
                check.sqlFile,
                list(parameters.keys()),
                list(parameters.values()),
            )

            start = time()
            rows = self._gcp.run_query_job(sql)
            end = time()
            execution_time = end - start
            result = dict(next(rows))
        except Exception as ex:
            logging.warn(traceback.format_exc())
            exception = str(ex)
            # if __debug__:
            #     breakpoint()

        return self._process_check(
            check, row, item, sql, result, execution_time, exception
        )

    def _get_cdm_sources(self) -> List[Any]:
        """Merges the uploaded custom concepts in the OMOP concept table.

        Returns:
            RowIterator | _EmptyRowIterator: The result rows
        """
        template = self._template_env.from_string(
            "select * from {{project_id}}.{{dataset_id_omop}}.cdm_source;"
        )
        sql = template.render(
            project_id=self._project_id,
            dataset_id_omop=self._dataset_id_omop,
        )
        rows = self._gcp.run_query_job(sql)
        return [dict(row.items()) for row in rows]

    def _store_dqd_run(self, dqd_run: dict):
        table = pa.Table.from_pylist([dqd_run])
        with TemporaryDirectory(prefix="dqd_run_") as tmp_dir:
            logging.debug("Writing DQD run to parquet")
            tmp_file = str(Path(tmp_dir) / "dqdashboard_runs.parquet")
            pq.write_table(table, where=tmp_file)

            logging.debug("Loading DQD run parquet into BigQuery table")
            # upload the Parquet file to the Cloud Storage Bucket
            uri = self._gcp.upload_file_to_bucket(tmp_file, self._bucket_uri)
            # load the uploaded Parquet file from the bucket into the specific standardised vocabulary table
            self._gcp.batch_load_from_bucket_into_bigquery_table(
                uri,
                self._project_id,
                self._dataset_id_omop,
                "dqdashboard_runs",
                write_disposition=bq.WriteDisposition.WRITE_APPEND,
            )

    def _store_dqd_result(self, dqd_result: pd.DataFrame):
        table = pa.Table.from_pandas(dqd_result)
        with TemporaryDirectory(prefix="dqd_results_") as tmp_dir:
            logging.debug("Writing DQD results to parquet")
            tmp_file = str(Path(tmp_dir) / "dqdashboard_results.parquet")
            pq.write_table(table, where=tmp_file)

            logging.debug("Loading DQD results parquet into BigQuery table")
            # upload the Parquet file to the Cloud Storage Bucket
            uri = self._gcp.upload_file_to_bucket(tmp_file, self._bucket_uri)
            # load the uploaded Parquet file from the bucket into the specific standardised vocabulary table
            self._gcp.batch_load_from_bucket_into_bigquery_table(
                uri,
                self._project_id,
                self._dataset_id_omop,
                "dqdashboard_results",
                write_disposition=bq.WriteDisposition.WRITE_APPEND,
            )
