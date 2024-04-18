# Copyright 2024 RADar-AZDelta
# SPDX-License-Identifier: gpl3+

import logging
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from time import time
from typing import Any, Tuple, cast

import polars as pl
from humanfriendly import format_timespan

from .etl_base import EtlBase
from .sql_render_base import SqlRenderBase


class Achilles(SqlRenderBase, EtlBase, ABC):
    """
    Class that runs the descriptive statistics
    """

    def __init__(
        self,
        achilles_version: str = "v1.7.2",
        default_analyses_only: bool = True,
        small_cell_count: int = 5,
        source_name: str | None = None,
        analysis_ids: list[int] | None = None,
        temp_achilles_prefix: str = "tmpach",
        optimize_atlas_cache: bool = False,
        exclude_analysis_ids: list[int] | None = None,
        update_given_analyses_only: bool = False,
        create_table: bool = True,
        drop_scratch_tables=True,
        output_folder: str = "output",
        **kwargs,
    ):
        super().__init__(**kwargs)

        self._achilles_version = achilles_version
        self._default_analyses_only = default_analyses_only
        self._temp_achilles_prefix = temp_achilles_prefix
        self._small_cell_count = small_cell_count
        self._source_name = source_name
        self._analysis_ids = analysis_ids
        self._optimize_atlas_cache = optimize_atlas_cache
        self._exclude_analysis_ids = exclude_analysis_ids
        self._update_given_analyses_only = update_given_analyses_only
        self._create_table = create_table
        self._output_folder = output_folder
        self._drop_scratch_tables = drop_scratch_tables

    def run(
        self,
    ):
        logging.info("Generating descriptive statistics")

        start = time()
        achilles_sql = []

        # Get source name if none provided
        # ----------------------------------------------------------------------------------------------
        if not self._source_name:
            self._source_name = self._get_source_name()

        # Obtain analyses to run
        # -----------------------------------------------------
        df_analysis_details = self._get_analysis_details()
        if self._analysis_ids:
            # If specific analysis_ids are given, run only those
            df_analysis_details = df_analysis_details.filter(
                pl.col("ANALYSIS_ID").apply(lambda s: s in (self._analysis_ids or []))
            )
        elif self._default_analyses_only:
            # If specific analyses are not given, determine whether or not to run only default analyses
            df_analysis_details = df_analysis_details.filter(pl.col("IS_DEFAULT") == 1)

        # Remove unwanted analyses that have not already been excluded, if any are specified
        if self._exclude_analysis_ids:
            df_analysis_details = df_analysis_details.filter(
                pl.col("ANALYSIS_ID").apply(lambda s: s not in (self._exclude_analysis_ids or []))
            )

        results_tables = [
            {
                "detail_type": "results",
                "table_prefix": self._temp_achilles_prefix,
                "schema": pl.read_csv(
                    str(
                        Path(__file__).parent.parent.resolve()
                        / "libs"
                        / "Achilles"
                        / "inst"
                        / "csv"
                        / "schemas"
                        / "schema_achilles_results.csv"
                    ),
                ),
                "analysis_ids": list(df_analysis_details.filter(pl.col("DISTRIBUTION") <= 0)["ANALYSIS_ID"]),
            },
            {
                "detail_type": "results_dist",
                "table_prefix": f"{self._temp_achilles_prefix}_dist",
                "schema": pl.read_csv(
                    str(
                        Path(__file__).parent.parent.resolve()
                        / "libs"
                        / "Achilles"
                        / "inst"
                        / "csv"
                        / "schemas"
                        / "schema_achilles_results_dist.csv"
                    ),
                ),
                "analysis_ids": list(df_analysis_details.filter(pl.col("DISTRIBUTION") == 1)["ANALYSIS_ID"]),
            },
        ]

        # Determine whether or not to create Achilles support tables
        # -----------------------------------------------------
        preserve_results = False
        if not self._create_table and not self._analysis_ids:
            self._create_table = True
            preserve_results = False
        elif not self._create_table and self._analysis_ids and not self._update_given_analyses_only:
            self._create_table = True
            preserve_results = False
        elif not self._create_table and self._analysis_ids and self._update_given_analyses_only:
            preserve_results = True

        # If not creating support tables, then either remove ALL prior results or only those results for
        if not self._create_table and not preserve_results:
            self._delete_existing_results(df_analysis_details)
        elif not self._create_table and preserve_results:
            self._delete_given_analysis(self._analysis_ids)

        # Create and populate the achilles_analysis table (assumes inst/csv/achilles_analysis_details.csv exists)
        if self._create_table:
            self._create_and_populate_analysis_table(df_analysis_details)

        # Clean up existing scratch tables -----------------------------------------------
        if not self._supports_temp_tables:
            self._drop_all_scratch_tables(results_tables)

        # Generate Main Analyses
        # -------------------------------------------------------------------------------------------
        main_analysis_ids = list(df_analysis_details["ANALYSIS_ID"])

        main_sqls = []
        with ThreadPoolExecutor(max_workers=self._max_worker_threads_per_table) as executor:
            futures = [executor.submit(self._get_analysis_sql, id) for id in main_analysis_ids]
            for result in as_completed(futures):
                analysis_id, sql = result.result()
                main_sqls.append({"analysis_id": analysis_id, "sql": sql})

        achilles_sql.extend([main_sql["sql"] for main_sql in main_sqls])

        benchmark = []
        with ThreadPoolExecutor(max_workers=self._max_worker_threads_per_table) as executor:
            futures = [
                executor.submit(self._execute_analysis, main_sql["sql"], main_sql["analysis_id"])
                for main_sql in main_sqls
            ]
            for result in as_completed(futures):
                (
                    analysis_id,
                    _,
                    execution_time,
                ) = result.result()
                benchmark.append((analysis_id, execution_time))

        df_benchmark = pl.DataFrame(benchmark, schema=["ANALYSIS_ID", "RUN_TIME"])
        failed_analysis_ids = list(df_benchmark.filter(pl.col("RUN_TIME") == -1)["ANALYSIS_ID"])

        for table in results_tables:
            for analysis_id in failed_analysis_ids:
                if analysis_id in table["analysis_ids"]:
                    table["analysis_ids"].remove(analysis_id)

        # Merge scratch tables into final analysis tables
        # -------------------------------------------------------------------------------------------
        results_tables_to_merge = [
            table for table in results_tables if len(set(table["analysis_ids"]) & set(df_benchmark["ANALYSIS_ID"]))
        ]

        merge_sqls = [
            self._merge_achilles_scratch_tables(
                results_table,
                df_benchmark,  # , main_analysis_ids
            )
            for results_table in results_tables_to_merge
        ]

        achilles_sql.extend(merge_sqls)

        with ThreadPoolExecutor(max_workers=self._max_worker_threads_per_table) as executor:
            futures = [executor.submit(self._run_query, sql) for sql in merge_sqls]
            for result in as_completed(futures):
                result.result()

        # Clean up scratch tables -----------------------------------------------
        if not self._supports_temp_tables and self._drop_scratch_tables:
            self._drop_all_scratch_tables(results_tables)

        # Create indices -----------------------------------------------------------------
        if self._create_indices:
            indices_sql = self._create_indices_sqls()
        else:
            indices_sql = [f"/* INDEX CREATION SKIPPED, INDICES NOT SUPPORTED IN {self._db_engine} */"]

        achilles_sql.extend(indices_sql)

        # Optimize Atlas Cache -----------------------------------------------------------
        if self._optimize_atlas_cache:
            optimize_atlas_cache_sql = self._get_optimize_atlas_cache_sql()
            achilles_sql.extend([optimize_atlas_cache_sql])

        # with open("achilles.sql", "w", encoding="UTF8") as file:
        #     file.write("\n\n".join(achilles_sql))

        end = time()
        execution_time = end - start

        if len(failed_analysis_ids):
            logging.warning("Failed analysis %s", ",".join([str(id) for id in failed_analysis_ids]))
        logging.info("Achilles took %s to run", format_timespan(execution_time))

    def _get_analysis_details(self):
        df_analysis_details = pl.read_csv(
            str(
                Path(__file__).parent.parent.resolve()
                / "libs"
                / "Achilles"
                / "inst"
                / "csv"
                / "achilles"
                / "achilles_analysis_details.csv"
            ),
        )
        return df_analysis_details

    def _delete_given_analysis(self, analysis_ids: list[int] | None):
        parameters = {
            "resultsDatabaseSchema": self._results_database_schema,
            "analysisIds": ",".join([str(id) for id in (analysis_ids or [])]),
        }
        sql = self._render_sql(
            self._db_engine,
            "delete from @resultsDatabaseSchema.achilles_results where analysis_id in (@analysisIds);",
            parameters,
        )
        self._run_query(sql)

        sql = self._render_sql(
            self._db_engine,
            "delete from @resultsDatabaseSchema.achilles_results_dist where analysis_id in (@analysisIds);",
            parameters,
        )
        self._run_query(sql)

    def _delete_existing_results(self, df_analysis_details: pl.DataFrame):
        result_ids = list(df_analysis_details.filter(pl.col("DISTRIBUTION") == 0)["ANALYSIS_ID"])
        dist_ids = list(df_analysis_details.filter(pl.col("DISTRIBUTION") == 1)["ANALYSIS_ID"])

        if result_ids:
            parameters = {
                "resultsDatabaseSchema": self._results_database_schema,
                "analysisIds": ",".join(result_ids),
            }
            sql = self._render_sql(
                self._db_engine,
                "delete from @resultsDatabaseSchema.achilles_results where analysis_id in (@analysisIds);",
                parameters,
            )
            self._run_query(sql)

        if dist_ids:
            parameters = {
                "resultsDatabaseSchema": self._results_database_schema,
                "analysisIds": ",".join(dist_ids),
            }
            sql = self._render_sql(
                self._db_engine,
                "delete from @resultsDatabaseSchema.achilles_results where analysis_id in (@analysisIds);",
                parameters,
            )
            self._run_query(sql)

    def _create_and_populate_analysis_table(self, df_analysis_details: pl.DataFrame):
        logging.info("Create and populate the achilles_analysis table")
        with open(
            Path(__file__).parent.parent.resolve()
            / "libs"
            / "Achilles"
            / "inst"
            / "sql"
            / "sql_server"
            / "analyses"
            / "achilles_analysis_ddl.sql",
            "r",
            encoding="utf-8",
        ) as file:
            sql = file.read()

        parameters = {"resultsDatabaseSchema": self._results_database_schema}

        sql = self._render_sql(self._db_engine, sql, parameters)
        self._run_query(sql)

        data_frame = df_analysis_details.drop(["DISTRIBUTION", "DISTRIBUTED_FIELD"])
        self._store_analysis_details(data_frame)

    @abstractmethod
    def _store_analysis_details(self, data_frame: pl.DataFrame):
        pass

    @abstractmethod
    def _run_query(self, sql) -> Tuple[pl.DataFrame, float]:
        pass

    @property
    @abstractmethod
    def _temp_emulation_schema(self) -> str:
        pass

    @property
    @abstractmethod
    def _voc_database_schema(self) -> str:
        pass

    @property
    @abstractmethod
    def _cdm_database_schema(self) -> str:
        pass

    @property
    @abstractmethod
    def _scratch_database_schema(self) -> str:
        pass

    @property
    @abstractmethod
    def _results_database_schema(self) -> str:
        pass

    @property
    @abstractmethod
    def _supports_temp_tables(self) -> bool:
        pass

    @property
    @abstractmethod
    def _schema_delim(self) -> str:
        pass

    def _pre_prep_analysis_query(self, sql: str) -> str:
        return sql

    def _post_prep_analysis_query(self, sql: str) -> str:
        return sql

    def _drop_all_scratch_tables(self, results_tables: list[Any], table_types: list[str] = ["achilles"]):
        if "achilles" not in table_types:
            return

        df_analysis_details = self._get_analysis_details()

        if self._default_analyses_only:
            df_results_tables = df_analysis_details.filter(
                (pl.col("DISTRIBUTION") <= 0) & (pl.col("IS_DEFAULT") == 1)
            ).with_columns(pl.col("ANALYSIS_ID").apply(lambda id: f"{self._temp_achilles_prefix}_{id}").alias("TABLE"))
        else:
            df_results_tables = df_analysis_details.filter((pl.col("DISTRIBUTION") <= 0)).with_columns(
                pl.col("ANALYSIS_ID").apply(lambda id: f"{self._temp_achilles_prefix}_{id}").alias("TABLE")
            )

        df_results_dist_tables = df_analysis_details.filter((pl.col("DISTRIBUTION") == 1)).with_columns(
            pl.col("ANALYSIS_ID").apply(lambda id: f"{self._temp_achilles_prefix}_dist_{id}").alias("TABLE")
        )

        tables = list(df_results_tables["TABLE"]) + list(df_results_dist_tables["TABLE"])
        tables.append("tmpach_dist_0")  # for some reason we are missing tmpach_dist_0

        with ThreadPoolExecutor(max_workers=self._max_worker_threads_per_table) as executor:
            futures = [
                executor.submit(
                    self._drop_scratch_table,
                    table,
                )
                for table in tables
            ]
            for result in as_completed(futures):
                result.result()

    def _drop_scratch_table(self, table: str):
        logging.info("Dropping table %s", table)
        sql = "IF OBJECT_ID('@scratchDatabaseSchema@schemaDelim@scratchTable', 'U') IS NOT NULL DROP TABLE @scratchDatabaseSchema@schemaDelim@scratchTable;"  # noqa: E501 # pylint: disable=line-too-long
        parameters = {
            "scratchDatabaseSchema": self._scratch_database_schema,
            "schemaDelim": self._schema_delim,
            "scratchTable": table,
        }
        rendered_sql = self._render_sql(self._db_engine, sql, parameters)
        self._run_query(rendered_sql)

    def _get_source_name(self) -> str:
        sql = "select cdm_source_name from @cdmDatabaseSchema.cdm_source"
        parameters = {
            "cdmDatabaseSchema": self._cdm_database_schema,
        }
        rendered_sql = self._render_sql(self._db_engine, sql, parameters)
        df_result, execution_time = self._run_query(rendered_sql)
        if execution_time == -1 or df_result.is_empty():
            raise Exception("Could not retrieve cdm_source_name from CDM database!")
        return next(iter(cast(pl.DataFrame, df_result).select("cdm_source_name")))[0]

    def _get_analysis_sql(self, analysis_id: int) -> Tuple[int, str]:
        with open(
            Path(__file__).parent.parent.resolve()
            / "libs"
            / "Achilles"
            / "inst"
            / "sql"
            / "sql_server"
            / "analyses"
            / f"{analysis_id}.sql",
            "r",
            encoding="utf-8",
        ) as file:
            sql = file.read()

        sql = self._pre_prep_analysis_query(sql)

        parameters = {
            "scratchDatabaseSchema": self._scratch_database_schema,
            "cdmDatabaseSchema": self._cdm_database_schema,
            "resultsDatabaseSchema": self._results_database_schema,
            "schemaDelim": self._schema_delim,
            "tempAchillesPrefix": self._temp_achilles_prefix,
            "tempEmulationSchema": self._temp_emulation_schema,
            "source_name": self._source_name,
            "achilles_version": self._achilles_version.lstrip("v"),
            "cdmVersion": self._omop_cdm_version.lstrip("v"),
        }
        rendered_sql = self._render_sql(self._db_engine, sql, parameters)
        rendered_sql = self._post_prep_analysis_query(rendered_sql)
        return analysis_id, rendered_sql

    def _execute_analysis(self, sql: str, analysis_id: int) -> Tuple[int, pl.DataFrame, float]:
        data_frame, execution_time = self._run_query(sql)
        return analysis_id, data_frame, execution_time

    def _render_casted_names(self, row):
        sql = "cast(@fieldName as @fieldType) as @fieldName"
        parameters = {"fieldName": row[0], "fieldType": row[1]}
        rendered_sql = self._render_sql(self._db_engine, sql, parameters)
        return rendered_sql

    def _get_benchmark_offset(self):
        return 2e06

    def _render_benchmark_selects(self, analysis_id, field_name, run_time):
        match field_name:
            case "analysis_id":
                return f"{self._get_benchmark_offset() + analysis_id} as analysis_id"
            case "stratum_1":
                return f"'{run_time}' as stratum_1"
            case "count_value":
                return f"{self._small_cell_count + 1} as count_value"
            case _:
                return f"NULL as {field_name}"

    def _render_analysis_sql(
        self,
        analysis_id: int,
        casted_names: str,
        run_time: float,
        results_table: Any,
    ):
        parameters = {
            "scratchDatabaseSchema": self._scratch_database_schema,
            "schemaDelim": self._schema_delim,
            "castedNames": casted_names,
            "tablePrefix": results_table["table_prefix"],
            "analysisId": str(analysis_id),
        }
        analysis_sql = self._render_sql(
            self._db_engine,
            "select @castedNames from @scratchDatabaseSchema@schemaDelim@tablePrefix_@analysisId",
            parameters,
        )

        df_benchmark_selects = results_table["schema"][["FIELD_NAME"]].apply(
            lambda x: self._render_benchmark_selects(analysis_id, x[0].lower(), run_time)
        )
        benchmark_selects = ", ".join(iter(df_benchmark_selects["map"]))

        parameters = {"benchmarkSelect": benchmark_selects}
        benchmark_sql = self._render_sql(self._db_engine, "select @benchmarkSelect", parameters)

        analysis_sql = " union all ".join([analysis_sql, benchmark_sql])
        return analysis_sql

    def _merge_achilles_scratch_tables(
        self,
        results_table: Any,
        df_benchmark: pl.DataFrame,
        # analysis_ids: list[int],
    ):
        logging.info("Merging achilles scratch tables")

        df_casted_names = results_table["schema"][["FIELD_NAME", "FIELD_TYPE"]].apply(self._render_casted_names)
        casted_names = ", ".join(iter(df_casted_names["map"]))

        detail_sqls = []
        with ThreadPoolExecutor(max_workers=self._max_worker_threads_per_table) as executor:
            futures = []
            for analysis_id in results_table["analysis_ids"]:
                benchmark = df_benchmark.filter((pl.col("ANALYSIS_ID") == analysis_id))
                run_time = -1 if benchmark.is_empty() else benchmark["RUN_TIME"][0]
                futures.append(
                    executor.submit(
                        self._render_analysis_sql,
                        analysis_id,
                        casted_names,
                        run_time,
                        results_table,
                    )
                )
            for result in as_completed(futures):
                analysis_sql = result.result()
                detail_sqls.append(analysis_sql)

        with open(
            Path(__file__).parent.parent.resolve()
            / "libs"
            / "Achilles"
            / "inst"
            / "sql"
            / "sql_server"
            / "analyses"
            / "merge_achilles_tables.sql",
            "r",
            encoding="utf-8",
        ) as file:
            sql = file.read()

        parameters = {
            "createTable": "TRUE",
            "resultsDatabaseSchema": self._results_database_schema,
            "tempEmulationSchema": self._temp_emulation_schema,
            "detailType": results_table["detail_type"],
            "detailSqls": " \nunion all\n ".join(detail_sqls),
            "fieldNames": ", ".join(iter(results_table["schema"][["FIELD_NAME"][0]])),
            "smallCellCount": str(self._small_cell_count),
        }
        rendered_sql = self._render_sql(self._db_engine, sql, parameters)
        return rendered_sql

    @property
    def _create_indices(self) -> bool:
        return True

    @property
    def _drop_index_sql(self) -> str:
        return "drop index @resultsDatabaseSchema.@indexName;"

    @property
    def _create_index_sql(self) -> str:
        return "create index @indexName on @resultsDatabaseSchema.@tableName (@fields);"

    def _create_indices_sqls(self) -> list[str]:
        achilles_tables = ["achilles_results", "achilles_results_dist"]
        df_indices_details = pl.read_csv(
            str(
                Path(__file__).parent.parent.resolve()
                / "libs"
                / "Achilles"
                / "inst"
                / "csv"
                / "post_processing"
                / "indices.csv"
            ),
        )
        drop_indices_sql = [
            self._render_sql(
                self._db_engine,
                self._drop_index_sql,
                {
                    "resultsDatabaseSchema": self._results_database_schema,
                    "indexName": index["INDEX_NAME"],
                },
            )
            for index in df_indices_details.iter_rows(named=True)
            if index["TABLE_NAME"] in achilles_tables
        ]

        with ThreadPoolExecutor(max_workers=self._max_worker_threads_per_table) as executor:
            futures = [executor.submit(self._run_query, sql) for sql in drop_indices_sql]
            for result in as_completed(futures):
                result.result()

        indices_sql = [
            self._render_sql(
                self._db_engine,
                self._create_index_sql,
                {
                    "resultsDatabaseSchema": self._results_database_schema,
                    "tableName": index["TABLE_NAME"],
                    "indexName": index["INDEX_NAME"],
                    "fields": ",".join(index["FIELDS"].split("~")),
                },
            )
            for index in df_indices_details.iter_rows(named=True)
            if index["TABLE_NAME"] in achilles_tables
        ]

        with ThreadPoolExecutor(max_workers=self._max_worker_threads_per_table) as executor:
            futures = [executor.submit(self._run_query, sql) for sql in indices_sql]
            for result in as_completed(futures):
                result.result()

        return drop_indices_sql + indices_sql

    def _get_optimize_atlas_cache_sql(self) -> str:
        df_results_concept_count_table = pl.read_csv(
            str(
                Path(__file__).parent.parent.resolve()
                / "libs"
                / "Achilles"
                / "inst"
                / "csv"
                / "schemas"
                / "schema_achilles_results_concept_count.csv"
            ),
        )
        with open(
            Path(__file__).parent.parent.resolve()
            / "libs"
            / "Achilles"
            / "inst"
            / "sql"
            / "sql_server"
            / "analyses"
            / "create_result_concept_table.sql",
            "r",
            encoding="utf-8",
        ) as file:
            sql = file.read()
        parameters = {
            "resultsDatabaseSchema": self._results_database_schema,
            "vocabDatabaseSchema": self._voc_database_schema,
            "fieldNames": ", ".join(iter(df_results_concept_count_table["FIELD_NAME"])),
        }
        rendered_sql = self._render_sql(self._db_engine, sql, parameters)
        self._run_query(rendered_sql)
        return rendered_sql
