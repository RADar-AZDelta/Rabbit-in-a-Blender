# Copyright 2024 RADar-AZDelta
# SPDX-License-Identifier: gpl3+

import json
import re
from pathlib import Path

import jpype
import jpype.imports


def render_sql(target_dialect: str, sql: str, parameters: dict) -> str:
    # import the Java module
    from org.ohdsi.sql import (  # type: ignore # pylint: disable=import-outside-toplevel,import-error
        SqlRender,
        SqlTranslate,
    )

    path_to_replacement_patterns = str(
        Path(__file__).parent.parent.resolve()
        / "src"
        / "riab"
        / "libs"
        / "SqlRender"
        / "inst"
        / "csv"
        / "replacementPatterns.csv"
    )

    sql = str(SqlRender.renderSql(sql, list(parameters.keys()), list(parameters.values())))

    sql = str(SqlTranslate.translateSqlWithPath(sql, target_dialect, None, None, path_to_replacement_patterns))
    return sql


def modify_bigquery_ddl(sql: str) -> str:
    # Solve some issues with the DDL
    sql = re.sub(
        r"create table",
        r"create table if not exists",
        sql,
    )
    sql = re.sub(
        r"@cdmDatabaseSchema",
        r"{{dataset_omop}}",
        sql,
    )
    sql = re.sub(r".(?<!not )null", r"", sql)
    sql = re.sub(r"\"", r"", sql)

    # add clustered indexes
    with open(
        str(
            Path(__file__).parent.parent.resolve()
            / "src"
            / "riab"
            / "etl"
            / "bigquery"
            / "templates"
            / "ddl"
            / "OMOPCDM_bigquery_5.4_clustering_fields.json"
        ),
        "r",
        encoding="UTF8",
    ) as file:
        clustering_fields = json.load(file)
    for table, fields in clustering_fields.items():
        sql = re.sub(
            rf"(create table if not exists\s+{{{{dataset_omop}}}}.{table})\s\((.*?\))(\s*);",
            rf"\1 (\2 cluster by {', '.join(fields)};",
            sql,
            flags=re.DOTALL,
        )

    return sql


def render_ddl_queries(db_dialect: str):
    for ddl_part in ["ddl", "primary_keys", "constraints", "indices"]:
        sql_path = str(
            Path(__file__).parent.parent.resolve()
            / "src"
            / "riab"
            / "libs"
            / "CommonDataModel"
            / "inst"
            / "ddl"
            / "5.4"
            / db_dialect
            / f"OMOPCDM_bigquery_5.4_{ddl_part}.sql"
        )
        with open(sql_path, "r", encoding="utf-8") as file:
            sql = file.read()
        rendered_sql = render_sql(db_dialect, sql, {})

        match db_dialect:
            case "bigquery":
                match ddl_part:
                    case "ddl":
                        rendered_sql = modify_bigquery_ddl(rendered_sql)
                    case "primary_keys" | "constraints" | "indices":
                        continue
            case _:
                raise ValueError(f"Unknown db_dialect: {db_dialect}")

        jinja_path = str(
            Path(__file__).parent.parent.resolve()
            / "src"
            / "riab"
            / "etl"
            / db_dialect
            / "templates"
            / "ddl"
            / f"OMOPCDM_bigquery_5.4_{ddl_part}.sql.jinja"
        )
        with open(jinja_path, "w", encoding="utf-8") as file:
            file.write(rendered_sql)


if __name__ == "__main__":
    # launch the JVM
    sqlrender_path = str(
        Path(__file__).parent.parent.resolve()
        / "src"
        / "riab"
        / "libs"
        / "SqlRender"
        / "inst"
        / "java"
        / "SqlRender.jar"
    )
    jpype.startJVM(classpath=[sqlrender_path])  # type: ignore

    for db_dialect in ["bigquery"]:
        render_ddl_queries(db_dialect)
