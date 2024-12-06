# Copyright 2024 RADar-AZDelta
# SPDX-License-Identifier: gpl3+

import json
import os
import re
from pathlib import Path

import jpype
import jpype.imports


def render_sql(target_dialect: str, sql: str) -> str:
    # import the Java module
    from org.ohdsi.sql import (  # type: ignore # pylint: disable=import-outside-toplevel,import-error
        # SqlRender,
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

    # if len(parameters):
    #     sql = str(SqlRender.renderSql(sql, list(parameters.keys()), list(parameters.values())))

    sql = str(SqlTranslate.translateSqlWithPath(sql, target_dialect, None, None, path_to_replacement_patterns))
    return sql


def modify_bigquery_cdm_ddl(sql: str) -> str:
    # Solve some issues with the DDL
    sql = re.sub(
        r"@cdmDatabaseSchema",
        r"{{dataset_omop}}",
        sql,
    )
    sql = re.sub(
        r"(create table )({{dataset_omop}}).(.*).(\([\S\s.]+?\);)",
        r"DROP TABLE IF EXISTS `{{dataset_omop}}.\3`; \n\1`\2.\3` \4",
        sql,
    )
    # sql = re.sub(r".(?<!not )null", r"", sql)
    # sql = re.sub(r"\"", r"", sql)

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
            rf"(create table\s+`{{{{dataset_omop}}}}.{table}`)\s(\([\S\s]*?\))(\s*);",
            rf"\1 \2\ncluster by {', '.join(fields)};",
            sql,
            flags=re.DOTALL,
        )

    return sql


def modify_sqlserver_cdm_ddl(sql: str, ddl_part: str) -> str:
    # Solve some issues with the DDL
    sql = re.sub(
        r"@cdmDatabaseSchema",
        r"[{{omop_database_catalog}}].[{{omop_database_schema}}]",
        sql,
    )
    sql = re.sub(
        r"(CREATE TABLE \[{{omop_database_catalog}}\].\[{{omop_database_schema}}\]).(.*).(\([\S\s.]+?\);)",
        r"IF OBJECT_ID(N'[{{omop_database_catalog}}].[{{omop_database_schema}}].\2', N'U') IS NOT NULL\n\tDROP TABLE [{{omop_database_catalog}}].[{{omop_database_schema}}].\2; \n\1.\2 \3",
        sql,
    )
    # see https://github.com/OHDSI/Vocabulary-v5.0/issues/389#issuecomment-1977413290
    sql = sql.replace("concept_name varchar(255) NOT NULL,", "concept_name varchar(510) NOT NULL,")
    sql = sql.replace("concept_synonym_name varchar(1000) NOT NULL,", "concept_synonym_name varchar(1100) NOT NULL,")
    sql = sql.replace("vocabulary_name varchar(255) NOT NULL,", "vocabulary_name varchar(510) NOT NULL,")
    
    # deviation from OMOP CDM 5.4 change the length of source_value columns from 50 chars to 255 (see https://github.com/RADar-AZDelta/Rabbit-in-a-Blender/issues/71)
    sql = sql.replace("_source_value varchar(50)", "_source_value varchar(255)")

    # also change length of the source_code
    sql = sql.replace("source_code varchar(50) NOT NULL", "source_code varchar(255) NOT NULL")
    
    if ddl_part == "ddl":
        sql = (
            """
-- use database
USE [{{omop_database_catalog}}];

-- drop constraints
DECLARE @DropConstraints NVARCHAR(max) = ''
SELECT @DropConstraints += 'ALTER TABLE ' + QUOTENAME(OBJECT_SCHEMA_NAME(parent_object_id)) + '.'
                        +  QUOTENAME(OBJECT_NAME(parent_object_id)) + ' ' + 'DROP CONSTRAINT' + QUOTENAME(name)
FROM sys.foreign_keys
EXECUTE sp_executesql @DropConstraints;
"""
            + sql
        )

    return sql


def render_cdm_ddl_queries(db_dialect: str):
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
            / f"OMOPCDM_{db_dialect}_5.4_{ddl_part}.sql"
        )
        with open(sql_path, "r", encoding="utf-8") as file:
            sql = file.read()

        match db_dialect:
            case "sql_server":
                target_dialect = "sql server"
            case _:
                target_dialect = db_dialect

        rendered_sql = render_sql(target_dialect, sql)

        match db_dialect:
            case "bigquery":
                match ddl_part:
                    case "ddl":
                        rendered_sql = modify_bigquery_cdm_ddl(rendered_sql)
                    case "primary_keys" | "constraints" | "indices":
                        continue
            case "sql_server":
                rendered_sql = modify_sqlserver_cdm_ddl(rendered_sql, ddl_part)

        jinja_path = str(
            Path(__file__).parent.parent.resolve()
            / "src"
            / "riab"
            / "etl"
            / db_dialect
            / "templates"
            / "ddl"
            / f"OMOPCDM_{db_dialect}_5.4_{ddl_part}.sql.jinja"
        )
        with open(jinja_path, "w", encoding="utf-8") as file:
            file.write(rendered_sql)


def modify_bigquery_dqd_ddl(sql: str) -> str:
    sql = re.sub(
        r"@tableName",
        r"{{dataset_dqd}}",
        sql,
    )
    sql = re.sub(
        r"DROP TABLE IF EXISTS {{dataset_dqd}};",
        r"DROP TABLE IF EXISTS `{{dataset_dqd}}`;",
        sql,
    )
    sql = re.sub(
        r"create table {{dataset_dqd}}",
        r"create table `{{dataset_dqd}}`",
        sql,
    )
    return sql


def modify_sqlserver_dqd_ddl(sql: str) -> str:
    sql = re.sub(
        r"@tableName",
        r"{{dqd_database_catalog}}.{{dqd_database_schema}}",
        sql,
    )
    return sql


def render_dqd_ddl_queries(db_dialect: str):
    for ddl_part in ["concept", "field", "table"]:
        sql_path = str(
            Path(__file__).parent.parent.resolve()
            / "src"
            / "riab"
            / "libs"
            / "DataQualityDashboard"
            / "inst"
            / "sql"
            / "sql_server"
            / f"result_table_ddl_{ddl_part}.sql"
        )
        with open(sql_path, "r", encoding="utf-8") as file:
            sql = file.read()

        match db_dialect:
            case "sql_server":
                target_dialect = "sql server"
            case _:
                target_dialect = db_dialect

        rendered_sql = render_sql(target_dialect, sql)

        match db_dialect:
            case "bigquery":
                rendered_sql = modify_bigquery_dqd_ddl(rendered_sql)
            case "sql_server":
                rendered_sql = modify_sqlserver_dqd_ddl(rendered_sql)

        jinja_path = str(
            Path(__file__).parent.parent.resolve()
            / "src"
            / "riab"
            / "etl"
            / db_dialect
            / "templates"
            / "ddl"
            / f"result_table_ddl_{ddl_part}.sql.jinja"
        )
        with open(jinja_path, "w", encoding="utf-8") as file:
            file.write(rendered_sql)


def post_process_sqlrender_to_sqlserver_jinja(sql: str, sql_file: str):
    return sql


def post_process_sqlrender_to_bigquery_jinja(sql: str, sql_file: str):
    sql = re.sub(
        r"`datetime`",
        r"'datetime'",
        sql,
    )
    sql = re.sub(
        r"`date`",
        r"'date'",
        sql,
    )
    return sql


def convert_sqlrender_to_sqlserver_jinja(sql: str, sql_file: str):
    return sql


def convert_sqlrender_to_bigquery_jinja(sql: str, sql_file: str):
    # remove the comment block
    sql = re.sub(
        r"/\*\*\*\*\*\*\*\*\*([\S\s.]+?)\*\*\*\*\*\*\*\*\*\*/",
        r"",
        sql,
    )

    # replace the if else statement
    def replaceIfElse(match):
        else_statement = f"{{% else %}}{match.group(4)}" if match.group(4) else ""
        return f"{{% if {match.group(1)} %}}{match.group(2)}{else_statement}{{% endif %}}"

    sql = re.sub(r"{([\S\s.]+?)}\s??\?\s?{([\S\s.]+?)}(\s?:\s?{([\S\s.]+?)})?", replaceIfElse, sql)

    # replace the quoted parameters in the if statements
    sql = re.sub(
        r"'@([a-zA-Z]*)'(?=.*%})",
        r"\1",
        sql,
    )

    # replace the parameters in the if statements
    def replaceParametersWithinIf(match):
        if match.group(0).endswith("{% endif %}"):
            return match.group(0)
        else:
            return re.sub(
                r"@([a-zA-Z]*)",
                r"\1",
                match.group(0),
            )
            # return f"{match.group(1)}{match.group(2)}{match.group(3)}{match.group(4)}{match.group(5)}"

    sql = re.sub(
        r"({% if)([\S\s.]+?)@([a-zA-Z]*)([\S\s.]+?)(%})",
        replaceParametersWithinIf,
        sql,
    )

    # replace the & in the if statement
    sql = re.sub(
        r" & (?=.*%})",
        r" and ",
        sql,
    )
    # replace the | in the if statement
    sql = re.sub(
        r" \| (?=.*%})",
        r" or ",
        sql,
    )
    # replace () with [] for arrays in the if statement
    sql = re.sub(
        r"\(([a-zA-Z_'\,]*)\)(?=.*%})",
        r"[\1]",
        sql,
    )

    # lowercase the quoted strings in the if statement
    def lowerCaseQuotedStringsWithinIf(match):
        return re.sub(
            r"'([A-Z_]+)'",
            lambda m: f"'{m.group(1).lower()}'",
            match.group(0),
        )

    sql = re.sub(
        r"({% if)([\S\s.]+?)('[A-Z_]+')*([\S\s.]+?)(%})",
        lowerCaseQuotedStringsWithinIf,
        sql,
    )

    # replace the parameters
    sql = re.sub(
        r"@([a-zA-Z]*)",
        r"{{\1}}",
        sql,
    )

    return sql


def render_dqd_queries(db_dialect: str):
    sql_files = list(
        (
            Path(__file__).parent.parent.resolve()
            / "src"
            / "riab"
            / "libs"
            / "DataQualityDashboard"
            / "inst"
            / "sql"
            / "sql_server"
        ).glob("*.sql")
    )
    for sql_file in sql_files:
        render_dqd_query(sql_file, db_dialect)


def render_dqd_query(sql_file: Path, db_dialect: str):
    jinja_path = str(
        Path(__file__).parent.parent.resolve()
        / "src"
        / "riab"
        / "etl"
        / db_dialect
        / "templates"
        / "dqd"
        / f"{os.path.basename(sql_file)}.jinja"
    )

    with open(sql_file, "r", encoding="utf-8") as fr, open(jinja_path, "w", encoding="utf-8") as fw:
        sql = fr.read()
        match db_dialect:
            case "bigquery":
                jinja_sql = convert_sqlrender_to_bigquery_jinja(sql, os.path.basename(sql_file))
            case "sql_server":
                jinja_sql = convert_sqlrender_to_sqlserver_jinja(sql, os.path.basename(sql_file))
        rendered_sql = render_sql(db_dialect, jinja_sql)
        match db_dialect:
            case "bigquery":
                rendered_sql = post_process_sqlrender_to_bigquery_jinja(rendered_sql, os.path.basename(sql_file))
            case "sql_server":
                rendered_sql = post_process_sqlrender_to_bigquery_jinja(rendered_sql, os.path.basename(sql_file))
        fw.write(rendered_sql)


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

    # render_dqd_query(
    #     Path(__file__).parent.parent.resolve()
    #     / "src"
    #     / "riab"
    #     / "libs"
    #     / "DataQualityDashboard"
    #     / "inst"
    #     / "sql"
    #     / "sql_server"
    #     / "table_person_completeness.sql",
    #     "bigquery",
    # )

    for db_dialect in ["bigquery", "sql_server"]:
        # render_dqd_queries(db_dialect) #not yet stable, will need to convert SqlTranslate.translateSql JAVA method to Python
        render_cdm_ddl_queries(db_dialect)
        render_dqd_ddl_queries(db_dialect)
