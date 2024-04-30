# Copyright 2024 RADar-AZDelta
# SPDX-License-Identifier: gpl3+

import logging
from abc import ABC
from pathlib import Path
from threading import Lock

import jpype
import jpype.imports


class SqlRenderBase(ABC):
    """
    Base class for the Java SQLRender
    """

    def __init__(
        self,
        **kwargs,
    ):
        super().__init__(**kwargs)

        self.path_to_replacement_patterns = str(
            Path(__file__).parent.parent.resolve() / "libs" / "SqlRender" / "inst" / "csv" / "replacementPatterns.csv"
        )

        # launch the JVM
        sqlrender_path = str(
            Path(__file__).parent.parent.resolve() / "libs" / "SqlRender" / "inst" / "java" / "SqlRender.jar"
        )
        logging.debug("Starting JVM")
        jpype.startJVM(classpath=[sqlrender_path])  # type: ignore
        self._lock_translate_sql = Lock()

    def _render_sql(self, target_dialect: str, sql: str, parameters: dict) -> str:
        """_summary_

        Args:
            sql (str): Original SQL
            parameters (list[str]): Query parameter names
            values (list[str]): Query parameter

        Returns:
            str: The rendered SQL
        """
        match target_dialect:
            case "sql_server":
                target_dialect = "sql server"

        # import the Java module
        from org.ohdsi.sql import (  # type: ignore # pylint: disable=import-outside-toplevel,import-error
            SqlRender,
            SqlTranslate,
        )

        # SqlTranslate.setReplacementPatterns(self.path_to_replacement_patterns)
        try:
            rendered_sql = str(
                SqlRender.renderSql(
                    jpype.JString(sql),
                    jpype.JArray(jpype.JString, 1)([jpype.JString(str(s)) for s in parameters.keys()]),
                    jpype.JArray(jpype.JString, 1)([jpype.JString(str(s)) for s in parameters.values()]),
                )
            )
        except Exception as e:
            raise Exception(f"Failed to render query!\nQuery:\n{sql}") from e

        self._lock_translate_sql.acquire()
        try:
            translated_sql = str(
                SqlTranslate.translateSqlWithPath(
                    rendered_sql, target_dialect, None, None, self.path_to_replacement_patterns
                )
            )
            return translated_sql
        except Exception as e:
            raise Exception(f"Failed to translate query!\nRendered query:\n{rendered_sql}") from e
        finally:
            self._lock_translate_sql.release()
