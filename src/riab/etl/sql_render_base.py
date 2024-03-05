# Copyright 2024 RADar-AZDelta
# SPDX-License-Identifier: gpl3+

from abc import ABC
from pathlib import Path

import jpype
import jpype.imports


class SqlRenderBase(ABC):  # TODO: make this obsolete!!!!
    """
    Base class for the Java SQLRender
    """

    def __init__(
        self,
        db_engine: str,
        **kwargs,
    ):
        super().__init__(**kwargs)

        self._db_engine = db_engine

        self.path_to_replacement_patterns = str(
            Path(__file__).parent.parent.resolve() / "libs" / "SqlRender" / "inst" / "csv" / "replacementPatterns.csv"
        )

        # launch the JVM
        sqlrender_path = str(
            Path(__file__).parent.parent.resolve() / "libs" / "SqlRender" / "inst" / "java" / "SqlRender.jar"
        )
        jpype.startJVM(classpath=[sqlrender_path])  # type: ignore

    def _render_sql(self, sql: str, parameters: dict) -> str:
        """_summary_

        Args:
            sql (str): Original SQL
            parameters (List[str]): Query parameter names
            values (List[str]): Query parameter

        Returns:
            str: The rendered SQL
        """
        # import the Java module
        from org.ohdsi.sql import (  # type: ignore # pylint: disable=import-outside-toplevel,import-error
            SqlRender,
            SqlTranslate,
        )

        sql = str(SqlRender.renderSql(sql, list(parameters.keys()), list(parameters.values())))

        sql = str(
            SqlTranslate.translateSqlWithPath(sql, self._db_engine, None, None, self.path_to_replacement_patterns)
        )
        return sql
