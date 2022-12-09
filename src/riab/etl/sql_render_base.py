# Copyright 2022 RADar-AZDelta
# SPDX-License-Identifier: gpl3+


from abc import ABC, abstractmethod
from pathlib import Path
from typing import List

import jpype
import jpype.imports


class SqlRenderBase(ABC):
    """
    Base class for the Java SQLRender
    """

    def __init__(
        self,
        target_dialect: str,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.target_dialect = target_dialect

        self.path_to_replacement_patterns = str(
            Path(__file__).parent.parent.resolve()
            / "libs"
            / "SqlRender"
            / "inst"
            / "csv"
            / "replacementPatterns.csv"
        )

        # launch the JVM
        sqlrender_path = str(
            Path(__file__).parent.parent.resolve()
            / "libs"
            / "SqlRender"
            / "inst"
            / "java"
            / "SqlRender.jar"
        )
        jpype.startJVM(classpath=[sqlrender_path])

    def _render_sql(self, sql: str, parameters: List[str], values: List[str]):
        # import the Java module
        from org.ohdsi.sql import SqlRender, SqlTranslate

        sql = str(SqlRender.renderSql(sql, parameters, values))

        sql = str(
            SqlTranslate.translateSqlWithPath(
                sql, self.target_dialect, None, None, self.path_to_replacement_patterns
            )
        )
        return sql
