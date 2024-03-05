# Copyright 2024 RADar-AZDelta
# SPDX-License-Identifier: gpl3+

from abc import ABC

from sqlalchemy import create_engine, engine

from ..etl_base import EtlBase


class SqlServerEtlBase(EtlBase, ABC):
    def __init__(
        self,
        server: str,
        user: str,
        password: str,
        port: int,
        omop_database_catalog: str,
        work_database_catalog: str,
        dqd_database_catalog: str,
        achilles_database_catalog: str,
        omop_database_schema: str,
        work_database_schema: str,
        dqd_database_schema: str,
        achilles_database_schema: str,
        **kwargs,
    ):
        """This class holds the SQL Server specific methods of the ETL process

        Args:

        """
        super().__init__(**kwargs)

        self._server = server
        self._user = user
        self._password = password
        self._port = port
        self._omop_database_catalog = omop_database_catalog
        self._work_database_catalog = work_database_catalog
        self._dqd_database_catalog = dqd_database_catalog
        self._achilles_database_catalog = achilles_database_catalog
        self._omop_database_schema = omop_database_schema
        self._work_database_schema = work_database_schema
        self._dqd_database_schema = dqd_database_schema
        self._achilles_database_schema = achilles_database_schema

        self._db_connection = engine.URL.create(
            drivername="mssql+pymssql",
            username=self._user,
            password=self._password,
            host=self._server,
            port=self._port,
        )
        # self._db_connection = engine.URL.create(
        #     drivername="mssql+pyodbc",
        #     username=self._user,
        #     password=self._password,
        #     host=self._server,
        #     port=self._port,
        #     database=self._omop_database_catalog,
        #     query={"driver": "ODBC Driver 17 for SQL Server", "TrustServerCertificate": "yes"},
        # )
        self._engine = create_engine(
            self._db_connection,
            # fast_executemany=True,
            use_insertmanyvalues=True,
        )

        self._upload_db_connection = engine.URL.create(
            drivername="mssql+pymssql",
            username=self._user,
            password=self._password,
            host=self._server,
            port=self._port,
            database=self._work_database_catalog,
        )
        # self._upload_db_connection = engine.URL.create(
        #     drivername="mssql+pyodbc",
        #     username=self._user,
        #     password=self._password,
        #     host=self._server,
        #     port=self._port,
        #     database=self._work_database_catalog,
        #     query={"driver": "ODBC Driver 17 for SQL Server", "TrustServerCertificate": "yes"},
        # )
        self._upload_engine = create_engine(
            self._upload_db_connection,
            # fast_executemany=True,
            use_insertmanyvalues=True,
        )
