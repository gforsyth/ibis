"""The Oracle backend."""

from __future__ import annotations

from typing import Any, Iterable

import sqlalchemy as sa
from sqlalchemy.dialects import oracle

import ibis.expr.datatypes as dt
import ibis.expr.schema as sch
from ibis.backends.base.sql.alchemy import BaseAlchemyBackend
from ibis.backends.oracle.compiler import OracleCompiler


class Backend(BaseAlchemyBackend):
    name = 'oracle'
    compiler = OracleCompiler
    supports_create_or_replace = True

    def do_connect(
        self,
        *,
        user: str,
        password: str,
        host: str = "localhost",
        port: int = 1521,
        database: str | None = "FREE",
        **_: Any,
    ) -> None:
        """Create an Ibis client using the passed connection parameters.

        Parameters
        ----------
        user
            Username
        password
            Password
        host
            Hostname
        port
            Port
        database
            Database to connect to
        """
        url = sa.engine.url.make_url(
            f"oracle+oracledb://{user}:{password}@{host}:{port}/{database}"
        )

        # ORACLE IS VERY CONFUSING
        # SID -- instance identifier -- meant to distinguish oracle instances running on the same machine
        # TABLESPACE -- logical grouping of tables and views, unclear how different from DATABASE
        # DATABASE can be assigned (defaults?) to a tablespace
        #
        # sqlplus ibis/ibis@localhost:1521/IBIS_TESTING
        # for connecting from docker exec
        #
        # for current session parameters
        # select * from nls_session_parameters;
        #
        # alter session parameter e.g.
        # alter session set nls_timestamp_format='YYYY-MM-DD HH24:MI:SS.FF3'
        #
        # see user tables
        # select table_name from user_tables

        self.database_name = database  # not sure what should go here

        # Note: for the moment, we need to pass the `database` in to the `make_url` call
        # AND specify it here as the `service_name`.  I don't know why.
        engine = sa.create_engine(
            url,
            poolclass=sa.pool.StaticPool,
            connect_args={
                "service_name": database,
            },
        )

        super().do_connect(engine)

    def _metadata(self, query: str) -> Iterable[tuple[str, dt.DataType]]:
        ...
