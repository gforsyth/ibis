"""The Oracle backend."""

from __future__ import annotations

from typing import Any, Iterable

import sqlalchemy as sa
from sqlalchemy.dialects import oracle

import ibis.expr.datatypes as dt
import ibis.expr.schema as sch
from ibis.backends.base.sql.alchemy import BaseAlchemyBackend
from ibis.backends.oracle.compiler import OracleCompiler
from ibis.backends.oracle.datatypes import dt as odt  # noqa: F401


class Backend(BaseAlchemyBackend):
    name = 'oracle'
    compiler = OracleCompiler
    supports_create_or_replace = False
    _quote_column_names = True
    _quote_table_names = True

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

        # Creating test DB and user
        # The ORACLE_DB env-var needs to be set in the docker-compose.yml file
        # Then, after the container is running, exec in and run (from `/opt/oracle`)
        # ./createAppUser user pass ORACLE_DB
        # where ORACLE_DB is the same name you used in the docker-compose file.

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

        res = super().do_connect(engine)

        def normalize_name(name):
            if name is None:
                return None
            elif not name:
                return ""
            elif name.lower() == name:
                return sa.sql.quoted_name(name, quote=True)
            else:
                return name

        self.con.dialect.normalize_name = normalize_name
        return res

    def _metadata(self, query: str) -> Iterable[tuple[str, dt.DataType]]:
        if not query.endswith("rows only"):
            query = f"{query.strip(';')} fetch next 1 rows only"
        with self.begin() as con, con.connection.cursor() as cur:
            result = cur.execute(query)
            desc = result.description

        for name, type_code, _, _, precision, scale, is_nullable in desc:
            if precision is not None and scale is not None and precision != 0:
                typ = dt.Decimal(precision=precision, scale=scale, nullable=is_nullable)
            elif precision == 0:
                # TODO: how to disambiguate between int and float here without inspecting the value?
                typ = dt.float
            else:
                typ = parse(FIELD_ID_TO_NAME[type_code]).copy(nullable=is_nullable)
            yield name, typ
