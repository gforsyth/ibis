from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy.dialects import oracle
from sqlalchemy.dialects.oracle.base import OracleDialect
from sqlalchemy.ext.compiler import compiles

import ibis.expr.datatypes as dt
import ibis.expr.schema as sch


@dt.dtype.register(OracleDialect, oracle.ROWID)
def sa_oracle_rowid(_, satype, nullable=False):
    return dt.String(nullable=nullable)


@dt.dtype.register(OracleDialect, sa.Numeric)
def sa_oracle_numeric(_, satype, nullable=True):
    if (scale := satype.scale) == 0:
        # kind of a lie, should be int128 because 38 digits
        return dt.Int64(nullable=nullable)
    return dt.Decimal(
        precision=satype.precision or 38,
        scale=scale or 0,
        nullable=nullable,
    )


@dt.dtype.register(OracleDialect, (sa.REAL, sa.FLOAT, sa.Float))
def dtype(_, satype, nullable=True):
    return dt.Float64(nullable=nullable)
