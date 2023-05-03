from __future__ import annotations

import sqlalchemy as sa
import toolz

import ibis.expr.operations as ops
from ibis.backends.base.sql.alchemy import (
    fixed_arity,
    reduction,
    sqlalchemy_operation_registry,
    sqlalchemy_window_functions_registry,
    unary,
)

operation_registry = sqlalchemy_operation_registry.copy()

operation_registry.update(sqlalchemy_window_functions_registry)


def _cot(t, op):
    arg = t.translate(op.arg)
    return 1.0 / sa.func.tan(arg, type_=t.get_sqla_type(op.arg.output_dtype))


def _cov(t, op):
    return t._reduction(getattr(sa.func, f"covar_{op.how[:4]}"), op)


def _corr(t, op):
    if op.how == "sample":
        raise ValueError(
            f"{t.__class__.__name__} only implements population correlation "
            "coefficient"
        )
    return t._reduction(sa.func.corr, op)


def _second(t, op):
    # Oracle returns fractional seconds, so `floor` the result to match
    # the behavior of other backends
    return sa.func.floor(sa.extract("SECOND", t.translate(op.arg)))


def _string_join(t, op):
    sep = t.translate(op.sep)
    values = list(map(t.translate, op.arg))
    return sa.func.concat(*toolz.interpose(sep, values))


operation_registry.update(
    {
        ops.Log2: unary(lambda arg: sa.func.log(2, arg)),
        ops.Log10: unary(lambda arg: sa.func.log(10, arg)),
        ops.Log: fixed_arity(lambda arg, base: sa.func.log(base, arg), 2),
        ops.Power: fixed_arity(sa.func.power, 2),
        ops.Cot: _cot,
        ops.Pi: lambda *_: sa.func.ACOS(-1),
        ops.Degrees: lambda t, op: 180 * t.translate(op.arg) / t.translate(ops.Pi()),
        ops.Radians: lambda t, op: t.translate(ops.Pi()) * t.translate(op.arg) / 180,
        # Aggregate Functions
        ops.Covariance: _cov,
        ops.Correlation: _corr,
        ops.ApproxMedian: reduction(lambda arg: sa.func.median(arg)),
        # Temporal
        ops.ExtractSecond: _second,
        # String
        ops.StrRight: fixed_arity(lambda arg, nchars: sa.func.substr(arg, -nchars), 2),
        ops.StringJoin: _string_join,
    }
)

_invalid_operations = {
    ops.StringFind,
}

operation_registry = {
    k: v for k, v in operation_registry.items() if k not in _invalid_operations
}
