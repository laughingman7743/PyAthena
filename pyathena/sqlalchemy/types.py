#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import annotations

from collections import OrderedDict
from datetime import date, datetime
from typing import TYPE_CHECKING, Any, Optional, Union

import sqlalchemy
from sqlalchemy.sql.sqltypes import Float, Indexable
from sqlalchemy.sql.type_api import TypeEngine, UserDefinedType

from pyathena.sqlalchemy import base
from pyathena.sqlalchemy.constants import sqlalchemy_1_4_or_more

if TYPE_CHECKING:
    from sqlalchemy import Dialect
    from sqlalchemy.sql.type_api import _LiteralProcessorType


class AthenaTimestamp(TypeEngine[datetime]):
    render_literal_cast = True
    render_bind_cast = True

    @staticmethod
    def process(value: Optional[Union[datetime, Any]]) -> str:
        if isinstance(value, datetime):
            return f"""TIMESTAMP '{value.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]}'"""
        return f"TIMESTAMP '{str(value)}'"

    def literal_processor(self, dialect: "Dialect") -> Optional["_LiteralProcessorType[datetime]"]:
        return self.process


class DOUBLE(Float):  # type: ignore
    __visit_name__ = "double"


class AthenaDate(TypeEngine[date]):
    render_literal_cast = True
    render_bind_cast = True

    @staticmethod
    def process(value: Union[date, datetime, Any]) -> str:
        if isinstance(value, (date, datetime)):
            f"DATE '{value:%Y-%m-%d}'"
        return f"DATE '{str(value)}'"

    def literal_processor(self, dialect: "Dialect") -> Optional["_LiteralProcessorType[date]"]:
        return self.process


if sqlalchemy_1_4_or_more:
    import sqlalchemy.sql.coercions
    import sqlalchemy.sql.roles


def _get_subtype_col_spec(type_):
    global _get_subtype_col_spec

    type_compiler = base.dialect.type_compiler(base.dialect())
    _get_subtype_col_spec = type_compiler.process
    return _get_subtype_col_spec(type_)


class STRUCT(Indexable, UserDefinedType):  # type: ignore
    """
    A type for Athena STRUCT data

    See https://docs.aws.amazon.com/athena/latest/ug/data-types.html
    Inspired by https://github.com/googleapis/python-bigquery-sqlalchemy
    """

    # See https://docs.sqlalchemy.org/en/14/core/custom_types.html#creating-new-types

    def __init__(
        self,
        *fields,
        **kwfields,
    ):
        # Note that because:
        # https://docs.python.org/3/whatsnew/3.6.html#pep-468-preserving-keyword-argument-order
        # We know that `kwfields` preserves order.
        self._struct_fields = tuple(
            (
                name,
                type_ if isinstance(type_, TypeEngine) else type_(),
            )
            for (name, type_) in (fields + tuple(kwfields.items()))
        )

        self._struct_by_name = {name.lower(): type_ for (name, type_) in self._struct_fields}

    def __repr__(self):
        fields = ", ".join(f"{name}={repr(type_)}" for name, type_ in self._struct_fields)
        return f"STRUCT({fields})"

    def get_col_spec(self, **kw):
        fields = ", ".join(
            f"{name}: {_get_subtype_col_spec(type_)}" for name, type_ in self._struct_fields
        )
        return f"STRUCT<{fields}>"

    def bind_processor(self, dialect):
        return OrderedDict

    class Comparator(Indexable.Comparator):  # type: ignore
        def _setup_getitem(self, name):
            if not isinstance(name, str):
                raise TypeError(
                    f"STRUCT fields can only be accessed with strings field names,"
                    f" not {repr(name)}."
                )
            subtype = self.expr.type._struct_by_name.get(name.lower())  # type: ignore
            if subtype is None:
                raise KeyError(name)
            operator = struct_getitem_op
            index = _field_index(self, name, operator)
            return operator, index, subtype

        def __getattr__(self, name):
            if name.lower() in self.expr.type._struct_by_name:  # type: ignore
                return self[name]

    comparator_factory = Comparator


# In the implementations of _field_index below, we're stealing from
# the JSON type implementation, but the code to steal changed in
# 1.4. :/
if sqlalchemy_1_4_or_more:

    def _field_index(self, name, operator):
        return sqlalchemy.sql.coercions.expect(
            sqlalchemy.sql.roles.BinaryElementRole,
            name,
            expr=self.expr,
            operator=operator,
            bindparam_type=sqlalchemy.types.String(),
        )

else:

    def _field_index(self, name, operator):
        return sqlalchemy.sql.default_comparator._check_literal(  # type: ignore
            self.expr,
            operator,
            name,
            bindparam_type=sqlalchemy.types.String(),
        )


def struct_getitem_op(a, b):
    raise NotImplementedError()


sqlalchemy.sql.default_comparator.operator_lookup[
    struct_getitem_op.__name__
] = sqlalchemy.sql.default_comparator.operator_lookup["json_getitem_op"]
