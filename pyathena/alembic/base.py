# -*- coding: utf-8; -*-
from alembic.ddl.base import AddColumn, alter_table
from alembic.ddl.impl import DefaultImpl
from sqlalchemy.ext.compiler import compiles

from pyathena.sqlalchemy.base import AthenaDialect


class AWSAthenaImpl(DefaultImpl):
    __dialect__ = AthenaDialect.name


@compiles(AddColumn, AthenaDialect.name)
def visit_add_column(element, compiler, **kw):
    return "%s %s" % (
        alter_table(compiler, element.table_name, element.schema),
        add_columns(compiler, element.column, **kw),
    )


def add_columns(compiler, column, **kw):
    return "ADD COLUMNS (%s)" % compiler.get_column_specification(column, **kw)


# vim: et:sw=4:syntax=python:ts=4:
