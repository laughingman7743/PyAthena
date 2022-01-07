# -*- coding: utf-8; -*-
import re

from sqlalchemy import Column, Integer, MetaData, String, Table
from sqlalchemy.sql.ddl import CreateTable

from pyathena.sqlalchemy_athena import AthenaDialect


def test_create_table():
    # Given
    table = Table('table_name', MetaData(), Column('column_name', String))
    dialect = AthenaDialect()

    # When
    statement = CreateTable(table).compile(dialect=dialect)

    # Then
    assert statement is not None


# vim: et:sw=4:syntax=python:ts=4:
