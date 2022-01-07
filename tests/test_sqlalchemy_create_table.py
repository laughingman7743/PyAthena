# -*- coding: utf-8; -*-
import re

import pytest
from sqlalchemy import Column, MetaData, String, Table
from sqlalchemy.sql.ddl import CreateTable

from pyathena.sqlalchemy_athena import AthenaDialect
from tests.conftest import SCHEMA


@pytest.fixture
def expected_location(location, schema_name, table_name):
    if location[-1] != "/":
        location += "/"
    if schema_name:
        return f"{location}{schema_name}/{table_name}/"
    return f"{location}{table_name}/"


@pytest.fixture(
    params=(
        pytest.param("s3://my-bucket", id="bucket no trailing /"),
        pytest.param("s3://my-bucket/", id="bucket w. trailing /"),
        pytest.param("s3://my-bucket/some/prefix", id="bucket with prefix"),
    )
)
def location(request):
    return request.param


@pytest.fixture(
    params=(
        pytest.param(None, id="no schema"),
        pytest.param(SCHEMA, id=f'Schema "{SCHEMA}"'),
    )
)
def schema_name(request):
    return request.param


@pytest.fixture(params=(pytest.param("table_name", id="some table name"),))
def table_name(request):
    return request.param


def test_create_table(expected_location, location, schema_name, table_name):
    # Given
    table = Table(
        table_name,
        MetaData(),
        Column("column_name", String),
        schema=schema_name,
        awsathena_location=location,
    )
    dialect = AthenaDialect()
    location_pattern = re.compile(r"(?:LOCATION ')([^']+)(?:')")

    # When
    statement = CreateTable(table).compile(dialect=dialect)

    # Then
    assert statement is not None
    assert location_pattern.findall(str(statement))[0] == expected_location


# vim: et:sw=4:syntax=python:ts=4:
