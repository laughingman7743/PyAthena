# -*- coding: utf-8 -*-
from __future__ import absolute_import, unicode_literals

import contextlib
import os
import uuid

import pytest

from pyathena import connect
from tests import BASE_PATH, ENV, S3_PREFIX, SCHEMA
from tests.util import read_query


@pytest.fixture(scope="session", autouse=True)
def _setup_session(request):
    request.addfinalizer(_teardown_session)
    with contextlib.closing(connect()) as conn:
        with conn.cursor() as cursor:
            _create_database(cursor)
            _create_table(cursor)


def _teardown_session():
    with contextlib.closing(connect()) as conn:
        with conn.cursor() as cursor:
            _drop_database(cursor)


def _create_database(cursor):
    for q in read_query(os.path.join(BASE_PATH, "sql", "create_database.sql")):
        cursor.execute(q.format(schema=SCHEMA))


def _drop_database(cursor):
    for q in read_query(os.path.join(BASE_PATH, "sql", "drop_database.sql")):
        cursor.execute(q.format(schema=SCHEMA))


def _create_table(cursor):
    location_one_row = "{0}{1}/{2}/".format(ENV.s3_staging_dir, S3_PREFIX, "one_row")
    location_many_rows = "{0}{1}/{2}/".format(
        ENV.s3_staging_dir, S3_PREFIX, "many_rows"
    )
    location_one_row_complex = "{0}{1}/{2}/".format(
        ENV.s3_staging_dir, S3_PREFIX, "one_row_complex"
    )
    location_partition_table = "{0}{1}/{2}/".format(
        ENV.s3_staging_dir, S3_PREFIX, "partition_table"
    )
    location_integer_na_values = "{0}{1}/{2}/".format(
        ENV.s3_staging_dir, S3_PREFIX, "integer_na_values"
    )
    location_boolean_na_values = "{0}{1}/{2}/".format(
        ENV.s3_staging_dir, S3_PREFIX, "boolean_na_values"
    )
    location_execute_many = "{0}{1}/{2}/".format(
        ENV.s3_staging_dir,
        S3_PREFIX,
        "execute_many_{0}".format(str(uuid.uuid4()).replace("-", "")),
    )
    location_execute_many_pandas = "{0}{1}/{2}/".format(
        ENV.s3_staging_dir,
        S3_PREFIX,
        "execute_many_pandas_{0}".format(str(uuid.uuid4()).replace("-", "")),
    )
    for q in read_query(os.path.join(BASE_PATH, "sql", "create_table.sql")):
        cursor.execute(
            q.format(
                schema=SCHEMA,
                location_one_row=location_one_row,
                location_many_rows=location_many_rows,
                location_one_row_complex=location_one_row_complex,
                location_partition_table=location_partition_table,
                location_integer_na_values=location_integer_na_values,
                location_boolean_na_values=location_boolean_na_values,
                location_execute_many=location_execute_many,
                location_execute_many_pandas=location_execute_many_pandas,
            )
        )
