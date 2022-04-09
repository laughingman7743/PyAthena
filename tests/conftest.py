# -*- coding: utf-8 -*-
import contextlib
import os
import uuid
from urllib.parse import quote_plus

import pytest
import sqlalchemy

from tests import BASE_PATH, ENV, S3_PREFIX, SCHEMA
from tests.util import read_query


def connect(schema_name="default", **kwargs):
    from pyathena import connect

    return connect(schema_name=schema_name, **kwargs)


def create_engine(**kwargs):
    conn_str = (
        "awsathena+rest://athena.{region_name}.amazonaws.com:443/"
        + "{schema_name}?s3_staging_dir={s3_staging_dir}&s3_dir={s3_dir}"
        + "&compression=snappy"
    )
    if "verify" in kwargs:
        conn_str += "&verify={verify}"
    if "duration_seconds" in kwargs:
        conn_str += "&duration_seconds={duration_seconds}"
    if "poll_interval" in kwargs:
        conn_str += "&poll_interval={poll_interval}"
    if "kill_on_interrupt" in kwargs:
        conn_str += "&kill_on_interrupt={kill_on_interrupt}"
    return sqlalchemy.engine.create_engine(
        conn_str.format(
            region_name=ENV.region_name,
            schema_name=SCHEMA,
            s3_staging_dir=quote_plus(ENV.s3_staging_dir),
            s3_dir=quote_plus(ENV.s3_staging_dir),
            **kwargs,
        )
    )


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
    table_execute_many = f"execute_many_{str(uuid.uuid4()).replace('-', '')}"
    table_execute_many_pandas = (
        f"execute_many_pandas_{str(uuid.uuid4()).replace('-', '')}"
    )
    for q in read_query(os.path.join(BASE_PATH, "sql", "create_table.sql")):
        cursor.execute(
            q.format(
                schema=SCHEMA,
                location_one_row=f"{ENV.s3_staging_dir}{S3_PREFIX}/one_row/",
                location_many_rows=f"{ENV.s3_staging_dir}{S3_PREFIX}/many_rows/",
                location_one_row_complex=f"{ENV.s3_staging_dir}{S3_PREFIX}/one_row_complex/",
                location_partition_table=f"{ENV.s3_staging_dir}{S3_PREFIX}/partition_table/",
                location_integer_na_values=f"{ENV.s3_staging_dir}{S3_PREFIX}/integer_na_values/",
                location_boolean_na_values=f"{ENV.s3_staging_dir}{S3_PREFIX}/boolean_na_values/",
                location_execute_many=f"{ENV.s3_staging_dir}{S3_PREFIX}/{table_execute_many}/",
                location_execute_many_pandas=f"{ENV.s3_staging_dir}{S3_PREFIX}/"
                f"{table_execute_many_pandas}/",
                location_parquet_with_compression=f"{ENV.s3_staging_dir}{S3_PREFIX}/"
                f"parquet_with_compression/",
            )
        )


def _cursor(cursor_class, request):
    if not hasattr(request, "param"):
        setattr(request, "param", {})
    with contextlib.closing(
        connect(schema_name=SCHEMA, cursor_class=cursor_class, **request.param)
    ) as conn:
        with conn.cursor() as cursor:
            yield cursor


@pytest.fixture
def cursor(request):
    from pyathena.cursor import Cursor

    yield from _cursor(Cursor, request)


@pytest.fixture
def dict_cursor(request):
    from pyathena.cursor import DictCursor

    yield from _cursor(DictCursor, request)


@pytest.fixture
def async_cursor(request):
    from pyathena.async_cursor import AsyncCursor

    yield from _cursor(AsyncCursor, request)


@pytest.fixture
def async_dict_cursor(request):
    from pyathena.async_cursor import AsyncDictCursor

    yield from _cursor(AsyncDictCursor, request)


@pytest.fixture
def pandas_cursor(request):
    from pyathena.pandas.cursor import PandasCursor

    yield from _cursor(PandasCursor, request)


@pytest.fixture
def async_pandas_cursor(request):
    from pyathena.pandas.async_cursor import AsyncPandasCursor

    yield from _cursor(AsyncPandasCursor, request)


@pytest.fixture
def engine(request):
    if not hasattr(request, "param"):
        setattr(request, "param", {})
    engine_ = create_engine(**request.param)
    try:
        with contextlib.closing(engine_.connect()) as conn:
            yield engine_, conn
    finally:
        engine_.dispose()


@pytest.fixture
def formatter():
    from pyathena.formatter import DefaultParameterFormatter

    return DefaultParameterFormatter()
