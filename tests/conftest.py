# -*- coding: utf-8 -*-
import contextlib
from pathlib import Path
from urllib.parse import quote_plus

import boto3
import pytest
import sqlalchemy

from tests import ENV
from tests.util import read_query


def connect(schema_name="default", **kwargs):
    from pyathena import connect

    if "work_group" not in kwargs:
        kwargs["work_group"] = ENV.default_work_group
    return connect(schema_name=schema_name, **kwargs)


def create_engine(**kwargs):
    conn_str = (
        "awsathena+rest://athena.{region_name}.amazonaws.com:443/"
        + "{schema_name}?s3_staging_dir={s3_staging_dir}&location={location}"
    )
    for arg in [
        "verify",
        "duration_seconds",
        "poll_interval",
        "kill_on_interrupt",
        "file_format",
        "row_format",
        "compression",
        "tblproperties",
        "serdeproperties",
        "partition",
        "cluster",
        "bucket_count",
    ]:
        if arg in kwargs:
            conn_str += f"&{arg}={{{arg}}}"
    return sqlalchemy.engine.create_engine(
        conn_str.format(
            region_name=ENV.region_name,
            schema_name=ENV.schema,
            s3_staging_dir=quote_plus(ENV.s3_staging_dir),
            location=quote_plus(ENV.s3_staging_dir),
            **kwargs,
        )
    )


@pytest.fixture(scope="session", autouse=True)
def _setup_session(request):
    request.addfinalizer(_teardown_session)
    _upload_rows()
    with contextlib.closing(connect()) as conn:
        with conn.cursor() as cursor:
            _create_database(cursor)
            _create_table(cursor)


def _teardown_session():
    with contextlib.closing(connect()) as conn:
        with conn.cursor() as cursor:
            _drop_database(cursor)
    _delete_rows()


def _upload_rows():
    client = boto3.client("s3")
    rows = Path(__file__).parent.resolve() / "resources" / "rows"
    for row in rows.iterdir():
        key = f"{ENV.s3_staging_key}{ENV.schema}/{row.stem}/{row.name}"
        client.upload_file(str(row), ENV.s3_staging_bucket, key)


def _delete_rows():
    client = boto3.client("s3")
    rows = Path(__file__).parent.resolve() / "resources" / "rows"
    for row in rows.iterdir():
        key = f"{ENV.s3_staging_key}{ENV.schema}/{row.stem}/{row.name}"
        client.delete_object(Bucket=ENV.s3_staging_bucket, Key=key)


def _create_database(cursor):
    for q in read_query("create_database.sql.jinja2", schema=ENV.schema):
        cursor.execute(q)


def _drop_database(cursor):
    for q in read_query("drop_database.sql.jinja2", schema=ENV.schema):
        cursor.execute(q)


def _create_table(cursor):
    for q in read_query(
        "create_table.sql.jinja2", s3_staging_dir=ENV.s3_staging_dir, schema=ENV.schema
    ):
        cursor.execute(q)


def _cursor(cursor_class, request):
    if not hasattr(request, "param"):
        setattr(request, "param", {})
    with contextlib.closing(
        connect(schema_name=ENV.schema, cursor_class=cursor_class, **request.param)
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
