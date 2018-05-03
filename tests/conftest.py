# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import contextlib
import os
import random
import string

import pytest
from past.builtins.misc import xrange

from pyathena import connect
from tests.util import Env, read_query

ENV = Env()
BASE_PATH = os.path.dirname(os.path.abspath(__file__))
S3_PREFIX = 'test_pyathena'
SCHEMA = 'test_pyathena_' + ''.join([random.choice(
    string.ascii_lowercase + string.digits) for i in xrange(10)])


@pytest.fixture(scope='session', autouse=True)
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
    for q in read_query(os.path.join(BASE_PATH, 'sql', 'create_database.sql')):
        cursor.execute(q.format(schema=SCHEMA))


def _drop_database(cursor):
    for q in read_query(os.path.join(BASE_PATH, 'sql', 'drop_database.sql')):
        cursor.execute(q.format(schema=SCHEMA))


def _create_table(cursor):
    location_one_row = '{0}{1}/{2}/'.format(
        ENV.s3_staging_dir, S3_PREFIX, 'one_row')
    location_many_rows = '{0}{1}/{2}/'.format(
        ENV.s3_staging_dir, S3_PREFIX, 'many_rows')
    location_one_row_complex = '{0}{1}/{2}/'.format(
        ENV.s3_staging_dir, S3_PREFIX, 'one_row_complex')
    location_partition_table = '{0}{1}/{2}/'.format(
        ENV.s3_staging_dir, S3_PREFIX, 'partition_table')
    for q in read_query(
            os.path.join(BASE_PATH, 'sql', 'create_table.sql')):
        cursor.execute(q.format(schema=SCHEMA,
                                location_one_row=location_one_row,
                                location_many_rows=location_many_rows,
                                location_one_row_complex=location_one_row_complex,
                                location_partition_table=location_partition_table))
