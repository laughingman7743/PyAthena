# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import contextlib
import time
import unittest
from datetime import datetime
from random import randint

from past.builtins.misc import xrange

from pyathena import connect
from pyathena.async_cursor import AsyncCursor
from pyathena.error import NotSupportedError, ProgrammingError
from pyathena.model import AthenaQueryExecution
from pyathena.result_set import AthenaResultSet
from tests.conftest import SCHEMA
from tests.util import with_async_cursor


class TestAsyncCursor(unittest.TestCase):

    def connect(self):
        return connect(schema_name=SCHEMA)

    @with_async_cursor
    def test_fetchone(self, cursor):
        query_id, future = cursor.execute('SELECT * FROM one_row')
        result_set = future.result()
        self.assertEqual(result_set.rownumber, 0)
        self.assertEqual(result_set.fetchone(), (1,))
        self.assertEqual(result_set.rownumber, 1)
        self.assertIsNone(result_set.fetchone())
        self.assertIsNotNone(result_set.query_id)
        self.assertIsNotNone(result_set.query)
        self.assertEqual(result_set.state, AthenaQueryExecution.STATE_SUCCEEDED)
        self.assertIsNone(result_set.state_change_reason)
        self.assertIsNotNone(result_set.completion_date_time)
        self.assertIsInstance(result_set.completion_date_time, datetime)
        self.assertIsNotNone(result_set.submission_date_time)
        self.assertIsInstance(result_set.submission_date_time, datetime)
        self.assertIsNotNone(result_set.data_scanned_in_bytes)
        self.assertIsNotNone(result_set.execution_time_in_millis)
        self.assertIsNotNone(result_set.output_location)

    @with_async_cursor
    def test_fetchmany(self, cursor):
        query_id, future = cursor.execute('SELECT * FROM many_rows LIMIT 15')
        result_set = future.result()
        self.assertEqual(len(result_set.fetchmany(10)), 10)
        self.assertEqual(len(result_set.fetchmany(10)), 5)

    @with_async_cursor
    def test_fetchall(self, cursor):
        query_id, future = cursor.execute('SELECT * FROM one_row')
        result_set = future.result()
        self.assertEqual(result_set.fetchall(), [(1,)])
        query_id, future = cursor.execute('SELECT a FROM many_rows ORDER BY a')
        result_set = future.result()
        self.assertEqual(result_set.fetchall(), [(i,) for i in xrange(10000)])

    @with_async_cursor
    def test_iterator(self, cursor):
        query_id, future = cursor.execute('SELECT * FROM one_row')
        result_set = future.result()
        self.assertEqual(list(result_set), [(1,)])
        self.assertRaises(StopIteration, result_set.__next__)

    @with_async_cursor
    def test_arraysize(self, cursor):
        cursor.arraysize = 5
        query_id, future = cursor.execute('SELECT * FROM many_rows LIMIT 20')
        result_set = future.result()
        self.assertEqual(len(result_set.fetchmany()), 5)

    @with_async_cursor
    def test_arraysize_default(self, cursor):
        self.assertEqual(cursor.arraysize, AthenaResultSet.DEFAULT_FETCH_SIZE)

    @with_async_cursor
    def test_invalid_arraysize(self, cursor):
        with self.assertRaises(ProgrammingError):
            cursor.arraysize = 10000
        with self.assertRaises(ProgrammingError):
            cursor.arraysize = -1

    @with_async_cursor
    def test_description(self, cursor):
        query_id, future = cursor.execute('SELECT 1 AS foobar FROM one_row')
        result_set = future.result()
        self.assertEqual(result_set.description,
                         [('foobar', 'integer', None, None, 10, 0, 'UNKNOWN')])

        future = cursor.description(query_id)
        description = future.result()
        self.assertEqual(result_set.description, description)

    @with_async_cursor
    def test_query_execution(self, cursor):
        query = 'SELECT * FROM one_row'
        query_id, future = cursor.execute(query)
        result_set = future.result()

        future = cursor.query_execution(query_id)
        query_execution = future.result()

        self.assertIsNotNone(query_execution.query_id)
        self.assertEqual(query_execution.query, query)
        self.assertEqual(query_execution.state, AthenaQueryExecution.STATE_SUCCEEDED)
        self.assertIsNone(query_execution.state_change_reason)
        self.assertIsNotNone(query_execution.completion_date_time)
        self.assertIsInstance(query_execution.completion_date_time, datetime)
        self.assertIsNotNone(query_execution.submission_date_time)
        self.assertIsInstance(query_execution.submission_date_time, datetime)
        self.assertIsNotNone(query_execution.data_scanned_in_bytes)
        self.assertIsNotNone(query_execution.execution_time_in_millis)
        self.assertIsNotNone(query_execution.output_location)

        self.assertEqual(result_set.query_id, query_execution.query_id)
        self.assertEqual(result_set.query, query_execution.query)
        self.assertEqual(result_set.state, query_execution.state)
        self.assertEqual(result_set.state_change_reason, query_execution.state_change_reason)
        self.assertEqual(result_set.completion_date_time, query_execution.completion_date_time)
        self.assertEqual(result_set.submission_date_time, query_execution.submission_date_time)
        self.assertEqual(result_set.data_scanned_in_bytes, query_execution.data_scanned_in_bytes)
        self.assertEqual(result_set.execution_time_in_millis,
                         query_execution.execution_time_in_millis)
        self.assertEqual(result_set.output_location, query_execution.output_location)

    @with_async_cursor
    def test_poll(self, cursor):
        query_id, _ = cursor.execute("SELECT * FROM one_row")
        future = cursor.poll(query_id)
        query_execution = future.result()
        self.assertIn(query_execution.state, [AthenaQueryExecution.STATE_QUEUED,
                                              AthenaQueryExecution.STATE_RUNNING,
                                              AthenaQueryExecution.STATE_SUCCEEDED,
                                              AthenaQueryExecution.STATE_FAILED,
                                              AthenaQueryExecution.STATE_CANCELLED])

    @with_async_cursor
    def test_bad_query(self, cursor):
        query_id, future = cursor.execute('SELECT does_not_exist FROM this_really_does_not_exist')
        result_set = future.result()
        self.assertEqual(result_set.state, AthenaQueryExecution.STATE_FAILED)
        self.assertIsNotNone(result_set.state_change_reason)

    @with_async_cursor
    def test_cancel(self, cursor):
        query_id, future = cursor.execute("""
                           SELECT a.a * rand(), b.a * rand()
                           FROM many_rows a
                           CROSS JOIN many_rows b
                           """)
        time.sleep(randint(1, 5))
        cursor.cancel(query_id)
        result_set = future.result()
        self.assertEqual(result_set.state, AthenaQueryExecution.STATE_CANCELLED)
        self.assertIsNone(result_set.state_change_reason)
        self.assertIsNone(result_set.description)
        self.assertIsNone(result_set.fetchone())
        self.assertEqual(result_set.fetchmany(), [])
        self.assertEqual(result_set.fetchall(), [])

    def test_open_close(self):
        with contextlib.closing(self.connect()) as conn:
            with conn.cursor(AsyncCursor):
                pass

    def test_no_ops(self):
        conn = self.connect()
        cursor = conn.cursor(AsyncCursor)
        self.assertRaises(NotSupportedError, lambda: cursor.executemany(
            'SELECT * FROM one_row', []))
        cursor.close()
        conn.close()
