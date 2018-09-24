#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import contextlib
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from decimal import Decimal
from random import randint

import numpy as np
import pandas as pd
from past.builtins.misc import xrange

from pyathena import DatabaseError, ProgrammingError, connect
from pyathena.error import NotSupportedError
from pyathena.model import AthenaQueryExecution
from pyathena.pandas_cursor import PandasCursor
from pyathena.result_set import AthenaPandasResultSet
from tests.conftest import SCHEMA
from tests.util import with_pandas_cursor


class TestPandasCursor(unittest.TestCase):

    def connect(self):
        return connect(schema_name=SCHEMA)

    @with_pandas_cursor
    def test_fetchone(self, cursor):
        cursor.execute('SELECT * FROM one_row')
        self.assertEqual(cursor.rownumber, 0)
        self.assertEqual(cursor.fetchone(), (1,))
        self.assertEqual(cursor.rownumber, 1)
        self.assertIsNone(cursor.fetchone())

    @with_pandas_cursor
    def test_fetchmany(self, cursor):
        cursor.execute('SELECT * FROM many_rows LIMIT 15')
        self.assertEqual(len(cursor.fetchmany(10)), 10)
        self.assertEqual(len(cursor.fetchmany(10)), 5)

    @with_pandas_cursor
    def test_fetchall(self, cursor):
        cursor.execute('SELECT * FROM one_row')
        self.assertEqual(cursor.fetchall(), [(1,)])
        cursor.execute('SELECT a FROM many_rows ORDER BY a')
        self.assertEqual(cursor.fetchall(), [(i,) for i in xrange(10000)])

    @with_pandas_cursor
    def test_iterator(self, cursor):
        cursor.execute('SELECT * FROM one_row')
        self.assertEqual(list(cursor), [(1,)])
        self.assertRaises(StopIteration, cursor.__next__)

    @with_pandas_cursor
    def test_arraysize(self, cursor):
        cursor.arraysize = 5
        cursor.execute('SELECT * FROM many_rows LIMIT 20')
        self.assertEqual(len(cursor.fetchmany()), 5)

    @with_pandas_cursor
    def test_arraysize_default(self, cursor):
        self.assertEqual(cursor.arraysize, AthenaPandasResultSet.DEFAULT_FETCH_SIZE)

    @with_pandas_cursor
    def test_invalid_arraysize(self, cursor):
        with self.assertRaises(ProgrammingError):
            cursor.arraysize = 10000
        with self.assertRaises(ProgrammingError):
            cursor.arraysize = -1

    @with_pandas_cursor
    def test_complex(self, cursor):
        cursor.execute("""
        SELECT
          col_boolean
          ,col_tinyint
          ,col_smallint
          ,col_int
          ,col_bigint
          ,col_float
          ,col_double
          ,col_string
          ,col_timestamp
          ,CAST(col_timestamp AS time) AS col_time
          ,col_date
          ,col_binary
          ,col_array
          ,CAST(col_array AS json) AS col_array_json
          ,col_map
          ,CAST(col_map AS json) AS col_map_json
          ,col_struct
          ,col_decimal
        FROM one_row_complex
        """)
        self.assertEqual(cursor.description, [
            ('col_boolean', 'boolean', None, None, 0, 0, 'UNKNOWN'),
            ('col_tinyint', 'tinyint', None, None, 3, 0, 'UNKNOWN'),
            ('col_smallint', 'smallint', None, None, 5, 0, 'UNKNOWN'),
            ('col_int', 'integer', None, None, 10, 0, 'UNKNOWN'),
            ('col_bigint', 'bigint', None, None, 19, 0, 'UNKNOWN'),
            ('col_float', 'float', None, None, 17, 0, 'UNKNOWN'),
            ('col_double', 'double', None, None, 17, 0, 'UNKNOWN'),
            ('col_string', 'varchar', None, None, 2147483647, 0, 'UNKNOWN'),
            ('col_timestamp', 'timestamp', None, None, 3, 0, 'UNKNOWN'),
            ('col_time', 'time', None, None, 3, 0, 'UNKNOWN'),
            ('col_date', 'date', None, None, 0, 0, 'UNKNOWN'),
            ('col_binary', 'varbinary', None, None, 1073741824, 0, 'UNKNOWN'),
            ('col_array', 'array', None, None, 0, 0, 'UNKNOWN'),
            ('col_array_json', 'json', None, None, 0, 0, 'UNKNOWN'),
            ('col_map', 'map', None, None, 0, 0, 'UNKNOWN'),
            ('col_map_json', 'json', None, None, 0, 0, 'UNKNOWN'),
            ('col_struct', 'row', None, None, 0, 0, 'UNKNOWN'),
            ('col_decimal', 'decimal', None, None, 10, 1, 'UNKNOWN'),
        ])
        rows = cursor.fetchall()
        expected = [(
            True,
            127,
            32767,
            2147483647,
            9223372036854775807,
            0.5,
            0.25,
            'a string',
            pd.Timestamp(2017, 1, 1, 0, 0, 0),
            datetime(2017, 1, 1, 0, 0, 0).time(),
            pd.Timestamp(2017, 1, 2),
            b'123',
            '[1, 2]',
            [1, 2],
            '{1=2, 3=4}',
            {'1': 2, '3': 4},
            '{a=1, b=2}',
            Decimal('0.1'),
        )]
        self.assertEqual(rows, expected)

    @with_pandas_cursor
    def test_fetch_no_data(self, cursor):
        self.assertRaises(ProgrammingError, cursor.fetchone)
        self.assertRaises(ProgrammingError, cursor.fetchmany)
        self.assertRaises(ProgrammingError, cursor.fetchall)
        self.assertRaises(ProgrammingError, cursor.as_pandas)

    @with_pandas_cursor
    def test_as_pandas(self, cursor):
        df = cursor.execute('SELECT * FROM one_row').as_pandas()
        self.assertEqual(df.shape[0], 1)
        self.assertEqual(df.shape[1], 1)
        self.assertEqual([(row['number_of_rows'],) for _, row in df.iterrows()], [(1,)])
        self.assertIsNotNone(cursor.query_id)
        self.assertIsNotNone(cursor.query)
        self.assertEqual(cursor.state, AthenaQueryExecution.STATE_SUCCEEDED)
        self.assertIsNone(cursor.state_change_reason)
        self.assertIsNotNone(cursor.completion_date_time)
        self.assertIsInstance(cursor.completion_date_time, datetime)
        self.assertIsNotNone(cursor.submission_date_time)
        self.assertIsInstance(cursor.submission_date_time, datetime)
        self.assertIsNotNone(cursor.data_scanned_in_bytes)
        self.assertIsNotNone(cursor.execution_time_in_millis)
        self.assertIsNotNone(cursor.output_location)

    @with_pandas_cursor
    def test_many_as_pandas(self, cursor):
        df = cursor.execute('SELECT * FROM many_rows').as_pandas()
        self.assertEqual(df.shape[0], 10000)
        self.assertEqual(df.shape[1], 1)
        self.assertEqual([(row['a'],) for _, row in df.iterrows()],
                         [(i,) for i in xrange(10000)])

    @with_pandas_cursor
    def test_complex_as_pandas(self, cursor):
        df = cursor.execute("""
        SELECT
          col_boolean
          ,col_tinyint
          ,col_smallint
          ,col_int
          ,col_bigint
          ,col_float
          ,col_double
          ,col_string
          ,col_timestamp
          ,CAST(col_timestamp AS time) AS col_time
          ,col_date
          ,col_binary
          ,col_array
          ,CAST(col_array AS json) AS col_array_json
          ,col_map
          ,CAST(col_map AS json) AS col_map_json
          ,col_struct
          ,col_decimal
        FROM one_row_complex
        """).as_pandas()
        self.assertEqual(df.shape[0], 1)
        self.assertEqual(df.shape[1], 18)
        dtypes = tuple([
            df['col_boolean'].dtype.type,
            df['col_tinyint'].dtype.type,
            df['col_smallint'].dtype.type,
            df['col_int'].dtype.type,
            df['col_bigint'].dtype.type,
            df['col_float'].dtype.type,
            df['col_double'].dtype.type,
            df['col_string'].dtype.type,
            df['col_timestamp'].dtype.type,
            df['col_time'].dtype.type,
            df['col_date'].dtype.type,
            df['col_binary'].dtype.type,
            df['col_array'].dtype.type,
            df['col_array_json'].dtype.type,
            df['col_map'].dtype.type,
            df['col_map_json'].dtype.type,
            df['col_struct'].dtype.type,
            df['col_decimal'].dtype.type,
        ])
        self.assertEqual(dtypes, tuple([
            np.bool_,
            np.int64,
            np.int64,
            np.int64,
            np.int64,
            np.float64,
            np.float64,
            np.object_,
            np.datetime64,
            np.object_,
            np.datetime64,
            np.object_,
            np.object_,
            np.object_,
            np.object_,
            np.object_,
            np.object_,
            np.object_,
        ]))
        rows = [tuple([
            row['col_boolean'],
            row['col_tinyint'],
            row['col_smallint'],
            row['col_int'],
            row['col_bigint'],
            row['col_float'],
            row['col_double'],
            row['col_string'],
            row['col_timestamp'],
            row['col_time'],
            row['col_date'],
            row['col_binary'],
            row['col_array'],
            row['col_array_json'],
            row['col_map'],
            row['col_map_json'],
            row['col_struct'],
            row['col_decimal'],
        ]) for _, row in df.iterrows()]
        self.assertEqual(rows, [(
            True,
            127,
            32767,
            2147483647,
            9223372036854775807,
            0.5,
            0.25,
            'a string',
            pd.Timestamp(2017, 1, 1, 0, 0, 0),
            datetime(2017, 1, 1, 0, 0, 0).time(),
            pd.Timestamp(2017, 1, 2),
            b'123',
            '[1, 2]',
            [1, 2],
            '{1=2, 3=4}',
            {'1': 2, '3': 4},
            '{a=1, b=2}',
            Decimal('0.1'),
        )])

    @with_pandas_cursor
    def test_cancel(self, cursor):
        def cancel(c):
            time.sleep(randint(1, 5))
            c.cancel()

        with ThreadPoolExecutor(max_workers=1) as executor:
            executor.submit(cancel, cursor)

            self.assertRaises(DatabaseError, lambda: cursor.execute("""
            SELECT a.a * rand(), b.a * rand()
            FROM many_rows a
            CROSS JOIN many_rows b
            """))

    @with_pandas_cursor
    def test_cancel_initial(self, cursor):
        self.assertRaises(ProgrammingError, cursor.cancel)

    def test_open_close(self):
        with contextlib.closing(self.connect()) as conn:
            with conn.cursor(PandasCursor):
                pass

    def test_no_ops(self):
        conn = self.connect()
        cursor = conn.cursor(PandasCursor)
        self.assertRaises(NotSupportedError, lambda: cursor.executemany(
            'SELECT * FROM one_row', []))
        cursor.close()
        conn.close()
