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

from pyathena import DataError, DatabaseError, connect
from pyathena.error import NotSupportedError
from pyathena.model import AthenaQueryExecution
from pyathena.pandas_cursor import PandasCursor
from tests.conftest import SCHEMA
from tests.util import with_pandas_cursor


class TestAsyncCursor(unittest.TestCase):

    def connect(self):
        return connect(schema_name=SCHEMA)

    def test_parse_output_location(self):
        conn = self.connect()
        cursor = conn.cursor(PandasCursor)
        actual = cursor._parse_output_location('s3://bucket/path/to')
        self.assertEqual(actual[0], 'bucket')
        self.assertEqual(actual[1], 'path/to')

    def test_parse_invalid_output_location(self):
        conn = self.connect()
        cursor = conn.cursor(PandasCursor)
        with self.assertRaises(DataError):
            cursor._parse_output_location('http://foobar')

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
