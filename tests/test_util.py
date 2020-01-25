# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import unittest
from datetime import datetime, date
from decimal import Decimal

import numpy as np

from pyathena import DataError
from pyathena.util import as_pandas, parse_output_location
from tests import WithConnect
from tests.util import with_cursor


class TestUtil(unittest.TestCase, WithConnect):

    def test_parse_output_location(self):
        # valid
        actual = parse_output_location('s3://bucket/path/to')
        self.assertEqual(actual[0], 'bucket')
        self.assertEqual(actual[1], 'path/to')

        # invalid
        with self.assertRaises(DataError):
            parse_output_location('http://foobar')

    def test_get_chunks(self):
        # TODO
        pass

    def get_reset_index(self):
        # TODO
        pass

    @with_cursor()
    def test_as_pandas(self, cursor):
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
        df = as_pandas(cursor)
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
        expected = [(
            True,
            127,
            32767,
            2147483647,
            9223372036854775807,
            0.5,
            0.25,
            'a string',
            datetime(2017, 1, 1, 0, 0, 0),
            datetime(2017, 1, 1, 0, 0, 0).time(),
            date(2017, 1, 2),
            b'123',
            '[1, 2]',
            [1, 2],
            '{1=2, 3=4}',
            {'1': 2, '3': 4},
            '{a=1, b=2}',
            Decimal('0.1'),
        )]
        self.assertEqual(rows, expected)

    @with_cursor()
    def test_as_pandas_integer_na_values(self, cursor):
        cursor.execute("""
        SELECT * FROM integer_na_values
        """)
        df = as_pandas(cursor, coerce_float=True)
        rows = [tuple([
            row['a'],
            row['b'],
        ]) for _, row in df.iterrows()]
        # TODO AssertionError: Lists differ:
        #  [(1.0, 2.0), (1.0, nan), (nan, nan)] != [(1.0, 2.0), (1.0, nan), (nan, nan)]
        # self.assertEqual(rows, [
        #     (1.0, 2.0),
        #     (1.0, np.nan),
        #     (np.nan, np.nan),
        # ])
        np.testing.assert_array_equal(rows, [
            (1, 2),
            (1, np.nan),
            (np.nan, np.nan),
        ])

    @with_cursor()
    def test_as_pandas_boolean_na_values(self, cursor):
        cursor.execute("""
        SELECT * FROM boolean_na_values
        """)
        df = as_pandas(cursor)
        rows = [tuple([
            row['a'],
            row['b'],
        ]) for _, row in df.iterrows()]
        self.assertEqual(rows, [
            (True, False),
            (False, None),
            (None, None),
        ])

    def test_to_sql_type_mappings(self):
        # TODO
        pass

    def test_get_column_names_and_types(self):
        # TODO
        pass

    def test_generate_ddl(self):
        # TODO
        pass
