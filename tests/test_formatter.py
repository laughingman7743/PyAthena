# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import unittest
from datetime import date, datetime
from decimal import Decimal

from pyathena.error import ProgrammingError
from pyathena.formatter import ParameterFormatter


class TestParameterFormatter(unittest.TestCase):

    # TODO More DDL statement test case & Complex parameter format test case

    FORMATTER = ParameterFormatter()

    def format(self, operation, parameters=None):
        return self.FORMATTER.format(operation, parameters)

    def test_add_partition(self):
        expected = """
        ALTER TABLE test_table
        ADD PARTITION (dt='2017-01-01', hour=1)
        """.strip()

        actual = self.format("""
        ALTER TABLE test_table
        ADD PARTITION (dt=%(dt)s, hour=%(hour)d)
        """, {'dt': date(2017, 1, 1), 'hour': 1})
        self.assertEqual(actual, expected)

    def test_drop_partition(self):
        expected = """
        ALTER TABLE test_table
        DROP PARTITION (dt='2017-01-01', hour=1)
        """.strip()

        actual = self.format("""
        ALTER TABLE test_table
        DROP PARTITION (dt=%(dt)s, hour=%(hour)d)
        """, {'dt': date(2017, 1, 1), 'hour': 1})
        self.assertEqual(actual, expected)

    def test_format_none(self):
        expected = """
        SELECT *
        FROM test_table
        WHERE col is null
        """.strip()

        actual = self.format("""
        SELECT *
        FROM test_table
        WHERE col is %(param)s
        """, {'param': None})
        self.assertEqual(actual, expected)

    def test_format_datetime(self):
        expected = """
        SELECT *
        FROM test_table
        WHERE col_timestamp >= timestamp'2017-01-01 12:00:00.000'
          AND col_timestamp <= timestamp'2017-01-02 06:00:00.000'
        """.strip()

        actual = self.format("""
        SELECT *
        FROM test_table
        WHERE col_timestamp >= %(start)s
          AND col_timestamp <= %(end)s
        """, {'start': datetime(2017, 1, 1, 12, 0, 0), 'end': datetime(2017, 1, 2, 6, 0, 0)})
        self.assertEqual(actual, expected)

    def test_format_date(self):
        expected = """
        SELECT *
        FROM test_table
        WHERE col_date between date'2017-01-01' and date'2017-01-02'
        """.strip()

        actual = self.format("""
        SELECT *
        FROM test_table
        WHERE col_date between %(start)s and %(end)s
        """, {'start': date(2017, 1, 1), 'end': date(2017, 1, 2)})
        self.assertEqual(actual, expected)

    def test_format_int(self):
        expected = """
        SELECT *
        FROM test_table
        WHERE col_int = 1
        """.strip()

        actual = self.format("""
        SELECT *
        FROM test_table
        WHERE col_int = %(param)s
        """, {'param': 1})
        self.assertEqual(actual, expected)

    def test_format_float(self):
        expected = """
        SELECT *
        FROM test_table
        WHERE col_float >= 0.1
        """.strip()

        actual = self.format("""
        SELECT *
        FROM test_table
        WHERE col_float >= %(param).1f
        """, {'param': 0.1})
        self.assertEqual(actual, expected)

    def test_format_decimal(self):
        expected = """
        SELECT *
        FROM test_table
        WHERE col_decimal <= 0.0000000001
        """.strip()

        actual = self.format("""
        SELECT *
        FROM test_table
        WHERE col_decimal <= %(param).10f
        """, {'param': Decimal('0.0000000001')})
        self.assertEqual(actual, expected)

    def test_format_bool(self):
        expected = """
        SELECT *
        FROM test_table
        WHERE col_boolean = True
        """.strip()

        actual = self.format("""
        SELECT *
        FROM test_table
        WHERE col_boolean = %(param)s
        """, {'param': True})
        self.assertEqual(actual, expected)

    def test_format_str(self):
        expected = """
        SELECT *
        FROM test_table
        WHERE col_string = 'amazon athena'
        """.strip()

        actual = self.format("""
        SELECT *
        FROM test_table
        WHERE col_string = %(param)s
        """, {'param': 'amazon athena'})
        self.assertEqual(actual, expected)

    def test_format_unicode(self):
        expected = """
        SELECT *
        FROM test_table
        WHERE col_string = '密林 女神'
        """.strip()

        actual = self.format("""
        SELECT *
        FROM test_table
        WHERE col_string = %(param)s
        """, {'param': '密林 女神'})
        self.assertEqual(actual, expected)

    def test_format_none_list(self):
        expected = """
        SELECT *
        FROM test_table
        WHERE col IN (null,null)
        """.strip()

        actual = self.format("""
        SELECT *
        FROM test_table
        WHERE col IN %(param)s
        """, {'param': [None, None]})
        self.assertEqual(actual, expected)

    def test_format_datetime_list(self):
        expected = """
        SELECT *
        FROM test_table
        WHERE col_timestamp IN
        (timestamp'2017-01-01 12:00:00.000',timestamp'2017-01-02 06:00:00.000')
        """.strip()

        actual = self.format("""
        SELECT *
        FROM test_table
        WHERE col_timestamp IN
        %(param)s
        """, {'param': [datetime(2017, 1, 1, 12, 0, 0), datetime(2017, 1, 2, 6, 0, 0)]})
        self.assertEqual(actual, expected)

    def test_format_date_list(self):
        expected = """
        SELECT *
        FROM test_table
        WHERE col_date IN (date'2017-01-01',date'2017-01-02')
        """.strip()

        actual = self.format("""
        SELECT *
        FROM test_table
        WHERE col_date IN %(param)s
        """, {'param': [date(2017, 1, 1), date(2017, 1, 2)]})
        self.assertEqual(actual, expected)

    def test_format_int_list(self):
        expected = """
        SELECT *
        FROM test_table
        WHERE col_int IN (1,2)
        """.strip()

        actual = self.format("""
        SELECT *
        FROM test_table
        WHERE col_int IN %(param)s
        """, {'param': [1, 2]})
        self.assertEqual(actual, expected)

    def test_format_float_list(self):
        # default precision is 6
        expected = """
        SELECT *
        FROM test_table
        WHERE col_float IN (0.100000,0.200000)
        """.strip()

        actual = self.format("""
        SELECT *
        FROM test_table
        WHERE col_float IN %(param)s
        """, {'param': [0.1, 0.2]})
        self.assertEqual(actual, expected)

    def test_format_decimal_list(self):
        expected = """
        SELECT *
        FROM test_table
        WHERE col_decimal IN (0.0000000001,99.9999999999)
        """.strip()

        actual = self.format("""
        SELECT *
        FROM test_table
        WHERE col_decimal IN %(param)s
        """, {'param': [Decimal('0.0000000001'), Decimal('99.9999999999')]})
        self.assertEqual(actual, expected)

    def test_format_bool_list(self):
        expected = """
        SELECT *
        FROM test_table
        WHERE col_boolean IN (True,False)
        """.strip()

        actual = self.format("""
        SELECT *
        FROM test_table
        WHERE col_boolean IN %(param)s
        """, {'param': [True, False]})
        self.assertEqual(actual, expected)

    def test_format_str_list(self):
        expected = """
        SELECT *
        FROM test_table
        WHERE col_string IN ('amazon','athena')
        """.strip()

        actual = self.format("""
        SELECT *
        FROM test_table
        WHERE col_string IN %(param)s
        """, {'param': ['amazon', 'athena']})
        self.assertEqual(actual, expected)

    def test_format_unicode_list(self):
        expected = """
        SELECT *
        FROM test_table
        WHERE col_string IN ('密林','女神')
        """.strip()

        actual = self.format("""
        SELECT *
        FROM test_table
        WHERE col_string IN %(param)s
        """, {'param': ['密林', '女神']})
        self.assertEqual(actual, expected)

    def test_format_bad_parameter(self):
        self.assertRaises(ProgrammingError, lambda: self.format("""
        SELECT *
        FROM test_table
        where col_int = $(param)d
        """.strip(), 1))

        self.assertRaises(ProgrammingError, lambda: self.format("""
        SELECT *
        FROM test_table
        where col_string = $(param)s
        """.strip(), 'a string'))

        self.assertRaises(ProgrammingError, lambda: self.format("""
        SELECT *
        FROM test_table
        where col_string in $(param)s
        """.strip(), ['a string']))
