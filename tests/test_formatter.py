# -*- coding: utf-8 -*-
from __future__ import absolute_import, unicode_literals

import textwrap
import unittest
from datetime import date, datetime
from decimal import Decimal

from pyathena.error import ProgrammingError
from pyathena.formatter import DefaultParameterFormatter


class TestDefaultParameterFormatter(unittest.TestCase):

    # TODO More DDL statement test case & Complex parameter format test case

    def setUp(self):
        self.formatter = DefaultParameterFormatter()

    def format(self, operation, parameters=None):
        return self.formatter.format(operation, parameters)

    def test_add_partition(self):
        expected = textwrap.dedent(
            """
            ALTER TABLE test_table
            ADD PARTITION (dt=DATE '2017-01-01', hour=1)
            """
        ).strip()

        actual = self.format(
            textwrap.dedent(
                """
                ALTER TABLE test_table
                ADD PARTITION (dt=%(dt)s, hour=%(hour)d)
                """
            ).strip(),
            {"dt": date(2017, 1, 1), "hour": 1},
        )
        self.assertEqual(actual, expected)

    def test_drop_partition(self):
        expected = textwrap.dedent(
            """
            ALTER TABLE test_table
            DROP PARTITION (dt=DATE '2017-01-01', hour=1)
            """
        ).strip()

        actual = self.format(
            textwrap.dedent(
                """
                ALTER TABLE test_table
                DROP PARTITION (dt=%(dt)s, hour=%(hour)d)
                """
            ).strip(),
            {"dt": date(2017, 1, 1), "hour": 1},
        )
        self.assertEqual(actual, expected)

    def test_format_none(self):
        expected = textwrap.dedent(
            """
            SELECT *
            FROM test_table
            WHERE col is null
            """
        ).strip()

        actual = self.format(
            textwrap.dedent(
                """
                SELECT *
                FROM test_table
                WHERE col is %(param)s
                """
            ).strip(),
            {"param": None},
        )
        self.assertEqual(actual, expected)

    def test_format_datetime(self):
        expected = textwrap.dedent(
            """
            SELECT *
            FROM test_table
            WHERE col_timestamp >= TIMESTAMP '2017-01-01 12:00:00.000'
            AND col_timestamp <= TIMESTAMP '2017-01-02 06:00:00.000'
            """
        ).strip()

        actual = self.format(
            textwrap.dedent(
                """
                SELECT *
                FROM test_table
                WHERE col_timestamp >= %(start)s
                AND col_timestamp <= %(end)s
                """
            ).strip(),
            {
                "start": datetime(2017, 1, 1, 12, 0, 0),
                "end": datetime(2017, 1, 2, 6, 0, 0),
            },
        )
        self.assertEqual(actual, expected)

    def test_format_date(self):
        expected = textwrap.dedent(
            """
            SELECT *
            FROM test_table
            WHERE col_date between DATE '2017-01-01' and DATE '2017-01-02'
            """
        ).strip()

        actual = self.format(
            textwrap.dedent(
                """
                SELECT *
                FROM test_table
                WHERE col_date between %(start)s and %(end)s
                """
            ).strip(),
            {"start": date(2017, 1, 1), "end": date(2017, 1, 2)},
        )
        self.assertEqual(actual, expected)

    def test_format_int(self):
        expected = textwrap.dedent(
            """
            SELECT *
            FROM test_table
            WHERE col_int = 1
            """
        ).strip()

        actual = self.format(
            textwrap.dedent(
                """
                SELECT *
                FROM test_table
                WHERE col_int = %(param)s
                """
            ).strip(),
            {"param": 1},
        )
        self.assertEqual(actual, expected)

    def test_format_float(self):
        expected = textwrap.dedent(
            """
            SELECT *
            FROM test_table
            WHERE col_float >= 0.1
            """
        ).strip()

        actual = self.format(
            textwrap.dedent(
                """
                SELECT *
                FROM test_table
                WHERE col_float >= %(param).1f
                """
            ).strip(),
            {"param": 0.1},
        )
        self.assertEqual(actual, expected)

    def test_format_decimal(self):
        expected = textwrap.dedent(
            """
            SELECT *
            FROM test_table
            WHERE col_decimal <= DECIMAL '0.0000000001'
            """
        ).strip()

        actual = self.format(
            textwrap.dedent(
                """
                SELECT *
                FROM test_table
                WHERE col_decimal <= %(param)s
                """
            ).strip(),
            {"param": Decimal("0.0000000001")},
        )
        self.assertEqual(actual, expected)

    def test_format_bool(self):
        expected = textwrap.dedent(
            """
            SELECT *
            FROM test_table
            WHERE col_boolean = True
            """
        ).strip()

        actual = self.format(
            textwrap.dedent(
                """
                SELECT *
                FROM test_table
                WHERE col_boolean = %(param)s
                """
            ).strip(),
            {"param": True},
        )
        self.assertEqual(actual, expected)

    def test_format_str(self):
        expected = textwrap.dedent(
            """
            SELECT *
            FROM test_table
            WHERE col_string = 'amazon athena'
            """
        ).strip()

        actual = self.format(
            textwrap.dedent(
                """
                SELECT *
                FROM test_table
                WHERE col_string = %(param)s
                """
            ).strip(),
            {"param": "amazon athena"},
        )
        self.assertEqual(actual, expected)

    def test_format_unicode(self):
        expected = textwrap.dedent(
            """
            SELECT *
            FROM test_table
            WHERE col_string = '密林 女神'
            """
        ).strip()

        actual = self.format(
            textwrap.dedent(
                """
                SELECT *
                FROM test_table
                WHERE col_string = %(param)s
                """
            ).strip(),
            {"param": "密林 女神"},
        )
        self.assertEqual(actual, expected)

    def test_format_none_list(self):
        expected = textwrap.dedent(
            """
            SELECT *
            FROM test_table
            WHERE col IN (null, null)
            """
        ).strip()

        actual = self.format(
            textwrap.dedent(
                """
                SELECT *
                FROM test_table
                WHERE col IN %(param)s
                """
            ).strip(),
            {"param": [None, None]},
        )
        self.assertEqual(actual, expected)

    def test_format_datetime_list(self):
        expected = textwrap.dedent(
            """
            SELECT *
            FROM test_table
            WHERE col_timestamp IN
            (TIMESTAMP '2017-01-01 12:00:00.000', TIMESTAMP '2017-01-02 06:00:00.000')
            """
        ).strip()

        actual = self.format(
            textwrap.dedent(
                """
                SELECT *
                FROM test_table
                WHERE col_timestamp IN
                %(param)s
                """
            ).strip(),
            {"param": [datetime(2017, 1, 1, 12, 0, 0), datetime(2017, 1, 2, 6, 0, 0)]},
        )
        self.assertEqual(actual, expected)

    def test_format_date_list(self):
        expected = textwrap.dedent(
            """
            SELECT *
            FROM test_table
            WHERE col_date IN (DATE '2017-01-01', DATE '2017-01-02')
            """
        ).strip()

        actual = self.format(
            textwrap.dedent(
                """
                SELECT *
                FROM test_table
                WHERE col_date IN %(param)s
                """
            ).strip(),
            {"param": [date(2017, 1, 1), date(2017, 1, 2)]},
        )
        self.assertEqual(actual, expected)

    def test_format_int_list(self):
        expected = textwrap.dedent(
            """
            SELECT *
            FROM test_table
            WHERE col_int IN (1, 2)
            """
        ).strip()

        actual = self.format(
            textwrap.dedent(
                """
                SELECT *
                FROM test_table
                WHERE col_int IN %(param)s
                """
            ).strip(),
            {"param": [1, 2]},
        )
        self.assertEqual(actual, expected)

    def test_format_float_list(self):
        # default precision is 6
        expected = textwrap.dedent(
            """
            SELECT *
            FROM test_table
            WHERE col_float IN (0.100000, 0.200000)
            """
        ).strip()

        actual = self.format(
            textwrap.dedent(
                """
                SELECT *
                FROM test_table
                WHERE col_float IN %(param)s
                """
            ).strip(),
            {"param": [0.1, 0.2]},
        )
        self.assertEqual(actual, expected)

    def test_format_decimal_list(self):
        expected = textwrap.dedent(
            """
            SELECT *
            FROM test_table
            WHERE col_decimal IN (DECIMAL '0.0000000001', DECIMAL '99.9999999999')
            """
        ).strip()

        actual = self.format(
            textwrap.dedent(
                """
                SELECT *
                FROM test_table
                WHERE col_decimal IN %(param)s
                """
            ).strip(),
            {"param": [Decimal("0.0000000001"), Decimal("99.9999999999")]},
        )
        self.assertEqual(actual, expected)

    def test_format_bool_list(self):
        expected = textwrap.dedent(
            """
            SELECT *
            FROM test_table
            WHERE col_boolean IN (True, False)
            """
        ).strip()

        actual = self.format(
            textwrap.dedent(
                """
                SELECT *
                FROM test_table
                WHERE col_boolean IN %(param)s
                """
            ).strip(),
            {"param": [True, False]},
        )
        self.assertEqual(actual, expected)

    def test_format_str_list(self):
        expected = textwrap.dedent(
            """
            SELECT *
            FROM test_table
            WHERE col_string IN ('amazon', 'athena')
            """
        ).strip()

        actual = self.format(
            textwrap.dedent(
                """
                SELECT *
                FROM test_table
                WHERE col_string IN %(param)s
                """
            ).strip(),
            {"param": ["amazon", "athena"]},
        )
        self.assertEqual(actual, expected)

    def test_format_unicode_list(self):
        expected = textwrap.dedent(
            """
            SELECT *
            FROM test_table
            WHERE col_string IN ('密林', '女神')
            """
        ).strip()

        actual = self.format(
            textwrap.dedent(
                """
                SELECT *
                FROM test_table
                WHERE col_string IN %(param)s
                """
            ).strip(),
            {"param": ["密林", "女神"]},
        )
        self.assertEqual(actual, expected)

    def test_format_bad_parameter(self):
        self.assertRaises(
            ProgrammingError,
            lambda: self.format(
                """
                SELECT *
                FROM test_table
                where col_int = $(param)d
                """.strip(),
                1,
            ),
        )

        self.assertRaises(
            ProgrammingError,
            lambda: self.format(
                """
                SELECT *
                FROM test_table
                where col_string = $(param)s
                """.strip(),
                "a string",
            ),
        )

        self.assertRaises(
            ProgrammingError,
            lambda: self.format(
                """
                SELECT *
                FROM test_table
                where col_string in $(param)s
                """.strip(),
                ["a string"],
            ),
        )
