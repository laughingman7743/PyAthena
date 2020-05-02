# -*- coding: utf-8 -*-
from __future__ import absolute_import, unicode_literals

import textwrap
import unittest
import uuid
from datetime import date, datetime
from decimal import Decimal

import numpy as np
import pandas as pd
from past.builtins import xrange

from pyathena import DataError, OperationalError
from pyathena.util import (
    as_pandas,
    generate_ddl,
    get_chunks,
    parse_output_location,
    reset_index,
    to_sql,
)
from tests import ENV, S3_PREFIX, SCHEMA, WithConnect
from tests.util import with_cursor


class TestUtil(unittest.TestCase, WithConnect):
    def test_parse_output_location(self):
        # valid
        actual = parse_output_location("s3://bucket/path/to")
        self.assertEqual(actual[0], "bucket")
        self.assertEqual(actual[1], "path/to")

        # invalid
        with self.assertRaises(DataError):
            parse_output_location("http://foobar")

    def test_get_chunks(self):
        df = pd.DataFrame({"a": [1, 2, 3, 4, 5]})
        actual1 = get_chunks(df)
        self.assertEqual([len(a) for a in actual1], [5])
        actual2 = get_chunks(df, chunksize=2)
        self.assertEqual([len(a) for a in actual2], [2, 2, 1])
        actual3 = get_chunks(df, chunksize=10)
        self.assertEqual([len(a) for a in actual3], [5])

        # empty
        self.assertEqual(list(get_chunks(pd.DataFrame())), [])

        # invalid
        with self.assertRaises(ValueError):
            list(get_chunks(df, chunksize=0))
        with self.assertRaises(ValueError):
            list(get_chunks(df, chunksize=-1))

    def test_reset_index(self):
        df = pd.DataFrame({"a": [1, 2, 3, 4, 5]})
        reset_index(df)
        self.assertEqual(list(df.columns), ["index", "a"])

        df = pd.DataFrame({"a": [1, 2, 3, 4, 5]})
        reset_index(df, index_label="__index__")
        self.assertEqual(list(df.columns), ["__index__", "a"])

        df = pd.DataFrame({"a": [1, 2, 3, 4, 5]})
        with self.assertRaises(ValueError):
            reset_index(df, index_label="a")

    @with_cursor()
    def test_as_pandas(self, cursor):
        cursor.execute(
            """
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
        """
        )
        df = as_pandas(cursor)
        rows = [
            tuple(
                [
                    row["col_boolean"],
                    row["col_tinyint"],
                    row["col_smallint"],
                    row["col_int"],
                    row["col_bigint"],
                    row["col_float"],
                    row["col_double"],
                    row["col_string"],
                    row["col_timestamp"],
                    row["col_time"],
                    row["col_date"],
                    row["col_binary"],
                    row["col_array"],
                    row["col_array_json"],
                    row["col_map"],
                    row["col_map_json"],
                    row["col_struct"],
                    row["col_decimal"],
                ]
            )
            for _, row in df.iterrows()
        ]
        expected = [
            (
                True,
                127,
                32767,
                2147483647,
                9223372036854775807,
                0.5,
                0.25,
                "a string",
                datetime(2017, 1, 1, 0, 0, 0),
                datetime(2017, 1, 1, 0, 0, 0).time(),
                date(2017, 1, 2),
                b"123",
                "[1, 2]",
                [1, 2],
                "{1=2, 3=4}",
                {"1": 2, "3": 4},
                "{a=1, b=2}",
                Decimal("0.1"),
            )
        ]
        self.assertEqual(rows, expected)

    @with_cursor()
    def test_as_pandas_integer_na_values(self, cursor):
        cursor.execute(
            """
            SELECT * FROM integer_na_values
            """
        )
        df = as_pandas(cursor, coerce_float=True)
        rows = [tuple([row["a"], row["b"]]) for _, row in df.iterrows()]
        # TODO AssertionError: Lists differ:
        #  [(1.0, 2.0), (1.0, nan), (nan, nan)] != [(1.0, 2.0), (1.0, nan), (nan, nan)]
        # self.assertEqual(rows, [
        #     (1.0, 2.0),
        #     (1.0, np.nan),
        #     (np.nan, np.nan),
        # ])
        np.testing.assert_array_equal(rows, [(1, 2), (1, np.nan), (np.nan, np.nan)])

    @with_cursor()
    def test_as_pandas_boolean_na_values(self, cursor):
        cursor.execute(
            """
        SELECT * FROM boolean_na_values
        """
        )
        df = as_pandas(cursor)
        rows = [tuple([row["a"], row["b"]]) for _, row in df.iterrows()]
        self.assertEqual(rows, [(True, False), (False, None), (None, None)])

    def test_generate_ddl(self):
        # TODO Add binary column (After dropping support for Python 2.7)
        df = pd.DataFrame(
            {
                "col_int": np.int32([1]),
                "col_bigint": np.int64([12345]),
                "col_float": np.float32([1.0]),
                "col_double": np.float64([1.2345]),
                "col_string": ["a"],
                "col_boolean": np.bool_([True]),
                "col_timestamp": [datetime(2020, 1, 1, 0, 0, 0)],
                "col_date": [date(2020, 12, 31)],
                "col_timedelta": [np.timedelta64(1, "D")],
            }
        )
        # Explicitly specify column order
        df = df[
            [
                "col_int",
                "col_bigint",
                "col_float",
                "col_double",
                "col_string",
                "col_boolean",
                "col_timestamp",
                "col_date",
                "col_timedelta",
            ]
        ]

        actual = generate_ddl(df, "test_table", "s3://bucket/path/to/", "test_schema")
        self.assertEqual(
            actual.strip(),
            textwrap.dedent(
                """
                CREATE EXTERNAL TABLE IF NOT EXISTS `test_schema`.`test_table` (
                `col_int` INT,
                `col_bigint` BIGINT,
                `col_float` FLOAT,
                `col_double` DOUBLE,
                `col_string` STRING,
                `col_boolean` BOOLEAN,
                `col_timestamp` TIMESTAMP,
                `col_date` DATE,
                `col_timedelta` BIGINT
                )
                STORED AS PARQUET
                LOCATION 's3://bucket/path/to/'
                """
            ).strip(),
        )

        # compression
        actual = generate_ddl(
            df,
            "test_table",
            "s3://bucket/path/to/",
            "test_schema",
            compression="snappy",
        )
        self.assertEqual(
            actual.strip(),
            textwrap.dedent(
                """
                CREATE EXTERNAL TABLE IF NOT EXISTS `test_schema`.`test_table` (
                `col_int` INT,
                `col_bigint` BIGINT,
                `col_float` FLOAT,
                `col_double` DOUBLE,
                `col_string` STRING,
                `col_boolean` BOOLEAN,
                `col_timestamp` TIMESTAMP,
                `col_date` DATE,
                `col_timedelta` BIGINT
                )
                STORED AS PARQUET
                LOCATION 's3://bucket/path/to/'
                TBLPROPERTIES ('parquet.compress'='SNAPPY')
                """
            ).strip(),
        )

        # partitions
        actual = generate_ddl(
            df,
            "test_table",
            "s3://bucket/path/to/",
            "test_schema",
            partitions=["col_int"],
        )
        self.assertEqual(
            actual.strip(),
            textwrap.dedent(
                """
                CREATE EXTERNAL TABLE IF NOT EXISTS `test_schema`.`test_table` (
                `col_bigint` BIGINT,
                `col_float` FLOAT,
                `col_double` DOUBLE,
                `col_string` STRING,
                `col_boolean` BOOLEAN,
                `col_timestamp` TIMESTAMP,
                `col_date` DATE,
                `col_timedelta` BIGINT
                )
                PARTITIONED BY (
                `col_int` INT
                )
                STORED AS PARQUET
                LOCATION 's3://bucket/path/to/'
                """
            ).strip(),
        )

        # multiple partitions
        actual = generate_ddl(
            df,
            "test_table",
            "s3://bucket/path/to/",
            "test_schema",
            partitions=["col_int", "col_string"],
        )
        self.assertEqual(
            actual.strip(),
            textwrap.dedent(
                """
                CREATE EXTERNAL TABLE IF NOT EXISTS `test_schema`.`test_table` (
                `col_bigint` BIGINT,
                `col_float` FLOAT,
                `col_double` DOUBLE,
                `col_boolean` BOOLEAN,
                `col_timestamp` TIMESTAMP,
                `col_date` DATE,
                `col_timedelta` BIGINT
                )
                PARTITIONED BY (
                `col_int` INT,
                `col_string` STRING
                )
                STORED AS PARQUET
                LOCATION 's3://bucket/path/to/'
                """
            ).strip(),
        )

        # complex
        df = pd.DataFrame({"col_complex": np.complex_([1.0, 2.0, 3.0, 4.0, 5.0])})
        with self.assertRaises(ValueError):
            generate_ddl(df, "test_table", "s3://bucket/path/to/")

        # time
        df = pd.DataFrame(
            {"col_time": [datetime(2020, 1, 1, 0, 0, 0).time()]}, index=["i"]
        )
        with self.assertRaises(ValueError):
            generate_ddl(df, "test_table", "s3://bucket/path/to/")

    @with_cursor()
    def test_to_sql(self, cursor):
        # TODO Add binary column (After dropping support for Python 2.7)
        df = pd.DataFrame(
            {
                "col_int": np.int32([1]),
                "col_bigint": np.int64([12345]),
                "col_float": np.float32([1.0]),
                "col_double": np.float64([1.2345]),
                "col_string": ["a"],
                "col_boolean": np.bool_([True]),
                "col_timestamp": [datetime(2020, 1, 1, 0, 0, 0)],
                "col_date": [date(2020, 12, 31)],
            }
        )
        # Explicitly specify column order
        df = df[
            [
                "col_int",
                "col_bigint",
                "col_float",
                "col_double",
                "col_string",
                "col_boolean",
                "col_timestamp",
                "col_date",
            ]
        ]
        table_name = "to_sql_{0}".format(str(uuid.uuid4()).replace("-", ""))
        location = "{0}{1}/{2}/".format(ENV.s3_staging_dir, S3_PREFIX, table_name)
        to_sql(
            df,
            table_name,
            cursor._connection,
            location,
            schema=SCHEMA,
            if_exists="fail",
            compression="snappy",
        )
        # table already exists
        with self.assertRaises(OperationalError):
            to_sql(
                df,
                table_name,
                cursor._connection,
                location,
                schema=SCHEMA,
                if_exists="fail",
                compression="snappy",
            )
        # replace
        to_sql(
            df,
            table_name,
            cursor._connection,
            location,
            schema=SCHEMA,
            if_exists="replace",
            compression="snappy",
        )

        cursor.execute("SELECT * FROM {0}".format(table_name))
        self.assertEqual(
            cursor.fetchall(),
            [
                (
                    1,
                    12345,
                    1.0,
                    1.2345,
                    "a",
                    True,
                    datetime(2020, 1, 1, 0, 0, 0),
                    date(2020, 12, 31),
                )
            ],
        )
        self.assertEqual(
            [(d[0], d[1]) for d in cursor.description],
            [
                ("col_int", "integer"),
                ("col_bigint", "bigint"),
                ("col_float", "float"),
                ("col_double", "double"),
                ("col_string", "varchar"),
                ("col_boolean", "boolean"),
                ("col_timestamp", "timestamp"),
                ("col_date", "date"),
            ],
        )

        # append
        to_sql(
            df,
            table_name,
            cursor._connection,
            location,
            schema=SCHEMA,
            if_exists="append",
            compression="snappy",
        )
        cursor.execute("SELECT * FROM {0}".format(table_name))
        self.assertEqual(
            cursor.fetchall(),
            [
                (
                    1,
                    12345,
                    1.0,
                    1.2345,
                    "a",
                    True,
                    datetime(2020, 1, 1, 0, 0, 0),
                    date(2020, 12, 31),
                ),
                (
                    1,
                    12345,
                    1.0,
                    1.2345,
                    "a",
                    True,
                    datetime(2020, 1, 1, 0, 0, 0),
                    date(2020, 12, 31),
                ),
            ],
        )

    @with_cursor()
    def test_to_sql_with_index(self, cursor):
        df = pd.DataFrame({"col_int": np.int32([1])})
        table_name = "to_sql_{0}".format(str(uuid.uuid4()).replace("-", ""))
        location = "{0}{1}/{2}/".format(ENV.s3_staging_dir, S3_PREFIX, table_name)
        to_sql(
            df,
            table_name,
            cursor._connection,
            location,
            schema=SCHEMA,
            if_exists="fail",
            compression="snappy",
            index=True,
            index_label="col_index",
        )
        cursor.execute("SELECT * FROM {0}".format(table_name))
        self.assertEqual(cursor.fetchall(), [(0, 1)])
        self.assertEqual(
            [(d[0], d[1]) for d in cursor.description],
            [("col_index", "bigint"), ("col_int", "integer")],
        )

    @with_cursor()
    def test_to_sql_with_partitions(self, cursor):
        df = pd.DataFrame(
            {
                "col_int": np.int32([i for i in xrange(10)]),
                "col_bigint": np.int64([12345 for _ in xrange(10)]),
                "col_string": ["a" for _ in xrange(10)],
            }
        )
        table_name = "to_sql_{0}".format(str(uuid.uuid4()).replace("-", ""))
        location = "{0}{1}/{2}/".format(ENV.s3_staging_dir, S3_PREFIX, table_name)
        to_sql(
            df,
            table_name,
            cursor._connection,
            location,
            schema=SCHEMA,
            partitions=["col_int"],
            if_exists="fail",
            compression="snappy",
        )
        cursor.execute("SHOW PARTITIONS {0}".format(table_name))
        self.assertEqual(
            sorted(cursor.fetchall()), [("col_int={0}".format(i),) for i in xrange(10)]
        )
        cursor.execute("SELECT COUNT(*) FROM {0}".format(table_name))
        self.assertEqual(cursor.fetchall(), [(10,)])

    @with_cursor()
    def test_to_sql_with_multiple_partitions(self, cursor):
        df = pd.DataFrame(
            {
                "col_int": np.int32([i for i in xrange(10)]),
                "col_bigint": np.int64([12345 for _ in xrange(10)]),
                "col_string": ["a" for _ in xrange(5)] + ["b" for _ in xrange(5)],
            }
        )
        table_name = "to_sql_{0}".format(str(uuid.uuid4()).replace("-", ""))
        location = "{0}{1}/{2}/".format(ENV.s3_staging_dir, S3_PREFIX, table_name)
        to_sql(
            df,
            table_name,
            cursor._connection,
            location,
            schema=SCHEMA,
            partitions=["col_int", "col_string"],
            if_exists="fail",
            compression="snappy",
        )
        cursor.execute("SHOW PARTITIONS {0}".format(table_name))
        self.assertEqual(
            sorted(cursor.fetchall()),
            [("col_int={0}/col_string=a".format(i),) for i in xrange(5)]
            + [("col_int={0}/col_string=b".format(i),) for i in xrange(5, 10)],
        )
        cursor.execute("SELECT COUNT(*) FROM {0}".format(table_name))
        self.assertEqual(cursor.fetchall(), [(10,)])

    @with_cursor()
    def test_to_sql_invalid_args(self, cursor):
        df = pd.DataFrame({"col_int": np.int32([1])})
        table_name = "to_sql_{0}".format(str(uuid.uuid4()).replace("-", ""))
        location = "{0}{1}/{2}/".format(ENV.s3_staging_dir, S3_PREFIX, table_name)
        # invalid if_exists
        with self.assertRaises(ValueError):
            to_sql(
                df,
                table_name,
                cursor._connection,
                location,
                schema=SCHEMA,
                if_exists="foobar",
                compression="snappy",
            )
        # invalid compression
        with self.assertRaises(ValueError):
            to_sql(
                df,
                table_name,
                cursor._connection,
                location,
                schema=SCHEMA,
                if_exists="fail",
                compression="foobar",
            )
