# -*- coding: utf-8 -*-
import contextlib
import random
import string
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from decimal import Decimal

import numpy as np
import pandas as pd
import pytest

from pyathena.error import DatabaseError, ProgrammingError
from pyathena.model import AthenaQueryExecution
from pyathena.pandas.cursor import PandasCursor
from pyathena.pandas.result_set import AthenaPandasResultSet
from tests import ENV
from tests.conftest import connect


class TestPandasCursor:
    def test_fetchone(self, pandas_cursor):
        pandas_cursor.execute("SELECT * FROM one_row")
        assert pandas_cursor.rownumber == 0
        assert pandas_cursor.fetchone() == (1,)
        assert pandas_cursor.rownumber == 1
        assert pandas_cursor.fetchone() is None

    def test_fetchmany(self, pandas_cursor):
        pandas_cursor.execute("SELECT * FROM many_rows LIMIT 15")
        assert len(pandas_cursor.fetchmany(10)) == 10
        assert len(pandas_cursor.fetchmany(10)) == 5

    def test_fetchall(self, pandas_cursor):
        pandas_cursor.execute("SELECT * FROM one_row")
        assert pandas_cursor.fetchall() == [(1,)]
        pandas_cursor.execute("SELECT a FROM many_rows ORDER BY a")
        assert pandas_cursor.fetchall() == [(i,) for i in range(10000)]

    def test_iterator(self, pandas_cursor):
        pandas_cursor.execute("SELECT * FROM one_row")
        assert list(pandas_cursor) == [(1,)]
        pytest.raises(StopIteration, pandas_cursor.__next__)

    def test_arraysize(self, pandas_cursor):
        pandas_cursor.arraysize = 5
        pandas_cursor.execute("SELECT * FROM many_rows LIMIT 20")
        assert len(pandas_cursor.fetchmany()) == 5

    def test_arraysize_default(self, pandas_cursor):
        assert pandas_cursor.arraysize == AthenaPandasResultSet.DEFAULT_FETCH_SIZE

    def test_invalid_arraysize(self, pandas_cursor):
        pandas_cursor.arraysize = 10000
        assert pandas_cursor.arraysize == 10000
        with pytest.raises(ProgrammingError):
            pandas_cursor.arraysize = -1

    def test_complex(self, pandas_cursor):
        pandas_cursor.execute(
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
              ,col_varchar
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
        assert pandas_cursor.description == [
            ("col_boolean", "boolean", None, None, 0, 0, "UNKNOWN"),
            ("col_tinyint", "tinyint", None, None, 3, 0, "UNKNOWN"),
            ("col_smallint", "smallint", None, None, 5, 0, "UNKNOWN"),
            ("col_int", "integer", None, None, 10, 0, "UNKNOWN"),
            ("col_bigint", "bigint", None, None, 19, 0, "UNKNOWN"),
            ("col_float", "float", None, None, 17, 0, "UNKNOWN"),
            ("col_double", "double", None, None, 17, 0, "UNKNOWN"),
            ("col_string", "varchar", None, None, 2147483647, 0, "UNKNOWN"),
            ("col_varchar", "varchar", None, None, 10, 0, "UNKNOWN"),
            ("col_timestamp", "timestamp", None, None, 3, 0, "UNKNOWN"),
            ("col_time", "time", None, None, 3, 0, "UNKNOWN"),
            ("col_date", "date", None, None, 0, 0, "UNKNOWN"),
            ("col_binary", "varbinary", None, None, 1073741824, 0, "UNKNOWN"),
            ("col_array", "array", None, None, 0, 0, "UNKNOWN"),
            ("col_array_json", "json", None, None, 0, 0, "UNKNOWN"),
            ("col_map", "map", None, None, 0, 0, "UNKNOWN"),
            ("col_map_json", "json", None, None, 0, 0, "UNKNOWN"),
            ("col_struct", "row", None, None, 0, 0, "UNKNOWN"),
            ("col_decimal", "decimal", None, None, 10, 1, "UNKNOWN"),
        ]
        rows = pandas_cursor.fetchall()
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
                "varchar",
                pd.Timestamp(2017, 1, 1, 0, 0, 0),
                datetime(2017, 1, 1, 0, 0, 0).time(),
                pd.Timestamp(2017, 1, 2),
                b"123",
                "[1, 2]",
                [1, 2],
                "{1=2, 3=4}",
                {"1": 2, "3": 4},
                "{a=1, b=2}",
                Decimal("0.1"),
            )
        ]
        assert rows == expected

    def test_fetch_no_data(self, pandas_cursor):
        pytest.raises(ProgrammingError, pandas_cursor.fetchone)
        pytest.raises(ProgrammingError, pandas_cursor.fetchmany)
        pytest.raises(ProgrammingError, pandas_cursor.fetchall)
        pytest.raises(ProgrammingError, pandas_cursor.as_pandas)

    def test_as_pandas(self, pandas_cursor):
        df = pandas_cursor.execute("SELECT * FROM one_row").as_pandas()
        assert df.shape[0] == 1
        assert df.shape[1] == 1
        assert [(row["number_of_rows"],) for _, row in df.iterrows()] == [(1,)]
        assert pandas_cursor.query_id
        assert pandas_cursor.query
        assert pandas_cursor.state == AthenaQueryExecution.STATE_SUCCEEDED
        assert pandas_cursor.state_change_reason is None
        assert pandas_cursor.completion_date_time
        assert isinstance(pandas_cursor.completion_date_time, datetime)
        assert pandas_cursor.submission_date_time
        assert isinstance(pandas_cursor.submission_date_time, datetime)
        assert pandas_cursor.data_scanned_in_bytes
        assert pandas_cursor.engine_execution_time_in_millis
        assert pandas_cursor.query_queue_time_in_millis
        assert pandas_cursor.total_execution_time_in_millis
        # assert pandas_cursor.query_planning_time_in_millis  # TODO flaky test
        # assert pandas_cursor.service_processing_time_in_millis  # TODO flaky test
        assert pandas_cursor.output_location
        assert pandas_cursor.data_manifest_location is None
        assert pandas_cursor.encryption_option is None
        assert pandas_cursor.kms_key is None

    def test_many_as_pandas(self, pandas_cursor):
        df = pandas_cursor.execute("SELECT * FROM many_rows").as_pandas()
        assert df.shape[0] == 10000
        assert df.shape[1] == 1
        assert [(row["a"],) for _, row in df.iterrows()] == [(i,) for i in range(10000)]

    def test_complex_as_pandas(self, pandas_cursor):
        df = pandas_cursor.execute(
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
              ,col_varchar
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
        ).as_pandas()
        assert df.shape[0] == 1
        assert df.shape[1] == 19
        dtypes = tuple(
            [
                df["col_boolean"].dtype.type,
                df["col_tinyint"].dtype.type,
                df["col_smallint"].dtype.type,
                df["col_int"].dtype.type,
                df["col_bigint"].dtype.type,
                df["col_float"].dtype.type,
                df["col_double"].dtype.type,
                df["col_string"].dtype.type,
                df["col_varchar"].dtype.type,
                df["col_timestamp"].dtype.type,
                df["col_time"].dtype.type,
                df["col_date"].dtype.type,
                df["col_binary"].dtype.type,
                df["col_array"].dtype.type,
                df["col_array_json"].dtype.type,
                df["col_map"].dtype.type,
                df["col_map_json"].dtype.type,
                df["col_struct"].dtype.type,
                df["col_decimal"].dtype.type,
            ]
        )
        assert dtypes == tuple(
            [
                np.bool_,
                np.int64,
                np.int64,
                np.int64,
                np.int64,
                np.float64,
                np.float64,
                np.object_,
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
            ]
        )
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
                    row["col_varchar"],
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
        assert rows == [
            (
                True,
                127,
                32767,
                2147483647,
                9223372036854775807,
                0.5,
                0.25,
                "a string",
                "varchar",
                pd.Timestamp(2017, 1, 1, 0, 0, 0),
                datetime(2017, 1, 1, 0, 0, 0).time(),
                pd.Timestamp(2017, 1, 2),
                b"123",
                "[1, 2]",
                [1, 2],
                "{1=2, 3=4}",
                {"1": 2, "3": 4},
                "{a=1, b=2}",
                Decimal("0.1"),
            )
        ]

    def test_cancel(self, pandas_cursor):
        def cancel(c):
            time.sleep(random.randint(5, 10))
            c.cancel()

        with ThreadPoolExecutor(max_workers=1) as executor:
            executor.submit(cancel, pandas_cursor)

            pytest.raises(
                DatabaseError,
                lambda: pandas_cursor.execute(
                    """
                    SELECT a.a * rand(), b.a * rand()
                    FROM many_rows a
                    CROSS JOIN many_rows b
                    """
                ),
            )

    def test_cancel_initial(self, pandas_cursor):
        pytest.raises(ProgrammingError, pandas_cursor.cancel)

    def test_open_close(self):
        with contextlib.closing(connect()) as conn:
            with conn.cursor(PandasCursor):
                pass

    def test_no_ops(self):
        conn = connect()
        cursor = conn.cursor(PandasCursor)
        cursor.close()
        conn.close()

    def test_show_columns(self, pandas_cursor):
        pandas_cursor.execute("SHOW COLUMNS IN one_row")
        assert pandas_cursor.description == [
            ("field", "string", None, None, 0, 0, "UNKNOWN")
        ]
        assert pandas_cursor.fetchall() == [("number_of_rows      ",)]

    def test_empty_result(self, pandas_cursor):
        table = "test_pandas_cursor_empty_result_" + "".join(
            [random.choice(string.ascii_lowercase + string.digits) for _ in range(10)]
        )
        location = "{0}{1}/{2}/".format(ENV.s3_staging_dir, ENV.schema, table)
        df = pandas_cursor.execute(
            """
            CREATE EXTERNAL TABLE IF NOT EXISTS
            {schema}.{table} (number_of_rows INT)
            ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
            LINES TERMINATED BY '\n' STORED AS TEXTFILE
            LOCATION '{location}'
            """.format(
                schema=ENV.schema, table=table, location=location
            )
        ).as_pandas()
        assert df.shape[0] == 0
        assert df.shape[1] == 0

    def test_integer_na_values(self, pandas_cursor):
        df = pandas_cursor.execute(
            """
            SELECT * FROM integer_na_values
            """
        ).as_pandas()
        rows = [tuple([row["a"], row["b"]]) for _, row in df.iterrows()]
        assert rows == [(1, 2), (1, pd.NA), (pd.NA, pd.NA)]

    def test_float_na_values(self, pandas_cursor):
        df = pandas_cursor.execute(
            """
            SELECT * FROM (VALUES (0.33), (NULL))
            """
        ).as_pandas()
        rows = [tuple([row[0]]) for _, row in df.iterrows()]
        np.testing.assert_equal(rows, [(0.33,), (np.nan,)])

    def test_boolean_na_values(self, pandas_cursor):
        df = pandas_cursor.execute(
            """
            SELECT * FROM boolean_na_values
            """
        ).as_pandas()
        rows = [tuple([row["a"], row["b"]]) for _, row in df.iterrows()]
        assert rows == [(True, False), (False, None), (None, None)]

    def test_executemany(self, pandas_cursor):
        rows = [(1, "foo"), (2, "bar"), (3, "jim o'rourke")]
        pandas_cursor.executemany(
            "INSERT INTO execute_many_pandas (a, b) VALUES (%(a)d, %(b)s)",
            [{"a": a, "b": b} for a, b in rows],
        )
        pandas_cursor.execute("SELECT * FROM execute_many_pandas")
        assert sorted(pandas_cursor.fetchall()) == [(a, b) for a, b in rows]

    def test_executemany_fetch(self, pandas_cursor):
        pandas_cursor.executemany(
            "SELECT %(x)d FROM one_row", [{"x": i} for i in range(1, 2)]
        )
        # Operations that have result sets are not allowed with executemany.
        pytest.raises(ProgrammingError, pandas_cursor.fetchall)
        pytest.raises(ProgrammingError, pandas_cursor.fetchmany)
        pytest.raises(ProgrammingError, pandas_cursor.fetchone)
        pytest.raises(ProgrammingError, pandas_cursor.as_pandas)

    def test_not_skip_blank_lines(self, pandas_cursor):
        pandas_cursor.execute(
            """
            SELECT * FROM (VALUES (1), (NULL))
            """
        )
        assert len(pandas_cursor.fetchall()) == 2

    def test_empty_and_null_string(self, pandas_cursor):
        # TODO https://github.com/laughingman7743/PyAthena/issues/118
        query = """
        SELECT * FROM (VALUES ('', 'a'), ('N/A', 'a'), ('NULL', 'a'), (NULL, 'a'))
        """
        pandas_cursor.execute(query)
        np.testing.assert_equal(
            pandas_cursor.fetchall(),
            [(np.nan, "a"), ("N/A", "a"), ("NULL", "a"), (np.nan, "a")],
        )
        pandas_cursor.execute(query, na_values=None)
        assert pandas_cursor.fetchall() == [
            ("", "a"),
            ("N/A", "a"),
            ("NULL", "a"),
            ("", "a"),
        ]

    def test_null_decimal_value(self, pandas_cursor):
        pandas_cursor.execute("SELECT CAST(null AS DECIMAL)")
        assert pandas_cursor.fetchall() == [(None,)]
