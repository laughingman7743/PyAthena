# -*- coding: utf-8 -*-
import contextlib
import math
import random
import string
import time
from datetime import datetime
from random import randint

import numpy as np
import pandas as pd
import pytest

from pyathena.error import NotSupportedError, ProgrammingError
from pyathena.model import AthenaQueryExecution
from pyathena.pandas.async_cursor import AsyncPandasCursor
from pyathena.result_set import AthenaResultSet
from tests import ENV
from tests.conftest import connect


class TestAsyncPandasCursor:
    @pytest.mark.parametrize(
        "async_pandas_cursor",
        [{"cursor_kwargs": {"unload": False}}, {"cursor_kwargs": {"unload": True}}],
        indirect=True,
    )
    def test_fetchone(self, async_pandas_cursor):
        query_id, future = async_pandas_cursor.execute("SELECT * FROM one_row")
        result_set = future.result()
        assert result_set.rownumber == 0
        assert result_set.fetchone() == (1,)
        assert result_set.rownumber == 1
        assert result_set.fetchone() is None
        assert result_set.query_id
        assert result_set.query
        assert result_set.state == AthenaQueryExecution.STATE_SUCCEEDED
        assert result_set.state_change_reason is None
        assert result_set.completion_date_time
        assert isinstance(result_set.completion_date_time, datetime)
        assert result_set.submission_date_time
        assert isinstance(result_set.submission_date_time, datetime)
        assert result_set.data_scanned_in_bytes
        assert result_set.engine_execution_time_in_millis
        assert result_set.query_queue_time_in_millis
        assert result_set.total_execution_time_in_millis
        # assert result_set.query_planning_time_in_millis  # TODO flaky test
        # assert result_set.service_processing_time_in_millis  # TODO flaky test
        assert result_set.output_location
        if async_pandas_cursor._unload:
            assert result_set.data_manifest_location
        else:
            assert result_set.data_manifest_location is None
        assert result_set.encryption_option is None
        assert result_set.kms_key is None

    @pytest.mark.parametrize(
        "async_pandas_cursor",
        [{"cursor_kwargs": {"unload": False}}, {"cursor_kwargs": {"unload": True}}],
        indirect=True,
    )
    def test_fetchmany(self, async_pandas_cursor):
        query_id, future = async_pandas_cursor.execute(
            "SELECT * FROM many_rows LIMIT 15"
        )
        result_set = future.result()
        assert len(result_set.fetchmany(10)) == 10
        assert len(result_set.fetchmany(10)) == 5

    @pytest.mark.parametrize(
        "async_pandas_cursor",
        [{"cursor_kwargs": {"unload": False}}, {"cursor_kwargs": {"unload": True}}],
        indirect=True,
    )
    def test_fetchall(self, async_pandas_cursor):
        query_id, future = async_pandas_cursor.execute("SELECT * FROM one_row")
        result_set = future.result()
        assert result_set.fetchall() == [(1,)]
        query_id, future = async_pandas_cursor.execute(
            "SELECT a FROM many_rows ORDER BY a"
        )
        result_set = future.result()
        assert result_set.fetchall() == [(i,) for i in range(10000)]

    @pytest.mark.parametrize(
        "async_pandas_cursor",
        [{"cursor_kwargs": {"unload": False}}, {"cursor_kwargs": {"unload": True}}],
        indirect=True,
    )
    def test_iterator(self, async_pandas_cursor):
        query_id, future = async_pandas_cursor.execute("SELECT * FROM one_row")
        result_set = future.result()
        assert list(result_set) == [(1,)]
        pytest.raises(StopIteration, result_set.__next__)

    @pytest.mark.parametrize(
        "async_pandas_cursor",
        [{"cursor_kwargs": {"unload": False}}, {"cursor_kwargs": {"unload": True}}],
        indirect=True,
    )
    def test_arraysize(self, async_pandas_cursor):
        async_pandas_cursor.arraysize = 5
        query_id, future = async_pandas_cursor.execute(
            "SELECT * FROM many_rows LIMIT 20"
        )
        result_set = future.result()
        assert len(result_set.fetchmany()) == 5

    def test_arraysize_default(self, async_pandas_cursor):
        assert async_pandas_cursor.arraysize == AthenaResultSet.DEFAULT_FETCH_SIZE

    def test_invalid_arraysize(self, async_pandas_cursor):
        async_pandas_cursor.arraysize = 10000
        assert async_pandas_cursor.arraysize == 10000
        with pytest.raises(ProgrammingError):
            async_pandas_cursor.arraysize = -1

    @pytest.mark.parametrize(
        "async_pandas_cursor",
        [{"cursor_kwargs": {"unload": False}}, {"cursor_kwargs": {"unload": True}}],
        indirect=True,
    )
    def test_description(self, async_pandas_cursor):
        query_id, future = async_pandas_cursor.execute(
            "SELECT 1 AS foobar FROM one_row"
        )
        result_set = future.result()
        assert result_set.fetchall() == [(1,)]
        if async_pandas_cursor._unload:
            assert result_set.description == [
                ("foobar", "integer", None, None, 10, 0, "NULLABLE")
            ]
        else:
            assert result_set.description == [
                ("foobar", "integer", None, None, 10, 0, "UNKNOWN")
            ]

        future = async_pandas_cursor.description(query_id)
        description = future.result()
        assert result_set.description == description

    @pytest.mark.parametrize(
        "async_pandas_cursor",
        [{"cursor_kwargs": {"unload": False}}, {"cursor_kwargs": {"unload": True}}],
        indirect=True,
    )
    def test_query_execution(self, async_pandas_cursor):
        query = "SELECT * FROM one_row"
        query_id, future = async_pandas_cursor.execute(query)
        result_set = future.result()

        future = async_pandas_cursor.query_execution(query_id)
        query_execution = future.result()

        assert query_execution.query_id
        if async_pandas_cursor._unload:
            assert query_execution.query.startswith("UNLOAD")
            assert query in query_execution.query
        else:
            assert query_execution.query == query
        assert query_execution.state == AthenaQueryExecution.STATE_SUCCEEDED
        assert query_execution.state_change_reason is None
        assert query_execution.completion_date_time
        assert isinstance(query_execution.completion_date_time, datetime)
        assert query_execution.submission_date_time
        assert isinstance(query_execution.submission_date_time, datetime)
        assert query_execution.data_scanned_in_bytes
        assert query_execution.engine_execution_time_in_millis
        assert query_execution.query_queue_time_in_millis
        assert query_execution.total_execution_time_in_millis
        # assert query_execution.query_planning_time_in_millis  # TODO flaky test
        # assert query_execution.service_processing_time_in_millis  # TODO flaky test
        assert query_execution.output_location

        assert result_set.query_id == query_execution.query_id
        assert result_set.query == query_execution.query
        assert result_set.state == query_execution.state
        assert result_set.state_change_reason == query_execution.state_change_reason
        assert result_set.completion_date_time == query_execution.completion_date_time
        assert result_set.submission_date_time == query_execution.submission_date_time
        assert result_set.data_scanned_in_bytes == query_execution.data_scanned_in_bytes
        assert (
            result_set.engine_execution_time_in_millis
            == query_execution.engine_execution_time_in_millis
        )
        assert (
            result_set.query_queue_time_in_millis
            == query_execution.query_queue_time_in_millis
        )
        assert (
            result_set.total_execution_time_in_millis
            == query_execution.total_execution_time_in_millis
        )
        assert (
            result_set.query_planning_time_in_millis
            == query_execution.query_planning_time_in_millis
        )
        assert (
            result_set.service_processing_time_in_millis
            == query_execution.service_processing_time_in_millis
        )
        assert result_set.output_location == query_execution.output_location
        assert (
            result_set.data_manifest_location == query_execution.data_manifest_location
        )

    @pytest.mark.parametrize(
        "async_pandas_cursor",
        [{"cursor_kwargs": {"unload": False}}, {"cursor_kwargs": {"unload": True}}],
        indirect=True,
    )
    def test_poll(self, async_pandas_cursor):
        query_id, _ = async_pandas_cursor.execute("SELECT * FROM one_row")
        future = async_pandas_cursor.poll(query_id)
        query_execution = future.result()
        assert query_execution.state in [
            AthenaQueryExecution.STATE_QUEUED,
            AthenaQueryExecution.STATE_RUNNING,
            AthenaQueryExecution.STATE_SUCCEEDED,
            AthenaQueryExecution.STATE_FAILED,
            AthenaQueryExecution.STATE_CANCELLED,
        ]

    @pytest.mark.parametrize(
        "async_pandas_cursor",
        [{"cursor_kwargs": {"unload": False}}, {"cursor_kwargs": {"unload": True}}],
        indirect=True,
    )
    def test_bad_query(self, async_pandas_cursor):
        query_id, future = async_pandas_cursor.execute(
            "SELECT does_not_exist FROM this_really_does_not_exist"
        )
        result_set = future.result()
        assert result_set.state == AthenaQueryExecution.STATE_FAILED
        assert result_set.state_change_reason is not None

    @pytest.mark.parametrize(
        "async_pandas_cursor",
        [{"cursor_kwargs": {"unload": False}}, {"cursor_kwargs": {"unload": True}}],
        indirect=True,
    )
    def test_as_pandas(self, async_pandas_cursor):
        query_id, future = async_pandas_cursor.execute("SELECT * FROM one_row")
        df = future.result().as_pandas()
        assert df.shape[0] == 1
        assert df.shape[1] == 1
        assert [(row["number_of_rows"],) for _, row in df.iterrows()] == [(1,)]

    @pytest.mark.parametrize(
        "async_pandas_cursor",
        [{"cursor_kwargs": {"unload": False}}, {"cursor_kwargs": {"unload": True}}],
        indirect=True,
    )
    def test_many_as_pandas(self, async_pandas_cursor):
        query_id, future = async_pandas_cursor.execute("SELECT * FROM many_rows")
        df = future.result().as_pandas()
        assert df.shape[0] == 10000
        assert df.shape[1] == 1
        assert [(row["a"],) for _, row in df.iterrows()] == [(i,) for i in range(10000)]

    def test_cancel(self, async_pandas_cursor):
        query_id, future = async_pandas_cursor.execute(
            """
            SELECT a.a * rand(), b.a * rand()
            FROM many_rows a
            CROSS JOIN many_rows b
            """
        )
        time.sleep(randint(5, 10))
        async_pandas_cursor.cancel(query_id)
        result_set = future.result()
        assert result_set.state == AthenaQueryExecution.STATE_CANCELLED
        # assert result_set.state_change_reason  # TODO flaky test
        assert result_set.description is None
        assert result_set.fetchone() is None
        assert result_set.fetchmany() == []
        assert result_set.fetchall() == []

    def test_open_close(self):
        with contextlib.closing(connect()) as conn:
            with conn.cursor(AsyncPandasCursor):
                pass

    def test_no_ops(self):
        conn = connect()
        cursor = conn.cursor(AsyncPandasCursor)
        pytest.raises(
            NotSupportedError, lambda: cursor.executemany("SELECT * FROM one_row", [])
        )
        cursor.close()
        conn.close()

    @pytest.mark.parametrize(
        "async_pandas_cursor",
        [{"cursor_kwargs": {"unload": False}}, {"cursor_kwargs": {"unload": True}}],
        indirect=True,
    )
    def test_empty_result(self, async_pandas_cursor):
        table = "test_pandas_cursor_empty_result_" + "".join(
            [random.choice(string.ascii_lowercase + string.digits) for _ in range(10)]
        )
        query_id, future = async_pandas_cursor.execute(
            f"""
            CREATE EXTERNAL TABLE IF NOT EXISTS
            {ENV.schema}.{table} (number_of_rows INT)
            ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
            LINES TERMINATED BY '\n' STORED AS TEXTFILE
            LOCATION '{ENV.s3_staging_dir}{ENV.schema}/{table}/'
            """
        )
        df = future.result().as_pandas()
        assert df.shape[0] == 0
        assert df.shape[1] == 0

    @pytest.mark.parametrize(
        "async_pandas_cursor",
        [
            {
                "cursor_kwargs": {"unload": True},
            },
        ],
        indirect=True,
    )
    def test_empty_result_unload(self, async_pandas_cursor):
        query_id, future = async_pandas_cursor.execute(
            """
            SELECT * FROM one_row LIMIT 0
            """
        )
        df = future.result().as_pandas()
        assert df.shape[0] == 0
        assert df.shape[1] == 0

    @pytest.mark.parametrize(
        "async_pandas_cursor",
        [{"cursor_kwargs": {"unload": False}}, {"cursor_kwargs": {"unload": True}}],
        indirect=True,
    )
    def test_integer_na_values(self, async_pandas_cursor):
        query_id, future = async_pandas_cursor.execute(
            """
            SELECT * FROM integer_na_values
            """
        )
        df = future.result().as_pandas()
        if async_pandas_cursor._unload:
            rows = [
                tuple(
                    [
                        True if math.isnan(row["a"]) else row["a"],
                        True if math.isnan(row["b"]) else row["b"],
                    ]
                )
                for _, row in df.iterrows()
            ]
            # If the UNLOAD option is enabled, it is converted to float for some reason.
            assert rows == [(1.0, 2.0), (1.0, True), (True, True)]
        else:
            rows = [tuple([row["a"], row["b"]]) for _, row in df.iterrows()]
            assert rows == [(1, 2), (1, pd.NA), (pd.NA, pd.NA)]

    @pytest.mark.parametrize(
        "async_pandas_cursor",
        [{"cursor_kwargs": {"unload": False}}, {"cursor_kwargs": {"unload": True}}],
        indirect=True,
    )
    def test_float_na_values(self, async_pandas_cursor):
        query_id, future = async_pandas_cursor.execute(
            """
            SELECT * FROM (VALUES (0.33), (NULL)) AS t (col)
            """
        )
        df = future.result().as_pandas()
        rows = [tuple([row[0]]) for _, row in df.iterrows()]
        np.testing.assert_equal(rows, [(0.33,), (np.nan,)])

    @pytest.mark.parametrize(
        "async_pandas_cursor",
        [{"cursor_kwargs": {"unload": False}}, {"cursor_kwargs": {"unload": True}}],
        indirect=True,
    )
    def test_boolean_na_values(self, async_pandas_cursor):
        query_id, future = async_pandas_cursor.execute(
            """
            SELECT * FROM boolean_na_values
            """
        )
        df = future.result().as_pandas()
        rows = [tuple([row["a"], row["b"]]) for _, row in df.iterrows()]
        assert rows == [(True, False), (False, None), (None, None)]

    @pytest.mark.parametrize(
        "async_pandas_cursor",
        [{"cursor_kwargs": {"unload": False}}, {"cursor_kwargs": {"unload": True}}],
        indirect=True,
    )
    def test_not_skip_blank_lines(self, async_pandas_cursor):
        query_id, future = async_pandas_cursor.execute(
            """
            SELECT col FROM (VALUES (1), (NULL)) AS t (col)
            """
        )
        result_set = future.result()
        assert len(result_set.fetchall()) == 2

    @pytest.mark.parametrize(
        "async_pandas_cursor",
        [{"cursor_kwargs": {"unload": False}}, {"cursor_kwargs": {"unload": True}}],
        indirect=True,
    )
    def test_empty_and_null_string(self, async_pandas_cursor):
        # TODO https://github.com/laughingman7743/PyAthena/issues/118
        query = """
        SELECT * FROM (VALUES ('', 'a'), ('N/A', 'a'), ('NULL', 'a'), (NULL, 'a'))
        AS t (col1, col2)
        """
        query_id, future = async_pandas_cursor.execute(query)
        result_set = future.result()
        if async_pandas_cursor._unload:
            # NULL and empty characters are correctly converted when the UNLOAD option is enabled.
            np.testing.assert_equal(
                result_set.fetchall(),
                [("", "a"), ("N/A", "a"), ("NULL", "a"), (None, "a")],
            )
        else:
            np.testing.assert_equal(
                result_set.fetchall(),
                [(np.nan, "a"), ("N/A", "a"), ("NULL", "a"), (np.nan, "a")],
            )
        query_id, future = async_pandas_cursor.execute(query, na_values=None)
        result_set = future.result()
        if async_pandas_cursor._unload:
            # NULL and empty characters are correctly converted when the UNLOAD option is enabled.
            assert result_set.fetchall() == [
                ("", "a"),
                ("N/A", "a"),
                ("NULL", "a"),
                (None, "a"),
            ]
        else:
            assert result_set.fetchall() == [
                ("", "a"),
                ("N/A", "a"),
                ("NULL", "a"),
                ("", "a"),
            ]

    @pytest.mark.parametrize(
        "async_pandas_cursor",
        [{"cursor_kwargs": {"unload": False}}, {"cursor_kwargs": {"unload": True}}],
        indirect=True,
    )
    def test_null_decimal_value(self, async_pandas_cursor):
        query_id, future = async_pandas_cursor.execute(
            "SELECT CAST(null AS DECIMAL) AS col_decimal"
        )
        result_set = future.result()
        assert result_set.fetchall() == [(None,)]
