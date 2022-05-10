# -*- coding: utf-8 -*-
import contextlib
import random
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from decimal import Decimal

import pytest

from pyathena.arrow.cursor import ArrowCursor
from pyathena.arrow.result_set import AthenaArrowResultSet
from pyathena.error import DatabaseError, ProgrammingError
from tests.conftest import connect


class TestArrowCursor:
    @pytest.mark.parametrize(
        "arrow_cursor",
        [{"cursor_kwargs": {"unload": False}}, {"cursor_kwargs": {"unload": True}}],
        indirect=True,
    )
    def test_fetchone(self, arrow_cursor):
        arrow_cursor.execute("SELECT * FROM one_row")
        assert arrow_cursor.rownumber == 0
        assert arrow_cursor.fetchone() == (1,)
        assert arrow_cursor.rownumber == 1
        assert arrow_cursor.fetchone() is None

    @pytest.mark.parametrize(
        "arrow_cursor",
        [{"cursor_kwargs": {"unload": False}}, {"cursor_kwargs": {"unload": True}}],
        indirect=True,
    )
    def test_fetchmany(self, arrow_cursor):
        arrow_cursor.execute("SELECT * FROM many_rows LIMIT 15")
        assert len(arrow_cursor.fetchmany(10)) == 10
        assert len(arrow_cursor.fetchmany(10)) == 5

    @pytest.mark.parametrize(
        "arrow_cursor",
        [{"cursor_kwargs": {"unload": False}}, {"cursor_kwargs": {"unload": True}}],
        indirect=True,
    )
    def test_fetchall(self, arrow_cursor):
        arrow_cursor.execute("SELECT * FROM one_row")
        assert arrow_cursor.fetchall() == [(1,)]
        arrow_cursor.execute("SELECT a FROM many_rows ORDER BY a")
        assert arrow_cursor.fetchall() == [(i,) for i in range(10000)]

    @pytest.mark.parametrize(
        "arrow_cursor",
        [{"cursor_kwargs": {"unload": False}}, {"cursor_kwargs": {"unload": True}}],
        indirect=True,
    )
    def test_iterator(self, arrow_cursor):
        arrow_cursor.execute("SELECT * FROM one_row")
        assert list(arrow_cursor) == [(1,)]
        pytest.raises(StopIteration, arrow_cursor.__next__)

    @pytest.mark.parametrize(
        "arrow_cursor",
        [{"cursor_kwargs": {"unload": False}}, {"cursor_kwargs": {"unload": True}}],
        indirect=True,
    )
    def test_arraysize(self, arrow_cursor):
        arrow_cursor.arraysize = 5
        arrow_cursor.execute("SELECT * FROM many_rows LIMIT 20")
        assert len(arrow_cursor.fetchmany()) == 5

    def test_arraysize_default(self, arrow_cursor):
        assert arrow_cursor.arraysize == AthenaArrowResultSet.DEFAULT_FETCH_SIZE

    def test_invalid_arraysize(self, arrow_cursor):
        arrow_cursor.arraysize = 10000
        assert arrow_cursor.arraysize == 10000
        with pytest.raises(ProgrammingError):
            arrow_cursor.arraysize = -1

    def test_complex(self, arrow_cursor):
        arrow_cursor.execute(
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
        assert arrow_cursor.description == [
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
        rows = arrow_cursor.fetchall()
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
                datetime(2017, 1, 1, 0, 0, 0),
                datetime(2017, 1, 1, 0, 0, 0).time(),
                datetime(2017, 1, 2).date(),
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

    @pytest.mark.parametrize(
        "arrow_cursor",
        [
            {
                "cursor_kwargs": {"unload": True},
            },
        ],
        indirect=True,
    )
    def test_complex_unload(self, arrow_cursor):
        # NOT_SUPPORTED: Unsupported Hive type: time
        # NOT_SUPPORTED: Unsupported Hive type: json
        arrow_cursor.execute(
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
              ,col_date
              ,col_binary
              ,col_array
              ,col_map
              ,col_struct
              ,col_decimal
            FROM one_row_complex
            """
        )
        assert arrow_cursor.description == [
            ("rows", "bigint", None, None, 19, 0, "UNKNOWN"),
        ]
        rows = arrow_cursor.fetchall()
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
                datetime(2017, 1, 1, 0, 0, 0),
                datetime(2017, 1, 2).date(),
                b"123",
                "[1, 2]",
                "{1=2, 3=4}",
                "{a=1, b=2}",
                Decimal("0.1"),
            )
        ]
        assert rows == expected

    def test_fetch_no_data(self, arrow_cursor):
        pytest.raises(ProgrammingError, arrow_cursor.fetchone)
        pytest.raises(ProgrammingError, arrow_cursor.fetchmany)
        pytest.raises(ProgrammingError, arrow_cursor.fetchall)
        pytest.raises(ProgrammingError, arrow_cursor.as_arrow)

    @pytest.mark.parametrize(
        "arrow_cursor",
        [{"cursor_kwargs": {"unload": False}}, {"cursor_kwargs": {"unload": True}}],
        indirect=True,
    )
    def test_as_arrow(self, arrow_cursor):
        # TODO
        pass

    @pytest.mark.parametrize(
        "arrow_cursor",
        [{"cursor_kwargs": {"unload": False}}, {"cursor_kwargs": {"unload": True}}],
        indirect=True,
    )
    def test_many_as_arrow(self, arrow_cursor):
        # TODO
        pass

    @pytest.mark.parametrize(
        "arrow_cursor",
        [{"cursor_kwargs": {"unload": False}}, {"cursor_kwargs": {"unload": True}}],
        indirect=True,
    )
    def test_complex_as_arrow(self, arrow_cursor):
        # TODO
        pass

    def test_cancel(self, arrow_cursor):
        def cancel(c):
            time.sleep(random.randint(5, 10))
            c.cancel()

        with ThreadPoolExecutor(max_workers=1) as executor:
            executor.submit(cancel, arrow_cursor)

            pytest.raises(
                DatabaseError,
                lambda: arrow_cursor.execute(
                    """
                    SELECT a.a * rand(), b.a * rand()
                    FROM many_rows a
                    CROSS JOIN many_rows b
                    """
                ),
            )

    def test_cancel_initial(self, arrow_cursor):
        pytest.raises(ProgrammingError, arrow_cursor.cancel)

    def test_open_close(self):
        with contextlib.closing(connect()) as conn:
            with conn.cursor(ArrowCursor):
                pass

    def test_no_ops(self):
        conn = connect()
        cursor = conn.cursor(ArrowCursor)
        cursor.close()
        conn.close()

    @pytest.mark.parametrize(
        "arrow_cursor",
        [{"cursor_kwargs": {"unload": False}}, {"cursor_kwargs": {"unload": True}}],
        indirect=True,
    )
    def test_show_columns(self, arrow_cursor):
        arrow_cursor.execute("SHOW COLUMNS IN one_row")
        assert arrow_cursor.fetchall() == [("number_of_rows      ",)]

    @pytest.mark.parametrize(
        "arrow_cursor",
        [{"cursor_kwargs": {"unload": False}}, {"cursor_kwargs": {"unload": True}}],
        indirect=True,
    )
    def test_empty_result(self, arrow_cursor):
        # TODO
        pass

    @pytest.mark.parametrize(
        "arrow_cursor",
        [{"cursor_kwargs": {"unload": False}}, {"cursor_kwargs": {"unload": True}}],
        indirect=True,
    )
    def test_executemany(self, arrow_cursor):
        rows = [(1, "foo"), (2, "bar"), (3, "jim o'rourke")]
        arrow_cursor.executemany(
            "INSERT INTO execute_many_arrow (a, b) VALUES (%(a)d, %(b)s)",
            [
                {"a": a, "b": b}
                for a, b in [(1, "foo"), (2, "bar"), (3, "jim o'rourke")]
            ],
        )
        arrow_cursor.execute("SELECT * FROM execute_many_arrow")
        assert sorted(arrow_cursor.fetchall()) == [(a, b) for a, b in rows]

    @pytest.mark.parametrize(
        "arrow_cursor",
        [{"cursor_kwargs": {"unload": False}}, {"cursor_kwargs": {"unload": True}}],
        indirect=True,
    )
    def test_executemany_fetch(self, arrow_cursor):
        arrow_cursor.executemany(
            "SELECT %(x)d AS x FROM one_row", [{"x": i} for i in range(1, 2)]
        )
        # Operations that have result sets are not allowed with executemany.
        pytest.raises(ProgrammingError, arrow_cursor.fetchall)
        pytest.raises(ProgrammingError, arrow_cursor.fetchmany)
        pytest.raises(ProgrammingError, arrow_cursor.fetchone)
        pytest.raises(ProgrammingError, arrow_cursor.as_arrow)
