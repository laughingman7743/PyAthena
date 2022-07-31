# -*- coding: utf-8 -*-
import contextlib
import re
import time
from concurrent import futures
from concurrent.futures.thread import ThreadPoolExecutor
from datetime import date, datetime
from decimal import Decimal
from random import randint

import pytest

from pyathena import BINARY, BOOLEAN, DATE, DATETIME, JSON, NUMBER, STRING, TIME
from pyathena.cursor import Cursor
from pyathena.error import DatabaseError, NotSupportedError, ProgrammingError
from pyathena.model import AthenaQueryExecution
from tests import ENV
from tests.conftest import connect


class TestCursor:
    def test_fetchone(self, cursor):
        cursor.execute("SELECT * FROM one_row")
        assert cursor.rownumber == 0
        assert cursor.fetchone() == (1,)
        assert cursor.rownumber == 1
        assert cursor.fetchone() is None
        assert cursor.database == ENV.schema
        assert cursor.query_id
        assert cursor.query
        assert cursor.statement_type == AthenaQueryExecution.STATEMENT_TYPE_DML
        assert cursor.state == AthenaQueryExecution.STATE_SUCCEEDED
        assert cursor.state_change_reason is None
        assert cursor.completion_date_time
        assert isinstance(cursor.completion_date_time, datetime)
        assert cursor.submission_date_time
        assert isinstance(cursor.submission_date_time, datetime)
        assert cursor.data_scanned_in_bytes
        assert cursor.engine_execution_time_in_millis
        assert cursor.query_queue_time_in_millis
        assert cursor.total_execution_time_in_millis
        # assert cursor.query_planning_time_in_millis  # TODO flaky test
        # assert cursor.service_processing_time_in_millis  # TODO flaky test
        assert cursor.output_location
        assert cursor.data_manifest_location is None
        assert cursor.encryption_option is None
        assert cursor.kms_key is None
        assert cursor.work_group == ENV.default_work_group

    def test_fetchmany(self, cursor):
        cursor.execute("SELECT * FROM many_rows LIMIT 15")
        actual1 = cursor.fetchmany(10)
        assert len(actual1) == 10
        assert actual1 == [(i,) for i in range(10)]
        actual2 = cursor.fetchmany(10)
        assert len(actual2) == 5
        assert actual2 == [(i,) for i in range(10, 15)]

    def test_fetchall(self, cursor):
        cursor.execute("SELECT * FROM one_row")
        assert cursor.fetchall() == [(1,)]
        cursor.execute("SELECT a FROM many_rows ORDER BY a")
        assert cursor.fetchall() == [(i,) for i in range(10000)]

    def test_iterator(self, cursor):
        cursor.execute("SELECT * FROM one_row")
        assert list(cursor) == [(1,)]
        pytest.raises(StopIteration, cursor.__next__)

    def test_cache_size(self, cursor):
        # To test caching, we need to make sure the query is unique, otherwise
        # we might accidentally pick up the cache results from another CI run.
        query = "SELECT * FROM one_row -- {0}".format(str(datetime.utcnow()))

        cursor.execute(query)
        first_query_id = cursor.query_id

        cursor.execute(query)
        second_query_id = cursor.query_id

        cursor.execute(query, cache_size=100)
        third_query_id = cursor.query_id

        # Make sure default behavior is no caching, i.e. same query has
        # run twice results in different query IDs
        assert first_query_id != second_query_id
        # When using caching, the same query ID should be returned.
        assert third_query_id in [first_query_id, second_query_id]

    @pytest.mark.parametrize("cursor", [{"work_group": ENV.work_group}], indirect=["cursor"])
    def test_cache_size_with_work_group(self, cursor):
        now = datetime.utcnow()
        cursor.execute("SELECT %(now)s as date", {"now": now})
        first_query_id = cursor.query_id

        cursor.execute("SELECT %(now)s as date", {"now": now})
        second_query_id = cursor.query_id

        cursor.execute("SELECT %(now)s as date", {"now": now}, cache_size=100)
        third_query_id = cursor.query_id

        assert first_query_id != second_query_id
        assert third_query_id in [first_query_id, second_query_id]

    def test_cache_expiration_time(self, cursor):
        query = "SELECT * FROM one_row -- {0}".format(str(datetime.utcnow()))

        cursor.execute(query)
        query_id_1 = cursor.query_id

        cursor.execute(query)
        query_id_2 = cursor.query_id

        cursor.execute(query, cache_expiration_time=3600)  # 1 hours
        query_id_3 = cursor.query_id

        assert query_id_1 != query_id_2
        assert query_id_3 in [query_id_1, query_id_2]

    def test_cache_expiration_time_with_cache_size(self, cursor):
        # Cache miss
        query = "SELECT * FROM one_row -- {0}".format(str(datetime.utcnow()))

        cursor.execute(query)
        query_id_1 = cursor.query_id

        cursor.execute(query)
        query_id_2 = cursor.query_id

        time.sleep(2)

        cursor.execute(query, cache_size=100, cache_expiration_time=1)  # 1 seconds
        query_id_3 = cursor.query_id

        assert query_id_1 != query_id_2
        assert query_id_3 not in [query_id_1, query_id_2]

        # Cache miss
        query = "SELECT * FROM one_row -- {0}".format(str(datetime.utcnow()))

        cursor.execute(query)
        query_id_4 = cursor.query_id

        cursor.execute(query)
        query_id_5 = cursor.query_id

        for _ in range(5):
            cursor.execute("SELECT %(now)s as date", {"now": datetime.utcnow()})

        cursor.execute(query, cache_size=1, cache_expiration_time=3600)  # 1 hours
        query_id_6 = cursor.query_id

        assert query_id_4 != query_id_5
        assert query_id_6 not in [query_id_4, query_id_5]

        # Cache hit
        query = "SELECT * FROM one_row -- {0}".format(str(datetime.utcnow()))

        cursor.execute(query)
        query_id_7 = cursor.query_id

        cursor.execute(query)
        query_id_8 = cursor.query_id

        time.sleep(2)
        for _ in range(5):
            cursor.execute("SELECT %(now)s as date", {"now": datetime.utcnow()})

        cursor.execute(query, cache_size=100, cache_expiration_time=3600)  # 1 hours
        query_id_9 = cursor.query_id

        assert query_id_7 != query_id_8
        assert query_id_9 in [query_id_7, query_id_8]

    def test_arraysize(self, cursor):
        cursor.arraysize = 5
        cursor.execute("SELECT * FROM many_rows LIMIT 20")
        assert len(cursor.fetchmany()) == 5

    def test_arraysize_default(self, cursor):
        assert cursor.arraysize == Cursor.DEFAULT_FETCH_SIZE

    def test_invalid_arraysize(self, cursor):
        with pytest.raises(ProgrammingError):
            cursor.arraysize = 10000
        with pytest.raises(ProgrammingError):
            cursor.arraysize = -1

    def test_description(self, cursor):
        cursor.execute("SELECT 1 AS foobar FROM one_row")
        assert cursor.description == [("foobar", "integer", None, None, 10, 0, "UNKNOWN")]

    def test_description_initial(self, cursor):
        assert cursor.description is None

    def test_description_failed(self, cursor):
        try:
            cursor.execute("blah_blah")
        except DatabaseError:
            pass
        assert cursor.description is None

    def test_bad_query(self, cursor):
        def run():
            cursor.execute("SELECT does_not_exist FROM this_really_does_not_exist")
            cursor.fetchone()

        pytest.raises(DatabaseError, run)

    def test_fetch_no_data(self, cursor):
        pytest.raises(ProgrammingError, cursor.fetchone)
        pytest.raises(ProgrammingError, cursor.fetchmany)
        pytest.raises(ProgrammingError, cursor.fetchall)

    def test_null_param(self, cursor):
        cursor.execute("SELECT %(param)s FROM one_row", {"param": None})
        assert cursor.fetchall() == [(None,)]

    def test_no_params(self, cursor):
        pytest.raises(DatabaseError, lambda: cursor.execute("SELECT %(param)s FROM one_row"))
        pytest.raises(KeyError, lambda: cursor.execute("SELECT %(param)s FROM one_row", {"a": 1}))

    def test_contain_special_character_query(self, cursor):
        cursor.execute(
            """
            SELECT col_string FROM one_row_complex
            WHERE col_string LIKE '%str%'
            """
        )
        assert cursor.fetchall() == [("a string",)]
        cursor.execute(
            """
            SELECT col_string FROM one_row_complex
            WHERE col_string LIKE '%%str%%'
            """
        )
        assert cursor.fetchall() == [("a string",)]
        cursor.execute(
            """
            SELECT col_string, '%' FROM one_row_complex
            WHERE col_string LIKE '%str%'
            """
        )
        assert cursor.fetchall() == [("a string", "%")]
        cursor.execute(
            """
            SELECT col_string, '%%' FROM one_row_complex
            WHERE col_string LIKE '%%str%%'
            """
        )
        assert cursor.fetchall() == [("a string", "%%")]

    def test_contain_special_character_query_with_parameter(self, cursor):
        pytest.raises(
            TypeError,
            lambda: cursor.execute(
                """
                SELECT col_string, %(param)s FROM one_row_complex
                WHERE col_string LIKE '%str%'
                """,
                {"param": "a string"},
            ),
        )
        cursor.execute(
            """
            SELECT col_string, %(param)s FROM one_row_complex
            WHERE col_string LIKE '%%str%%'
            """,
            {"param": "a string"},
        )
        assert cursor.fetchall() == [("a string", "a string")]
        pytest.raises(
            ValueError,
            lambda: cursor.execute(
                """
                SELECT col_string, '%' FROM one_row_complex
                WHERE col_string LIKE %(param)s
                """,
                {"param": "%str%"},
            ),
        )
        cursor.execute(
            """
            SELECT col_string, '%%' FROM one_row_complex
            WHERE col_string LIKE %(param)s
            """,
            {"param": "%str%"},
        )
        assert cursor.fetchall() == [("a string", "%")]

    def test_escape(self, cursor):
        bad_str = """`~!@#$%^&*()_+-={}[]|\\;:'",./<>?\n\r\t """
        cursor.execute("SELECT %(a)d, %(b)s FROM one_row", {"a": 1, "b": bad_str})
        assert cursor.fetchall() == [
            (
                1,
                bad_str,
            )
        ]

    def test_none_empty_query(self, cursor):
        pytest.raises(ProgrammingError, lambda: cursor.execute(None))
        pytest.raises(ProgrammingError, lambda: cursor.execute(""))

    def test_invalid_params(self, cursor):
        pytest.raises(
            TypeError,
            lambda: cursor.execute("SELECT * FROM one_row", {"foo": {"bar": 1}}),
        )

    def test_open_close(self):
        with contextlib.closing(connect()):
            pass
        with contextlib.closing(connect()) as conn:
            with conn.cursor():
                pass

    def test_unicode(self, cursor):
        unicode_str = "王兢"
        cursor.execute("SELECT %(param)s FROM one_row", {"param": unicode_str})
        assert cursor.fetchall() == [(unicode_str,)]

    def test_decimal(self, cursor):
        cursor.execute("SELECT %(decimal)s", {"decimal": Decimal("0.00000000001")})
        assert cursor.fetchall() == [(Decimal("0.00000000001"),)]

    def test_null(self, cursor):
        cursor.execute("SELECT null FROM many_rows")
        assert cursor.fetchall() == [(None,)] * 10000
        cursor.execute("SELECT IF(a % 11 = 0, null, a) FROM many_rows")
        assert cursor.fetchall() == [(None if a % 11 == 0 else a,) for a in range(10000)]

    def test_query_id(self, cursor):
        assert cursor.query_id is None
        cursor.execute("SELECT * from one_row")
        # query_id is UUID v4
        expected_pattern = (
            r"^[0-9a-f]{8}-[0-9a-f]{4}-[4][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
        )
        assert re.match(expected_pattern, cursor.query_id)

    def test_output_location(self, cursor):
        assert cursor.output_location is None
        cursor.execute("SELECT * from one_row")
        assert cursor.output_location == "{0}{1}.csv".format(ENV.s3_staging_dir, cursor.query_id)

    def test_query_execution_initial(self, cursor):
        assert not cursor.has_result_set
        assert cursor.rownumber is None
        assert cursor.query_id is None
        assert cursor.query is None
        assert cursor.state is None
        assert cursor.state_change_reason is None
        assert cursor.completion_date_time is None
        assert cursor.submission_date_time is None
        assert cursor.data_scanned_in_bytes is None
        assert cursor.engine_execution_time_in_millis is None
        assert cursor.query_queue_time_in_millis is None
        assert cursor.total_execution_time_in_millis is None
        assert cursor.query_planning_time_in_millis is None
        assert cursor.output_location is None
        assert cursor.data_manifest_location is None
        assert cursor.encryption_option is None
        assert cursor.kms_key is None
        assert cursor.work_group is None

    def test_complex(self, cursor):
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
        assert cursor.description == [
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
        rows = cursor.fetchall()
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
        assert rows == expected
        # catch unicode/str
        assert list(map(type, rows[0])) == list(map(type, expected[0]))
        # compare dbapi type object
        assert [d[1] for d in cursor.description] == [
            BOOLEAN,
            NUMBER,
            NUMBER,
            NUMBER,
            NUMBER,
            NUMBER,
            NUMBER,
            STRING,
            STRING,
            DATETIME,
            TIME,
            DATE,
            BINARY,
            STRING,
            JSON,
            STRING,
            JSON,
            STRING,
            NUMBER,
        ]

    def test_cancel(self, cursor):
        def cancel(c):
            time.sleep(randint(5, 10))
            c.cancel()

        with ThreadPoolExecutor(max_workers=1) as executor:
            executor.submit(cancel, cursor)

            pytest.raises(
                DatabaseError,
                lambda: cursor.execute(
                    """
                    SELECT a.a * rand(), b.a * rand()
                    FROM many_rows a
                    CROSS JOIN many_rows b
                    """
                ),
            )

    def test_cancel_initial(self, cursor):
        pytest.raises(ProgrammingError, cursor.cancel)

    def test_multiple_connection(self):
        def execute_other_thread():
            with contextlib.closing(connect(schema_name=ENV.schema)) as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT * FROM one_row")
                    return cursor.fetchall()

        with ThreadPoolExecutor(max_workers=2) as executor:
            fs = [executor.submit(execute_other_thread) for _ in range(2)]
            for f in futures.as_completed(fs):
                assert f.result() == [(1,)]

    def test_no_ops(self):
        conn = connect()
        cursor = conn.cursor()
        assert cursor.rowcount == -1
        cursor.setinputsizes([])
        cursor.setoutputsize(1, "blah")
        conn.commit()
        pytest.raises(NotSupportedError, lambda: conn.rollback())
        cursor.close()
        conn.close()

    def test_show_partition(self, cursor):
        location = "{0}{1}/{2}/".format(ENV.s3_staging_dir, ENV.schema, "partition_table")
        for i in range(10):
            cursor.execute(
                """
                ALTER TABLE partition_table ADD PARTITION (b=%(b)d)
                LOCATION %(location)s
                """,
                {"b": i, "location": location},
            )
        cursor.execute("SHOW PARTITIONS partition_table")
        assert sorted(cursor.fetchall()) == [("b={0}".format(i),) for i in range(10)]

    @pytest.mark.parametrize("cursor", [{"work_group": ENV.work_group}], indirect=["cursor"])
    def test_workgroup(self, cursor):
        cursor.execute("SELECT * FROM one_row")
        assert cursor.work_group == ENV.work_group

    @pytest.mark.parametrize("cursor", [{"work_group": ENV.work_group}], indirect=["cursor"])
    def test_no_s3_staging_dir(self, cursor):
        cursor._s3_staging_dir = None
        cursor.execute("SELECT * FROM one_row")
        assert cursor.output_location

    def test_executemany(self, cursor):
        rows = [(1, "foo"), (2, "bar"), (3, "jim o'rourke")]
        cursor.executemany(
            "INSERT INTO execute_many (a, b) VALUES (%(a)d, %(b)s)",
            [{"a": a, "b": b} for a, b in rows],
        )
        cursor.execute("SELECT * FROM execute_many")
        assert sorted(cursor.fetchall()) == [(a, b) for a, b in rows]

    def test_executemany_fetch(self, cursor):
        cursor.executemany("SELECT %(x)d FROM one_row", [{"x": i} for i in range(1, 2)])
        # Operations that have result sets are not allowed with executemany.
        pytest.raises(ProgrammingError, cursor.fetchall)
        pytest.raises(ProgrammingError, cursor.fetchmany)
        pytest.raises(ProgrammingError, cursor.fetchone)


class TestDictCursor:
    def test_fetchone(self, dict_cursor):
        dict_cursor.execute("SELECT * FROM one_row")
        assert dict_cursor.fetchone() == {"number_of_rows": 1}

    def test_fetchmany(self, dict_cursor):
        dict_cursor.execute("SELECT * FROM many_rows LIMIT 15")
        actual1 = dict_cursor.fetchmany(10)
        assert len(actual1) == 10
        assert actual1 == [{"a": i} for i in range(10)]
        actual2 = dict_cursor.fetchmany(10)
        assert len(actual2) == 5
        assert actual2 == [{"a": i} for i in range(10, 15)]

    def test_fetchall(self, dict_cursor):
        dict_cursor.execute("SELECT * FROM one_row")
        assert dict_cursor.fetchall() == [{"number_of_rows": 1}]
        dict_cursor.execute("SELECT a FROM many_rows ORDER BY a")
        assert dict_cursor.fetchall() == [{"a": i} for i in range(10000)]
