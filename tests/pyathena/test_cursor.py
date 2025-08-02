# -*- coding: utf-8 -*-
import contextlib
import logging
import re
import time
from concurrent import futures
from concurrent.futures.thread import ThreadPoolExecutor
from datetime import date, datetime, timezone
from decimal import Decimal
from random import randint

import pytest

from pyathena import BINARY, BOOLEAN, DATE, DATETIME, JSON, NUMBER, STRING, TIME
from pyathena.converter import _to_struct
from pyathena.cursor import Cursor
from pyathena.error import DatabaseError, NotSupportedError, ProgrammingError
from pyathena.model import AthenaQueryExecution
from tests import ENV
from tests.pyathena.conftest import connect

_logger = logging.getLogger(__name__)


class TestCursor:
    def test_fetchone(self, cursor):
        cursor.execute("SELECT * FROM one_row")
        assert cursor.rowcount == -1
        assert cursor.rownumber == 0
        assert cursor.fetchone() == (1,)
        assert cursor.rownumber == 1
        assert cursor.fetchone() is None
        assert cursor.database == ENV.schema
        assert cursor.catalog
        assert cursor.query_id
        assert cursor.query
        assert cursor.statement_type == AthenaQueryExecution.STATEMENT_TYPE_DML
        assert cursor.work_group == ENV.default_work_group
        assert cursor.state == AthenaQueryExecution.STATE_SUCCEEDED
        assert cursor.state_change_reason is None
        assert cursor.submission_date_time
        assert isinstance(cursor.submission_date_time, datetime)
        assert cursor.completion_date_time
        assert isinstance(cursor.completion_date_time, datetime)
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
        assert cursor.selected_engine_version
        assert cursor.effective_engine_version

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
        query = f"SELECT * FROM one_row -- {str(datetime.now(timezone.utc))}"

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
        now = datetime.now(timezone.utc)
        cursor.execute("SELECT %(now)s as date", {"now": now})
        first_query_id = cursor.query_id

        cursor.execute("SELECT %(now)s as date", {"now": now})
        second_query_id = cursor.query_id

        cursor.execute("SELECT %(now)s as date", {"now": now}, cache_size=100)
        third_query_id = cursor.query_id

        assert first_query_id != second_query_id
        assert third_query_id in [first_query_id, second_query_id]

    def test_cache_expiration_time(self, cursor):
        query = f"SELECT * FROM one_row -- {str(datetime.now(timezone.utc))}"

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
        query = f"SELECT * FROM one_row -- {str(datetime.now(timezone.utc))}"

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
        query = f"SELECT * FROM one_row -- {str(datetime.now(timezone.utc))}"

        cursor.execute(query)
        query_id_4 = cursor.query_id

        cursor.execute(query)
        query_id_5 = cursor.query_id

        for _ in range(5):
            cursor.execute("SELECT %(now)s as date", {"now": datetime.now(timezone.utc)})

        cursor.execute(query, cache_size=1, cache_expiration_time=3600)  # 1 hours
        query_id_6 = cursor.query_id

        assert query_id_4 != query_id_5
        assert query_id_6 not in [query_id_4, query_id_5]

        # Cache hit
        query = f"SELECT * FROM one_row -- {str(datetime.now(timezone.utc))}"

        cursor.execute(query)
        query_id_7 = cursor.query_id

        cursor.execute(query)
        query_id_8 = cursor.query_id

        time.sleep(2)
        for _ in range(5):
            cursor.execute("SELECT %(now)s as date", {"now": datetime.now(timezone.utc)})

        cursor.execute(query, cache_size=1000, cache_expiration_time=3600)  # 1 hours
        query_id_9 = cursor.query_id

        assert query_id_7 != query_id_8
        assert query_id_9 in [query_id_7, query_id_8]

    @pytest.mark.parametrize(
        "cursor",
        [{"work_group": ENV.work_group, "result_reuse_enable": True, "result_reuse_minutes": 5}],
        indirect=["cursor"],
    )
    def test_cursor_query_result_reuse(self, cursor):
        now = datetime.now(timezone.utc)
        cursor.execute("SELECT %(now)s as date", {"now": now})
        assert not cursor.reused_previous_result
        assert cursor.result_reuse_enabled
        assert cursor.result_reuse_minutes == 5
        cursor.execute("SELECT %(now)s as date", {"now": now})
        assert cursor.reused_previous_result
        assert cursor.result_reuse_enabled
        assert cursor.result_reuse_minutes == 5

    @pytest.mark.parametrize("cursor", [{"work_group": ENV.work_group}], indirect=["cursor"])
    def test_execute_query_result_reuse(self, cursor):
        now = datetime.now(timezone.utc)
        cursor.execute(
            "SELECT %(now)s as date", {"now": now}, result_reuse_enable=True, result_reuse_minutes=5
        )
        assert not cursor.reused_previous_result
        assert cursor.result_reuse_enabled
        assert cursor.result_reuse_minutes == 5
        cursor.execute(
            "SELECT %(now)s as date", {"now": now}, result_reuse_enable=True, result_reuse_minutes=5
        )
        assert cursor.reused_previous_result
        assert cursor.result_reuse_enabled
        assert cursor.result_reuse_minutes == 5

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
        with contextlib.suppress(DatabaseError):
            cursor.execute("blah_blah")
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

    def test_query_with_parameter(self, cursor):
        cursor.execute(
            """
            SELECT * FROM many_rows
            WHERE a < %(param)d
            """,
            {"param": 10},
        )
        assert cursor.fetchall() == [(i,) for i in range(10)]

        cursor.execute(
            """
            SELECT col_string FROM one_row_complex
            WHERE col_string = %(param)s
            """,
            {"param": "a string"},
        )
        assert cursor.fetchall() == [("a string",)]

    def test_query_with_parameter_qmark(self, cursor):
        cursor.execute(
            """
            SELECT * FROM many_rows
            WHERE a < ?
            """,
            ["10"],
            paramstyle="qmark",
        )
        assert cursor.fetchall() == [(i,) for i in range(10)]

        cursor.execute(
            """
            SELECT col_string FROM one_row_complex
            WHERE col_string = ?
            """,
            ["'a string'"],
            paramstyle="qmark",
        )
        assert cursor.fetchall() == [("a string",)]

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
        with contextlib.closing(connect()) as conn, conn.cursor():
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
        assert cursor.output_location == f"{ENV.s3_staging_dir}{cursor.query_id}.csv"

    def test_query_execution_initial(self, cursor):
        assert not cursor.has_result_set
        assert cursor.rownumber is None
        assert cursor.rowcount == -1
        assert cursor.database is None
        assert cursor.catalog is None
        assert cursor.query_id is None
        assert cursor.query is None
        assert cursor.statement_type is None
        assert cursor.work_group is None
        assert cursor.state is None
        assert cursor.state_change_reason is None
        assert cursor.submission_date_time is None
        assert cursor.completion_date_time is None
        assert cursor.data_scanned_in_bytes is None
        assert cursor.engine_execution_time_in_millis is None
        assert cursor.query_queue_time_in_millis is None
        assert cursor.total_execution_time_in_millis is None
        assert cursor.query_planning_time_in_millis is None
        assert cursor.service_processing_time_in_millis is None
        assert cursor.output_location is None
        assert cursor.data_manifest_location is None
        assert cursor.encryption_option is None
        assert cursor.kms_key is None
        assert cursor.selected_engine_version is None
        assert cursor.effective_engine_version is None

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
              ,CAST(col_timestamp AS timestamp with time zone) AS col_timestamp_tz
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
            ("col_timestamp_tz", "timestamp with time zone", None, None, 3, 0, "UNKNOWN"),
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
                datetime(2017, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
                datetime(2017, 1, 1, 0, 0, 0).time(),
                date(2017, 1, 2),
                b"123",
                "[1, 2]",
                [1, 2],
                "{1=2, 3=4}",
                {"1": 2, "3": 4},
                {"a": 1, "b": 2},
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
            with (
                contextlib.closing(connect(schema_name=ENV.schema)) as conn,
                conn.cursor() as cursor,
            ):
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
        location = f"{ENV.s3_staging_dir}{ENV.schema}/partition_table/"
        for i in range(10):
            cursor.execute(
                """
                ALTER TABLE partition_table ADD PARTITION (b=%(b)d)
                LOCATION %(location)s
                """,
                {"b": i, "location": location},
            )
        cursor.execute("SHOW PARTITIONS partition_table")
        assert sorted(cursor.fetchall()) == [(f"b={i}",) for i in range(10)]

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
        # rowcount is not supported for executemany
        assert cursor.rowcount == -1
        cursor.execute("SELECT * FROM execute_many")
        assert sorted(cursor.fetchall()) == list(rows)

    def test_executemany_fetch(self, cursor):
        cursor.executemany("SELECT %(x)d FROM one_row", [{"x": i} for i in range(1, 2)])
        # Operations that have result sets are not allowed with executemany.
        pytest.raises(ProgrammingError, cursor.fetchall)
        pytest.raises(ProgrammingError, cursor.fetchmany)
        pytest.raises(ProgrammingError, cursor.fetchone)

    def test_iceberg_table(self, cursor):
        iceberg_table = "test_iceberg_table_cursor"
        cursor.execute(
            f"""
            CREATE TABLE {ENV.schema}.{iceberg_table} (
              id INT,
              col1 STRING
            )
            LOCATION '{ENV.s3_staging_dir}{ENV.schema}/{iceberg_table}/'
            tblproperties('table_type'='ICEBERG')
            """
        )
        cursor.execute(
            f"""
            INSERT INTO {ENV.schema}.{iceberg_table} (id, col1)
            VALUES (1, 'test1'), (2, 'test2')
            """
        )
        assert cursor.rowcount == 2
        cursor.execute(
            f"""
            SELECT COUNT(*) FROM {ENV.schema}.{iceberg_table}
            """
        )
        assert cursor.fetchall() == [(2,)]
        assert cursor.rowcount == -1

        cursor.execute(
            f"""
            UPDATE {ENV.schema}.{iceberg_table}
            SET col1 = 'test1_update'
            WHERE id = 1
            """
        )
        assert cursor.rowcount == 1
        cursor.execute(
            f"""
            SELECT col1
            FROM {ENV.schema}.{iceberg_table}
            WHERE id = 1
            """
        )
        assert cursor.fetchall() == [("test1_update",)]

        cursor.execute(
            f"""
            CREATE TABLE {ENV.schema}.{iceberg_table}_merge
            WITH (
              table_type = 'ICEBERG',
              location = '{ENV.s3_staging_dir}{ENV.schema}/{iceberg_table}_merge/',
              is_external = false
            )
            AS SELECT 1 AS id, 'foobar' AS col1
            """
        )
        assert cursor.rowcount == 1
        cursor.execute(
            f"""
            MERGE INTO {ENV.schema}.{iceberg_table} AS t1
            USING {ENV.schema}.{iceberg_table}_merge AS t2
              ON t1.id = t2.id
            WHEN MATCHED
              THEN UPDATE SET col1 = t2.col1
            """
        )
        assert cursor.rowcount == 1
        cursor.execute(
            f"""
            SELECT col1
            FROM {ENV.schema}.{iceberg_table}
            WHERE id = 1
            """
        )
        assert cursor.fetchall() == [("foobar",)]

        cursor.execute(
            f"""
            VACUUM {ENV.schema}.{iceberg_table}
            """
        )

        cursor.execute(
            f"""
            DELETE FROM {ENV.schema}.{iceberg_table}
            WHERE id = 2
            """
        )
        assert cursor.rowcount == 1
        cursor.execute(
            f"""
            SELECT COUNT(*) FROM {ENV.schema}.{iceberg_table}
            """
        )
        assert cursor.fetchall() == [(1,)]


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


class TestComplexDataTypes:
    """Test complex data types (STRUCT, ARRAY, MAP) with actual Athena queries."""

    def test_struct_types(self, cursor):
        """Test various STRUCT type scenarios to understand Athena's behavior."""
        test_cases = [
            # Basic struct
            ("SELECT ROW('John', 30) AS simple_struct", "simple_struct"),
            # Named struct fields
            (
                "SELECT CAST(ROW('Alice', 25) AS ROW(name VARCHAR, age INTEGER)) AS named_struct",
                "named_struct",
            ),
            # Struct with special characters
            ("SELECT ROW('Hello, world', 'x=y+1') AS special_chars_struct", "special_chars_struct"),
            # Struct with quotes
            ("SELECT ROW('He said \"hello\"', 'It''s working') AS quotes_struct", "quotes_struct"),
            # Struct with NULL values
            ("SELECT ROW('Alice', NULL, 'active') AS null_struct", "null_struct"),
            # Nested struct
            (
                "SELECT ROW(ROW('John', 30), ROW('Engineer', 'Tech')) AS nested_struct",
                "nested_struct",
            ),
            # Struct as JSON (using CAST AS JSON)
            ("SELECT CAST(ROW('Alice', 25, 'Hello, world') AS JSON) AS json_struct", "json_struct"),
        ]

        _logger.info("=== STRUCT Type Test Results ===")
        for query, description in test_cases:
            cursor.execute(query)
            result = cursor.fetchone()
            struct_value = result[0]
            _logger.info(f"{description}: {struct_value!r} (type: {type(struct_value).__name__})")

            # Validate struct value and converter behavior
            assert struct_value is not None, f"STRUCT value should not be None for {description}"

            # Test struct conversion behavior
            if isinstance(struct_value, str):
                converted = _to_struct(struct_value)
                _logger.info(f"{description}: Converted {struct_value!r} -> {converted!r}")
                # For string structs, conversion should succeed or return None for complex cases
                if converted is not None:
                    assert isinstance(converted, dict), (
                        f"Converted struct should be dict for {description}"
                    )
            elif isinstance(struct_value, dict):
                # Already converted by the cursor converter
                _logger.info(f"{description}: Already converted to dict: {struct_value!r}")
            else:
                # Log unexpected types for debugging but don't fail
                _logger.warning(
                    f"{description}: Unexpected type {type(struct_value).__name__}: "
                    f"{struct_value!r}"
                )

    def test_array_types(self, cursor):
        """Test various ARRAY type scenarios."""
        test_cases = [
            # Simple array
            ("SELECT ARRAY[1, 2, 3, 4, 5] AS simple_array", "simple_array"),
            # String array
            ("SELECT ARRAY['apple', 'banana', 'cherry'] AS string_array", "string_array"),
            # Array with special characters
            (
                "SELECT ARRAY['Hello, world', 'x=y+1', 'It''s working'] AS special_array",
                "special_array",
            ),
            # Array of structs
            ("SELECT ARRAY[ROW('Alice', 25), ROW('Bob', 30)] AS struct_array", "struct_array"),
            # Nested arrays
            ("SELECT ARRAY[ARRAY[1, 2], ARRAY[3, 4]] AS nested_array", "nested_array"),
            # Array as JSON (wrapped in object - top-level arrays not supported)
            (
                "SELECT CAST(MAP(ARRAY['items'], ARRAY[ARRAY['Alice', 'Bob', 'Charlie']]) AS JSON) "
                "AS json_array",
                "json_array",
            ),
        ]

        _logger.info("=== ARRAY Type Test Results ===")
        for query, description in test_cases:
            cursor.execute(query)
            result = cursor.fetchone()
            array_value = result[0]
            _logger.info(f"{description}: {array_value!r} (type: {type(array_value).__name__})")

            # Validate array value
            assert array_value is not None, f"ARRAY value should not be None for {description}"
            _logger.info(f"{description}: Array value type {type(array_value).__name__}")

    def test_map_types(self, cursor):
        """Test various MAP type scenarios."""
        test_cases = [
            # Simple map
            (
                "SELECT MAP(ARRAY[1, 2, 3], ARRAY['one', 'two', 'three']) AS simple_map",
                "simple_map",
            ),
            # String key map
            (
                (
                    "SELECT MAP(ARRAY['name', 'age', 'city'], "
                    "ARRAY['John', '30', 'Tokyo']) AS string_map"
                ),
                "string_map",
            ),
            # Map with special characters
            (
                (
                    "SELECT MAP(ARRAY['msg', 'formula'], "
                    "ARRAY['Hello, world', 'x=y+1']) AS special_map"
                ),
                "special_map",
            ),
            # Map with struct values
            (
                (
                    "SELECT MAP(ARRAY['person1', 'person2'], "
                    "ARRAY[ROW('Alice', 25), ROW('Bob', 30)]) AS struct_value_map"
                ),
                "struct_value_map",
            ),
            # Map as JSON (using CAST AS JSON)
            (
                "SELECT CAST(MAP(ARRAY['name', 'age'], ARRAY['Alice', '25']) AS JSON) AS json_map",
                "json_map",
            ),
        ]

        _logger.info("=== MAP Type Test Results ===")
        for query, description in test_cases:
            cursor.execute(query)
            result = cursor.fetchone()
            map_value = result[0]
            _logger.info(f"{description}: {map_value!r} (type: {type(map_value).__name__})")

            # Validate map value
            assert map_value is not None, f"MAP value should not be None for {description}"
            _logger.info(f"{description}: Map value type {type(map_value).__name__}")

    def test_complex_combinations(self, cursor):
        """Test complex combinations of data types."""
        test_cases = [
            # Struct containing array and map
            (
                (
                    "SELECT ROW(ARRAY[1, 2, 3], "
                    "MAP(ARRAY['a', 'b'], ARRAY[1, 2])) AS struct_with_collections"
                ),
                "struct_with_collections",
            ),
            # Array of maps
            (
                (
                    "SELECT ARRAY[MAP(ARRAY['name'], ARRAY['Alice']), "
                    "MAP(ARRAY['name'], ARRAY['Bob'])] AS array_of_maps"
                ),
                "array_of_maps",
            ),
            # Map with array values (using consistent types)
            (
                (
                    "SELECT MAP(ARRAY['numbers', 'letters'], "
                    "ARRAY[ARRAY['1', '2', '3'], ARRAY['a', 'b', 'c']]) AS map_with_arrays"
                ),
                "map_with_arrays",
            ),
        ]

        _logger.info("=== Complex Combinations Test Results ===")
        for query, description in test_cases:
            cursor.execute(query)
            result = cursor.fetchone()
            complex_value = result[0]
            _logger.info(f"{description}: {complex_value!r} (type: {type(complex_value).__name__})")

            # Validate complex value
            assert complex_value is not None, f"Complex value should not be None for {description}"
            _logger.info(f"{description}: Complex value type {type(complex_value).__name__}")
