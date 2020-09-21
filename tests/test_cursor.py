# -*- coding: utf-8 -*-
import contextlib
import re
import time
import unittest
from concurrent import futures
from concurrent.futures.thread import ThreadPoolExecutor
from datetime import date, datetime
from decimal import Decimal
from random import randint

from pyathena import (
    BINARY,
    BOOLEAN,
    DATE,
    DATETIME,
    JSON,
    NUMBER,
    STRING,
    TIME,
    connect,
)
from pyathena.cursor import Cursor
from pyathena.error import DatabaseError, NotSupportedError, ProgrammingError
from pyathena.model import AthenaQueryExecution
from tests import ENV, S3_PREFIX, SCHEMA, WORK_GROUP, WithConnect
from tests.util import with_cursor


class TestCursor(unittest.TestCase, WithConnect):
    """Reference test case is following:

    https://github.com/dropbox/PyHive/blob/master/pyhive/tests/dbapi_test_case.py
    https://github.com/dropbox/PyHive/blob/master/pyhive/tests/test_hive.py
    https://github.com/dropbox/PyHive/blob/master/pyhive/tests/test_presto.py
    """

    @with_cursor()
    def test_fetchone(self, cursor):
        cursor.execute("SELECT * FROM one_row")
        self.assertEqual(cursor.rownumber, 0)
        self.assertEqual(cursor.fetchone(), (1,))
        self.assertEqual(cursor.rownumber, 1)
        self.assertIsNone(cursor.fetchone())
        self.assertEqual(cursor.database, SCHEMA)
        self.assertIsNotNone(cursor.query_id)
        self.assertIsNotNone(cursor.query)
        self.assertEqual(cursor.statement_type, AthenaQueryExecution.STATEMENT_TYPE_DML)
        self.assertEqual(cursor.state, AthenaQueryExecution.STATE_SUCCEEDED)
        self.assertIsNone(cursor.state_change_reason)
        self.assertIsNotNone(cursor.completion_date_time)
        self.assertIsInstance(cursor.completion_date_time, datetime)
        self.assertIsNotNone(cursor.submission_date_time)
        self.assertIsInstance(cursor.submission_date_time, datetime)
        self.assertIsNotNone(cursor.data_scanned_in_bytes)
        self.assertIsNotNone(cursor.engine_execution_time_in_millis)
        self.assertIsNotNone(cursor.query_queue_time_in_millis)
        self.assertIsNotNone(cursor.total_execution_time_in_millis)
        # self.assertIsNotNone(cursor.query_planning_time_in_millis)  # TODO flaky test
        # self.assertIsNotNone(cursor.service_processing_time_in_millis)  # TODO flaky test
        self.assertIsNotNone(cursor.output_location)
        self.assertIsNone(cursor.data_manifest_location)
        self.assertIsNone(cursor.encryption_option)
        self.assertIsNone(cursor.kms_key)
        self.assertEqual(cursor.work_group, "primary")

    @with_cursor()
    def test_fetchmany(self, cursor):
        cursor.execute("SELECT * FROM many_rows LIMIT 15")
        self.assertEqual(len(cursor.fetchmany(10)), 10)
        self.assertEqual(len(cursor.fetchmany(10)), 5)

    @with_cursor()
    def test_fetchall(self, cursor):
        cursor.execute("SELECT * FROM one_row")
        self.assertEqual(cursor.fetchall(), [(1,)])
        cursor.execute("SELECT a FROM many_rows ORDER BY a")
        self.assertEqual(cursor.fetchall(), [(i,) for i in range(10000)])

    @with_cursor()
    def test_iterator(self, cursor):
        cursor.execute("SELECT * FROM one_row")
        self.assertEqual(list(cursor), [(1,)])
        self.assertRaises(StopIteration, cursor.__next__)

    @with_cursor()
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
        self.assertNotEqual(first_query_id, second_query_id)
        # When using caching, the same query ID should be returned.
        self.assertIn(third_query_id, [first_query_id, second_query_id])

    @with_cursor(work_group=WORK_GROUP)
    def test_cache_size_with_work_group(self, cursor):
        now = datetime.utcnow()
        cursor.execute("SELECT %(now)s as date", {"now": now})
        first_query_id = cursor.query_id

        cursor.execute("SELECT %(now)s as date", {"now": now})
        second_query_id = cursor.query_id

        cursor.execute("SELECT %(now)s as date", {"now": now}, cache_size=100)
        third_query_id = cursor.query_id

        self.assertNotEqual(first_query_id, second_query_id)
        self.assertIn(third_query_id, [first_query_id, second_query_id])

    @with_cursor()
    def test_arraysize(self, cursor):
        cursor.arraysize = 5
        cursor.execute("SELECT * FROM many_rows LIMIT 20")
        self.assertEqual(len(cursor.fetchmany()), 5)

    @with_cursor()
    def test_arraysize_default(self, cursor):
        self.assertEqual(cursor.arraysize, Cursor.DEFAULT_FETCH_SIZE)

    @with_cursor()
    def test_invalid_arraysize(self, cursor):
        with self.assertRaises(ProgrammingError):
            cursor.arraysize = 10000
        with self.assertRaises(ProgrammingError):
            cursor.arraysize = -1

    @with_cursor()
    def test_description(self, cursor):
        cursor.execute("SELECT 1 AS foobar FROM one_row")
        self.assertEqual(
            cursor.description, [("foobar", "integer", None, None, 10, 0, "UNKNOWN")]
        )

    @with_cursor()
    def test_description_initial(self, cursor):
        self.assertIsNone(cursor.description)

    @with_cursor()
    def test_description_failed(self, cursor):
        try:
            cursor.execute("blah_blah")
        except DatabaseError:
            pass
        self.assertIsNone(cursor.description)

    @with_cursor()
    def test_bad_query(self, cursor):
        def run():
            cursor.execute("SELECT does_not_exist FROM this_really_does_not_exist")
            cursor.fetchone()

        self.assertRaises(DatabaseError, run)

    @with_cursor()
    def test_fetch_no_data(self, cursor):
        self.assertRaises(ProgrammingError, cursor.fetchone)
        self.assertRaises(ProgrammingError, cursor.fetchmany)
        self.assertRaises(ProgrammingError, cursor.fetchall)

    @with_cursor()
    def test_null_param(self, cursor):
        cursor.execute("SELECT %(param)s FROM one_row", {"param": None})
        self.assertEqual(cursor.fetchall(), [(None,)])

    @with_cursor()
    def test_no_params(self, cursor):
        self.assertRaises(
            DatabaseError, lambda: cursor.execute("SELECT %(param)s FROM one_row")
        )
        self.assertRaises(
            KeyError, lambda: cursor.execute("SELECT %(param)s FROM one_row", {"a": 1})
        )

    @with_cursor()
    def test_contain_special_character_query(self, cursor):
        cursor.execute(
            """
            SELECT col_string FROM one_row_complex
            WHERE col_string LIKE '%str%'
            """
        )
        self.assertEqual(cursor.fetchall(), [("a string",)])
        cursor.execute(
            """
            SELECT col_string FROM one_row_complex
            WHERE col_string LIKE '%%str%%'
            """
        )
        self.assertEqual(cursor.fetchall(), [("a string",)])
        cursor.execute(
            """
            SELECT col_string, '%' FROM one_row_complex
            WHERE col_string LIKE '%str%'
            """
        )
        self.assertEqual(cursor.fetchall(), [("a string", "%")])
        cursor.execute(
            """
            SELECT col_string, '%%' FROM one_row_complex
            WHERE col_string LIKE '%%str%%'
            """
        )
        self.assertEqual(cursor.fetchall(), [("a string", "%%")])

    @with_cursor()
    def test_contain_special_character_query_with_parameter(self, cursor):
        self.assertRaises(
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
        self.assertEqual(cursor.fetchall(), [("a string", "a string")])
        self.assertRaises(
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
        self.assertEqual(cursor.fetchall(), [("a string", "%")])

    def test_escape(self):
        bad_str = """`~!@#$%^&*()_+-={}[]|\\;:'",./<>?\n\r\t """
        self.run_escape_case(bad_str)

    @with_cursor()
    def run_escape_case(self, cursor, bad_str):
        cursor.execute("SELECT %(a)d, %(b)s FROM one_row", {"a": 1, "b": bad_str})
        self.assertEqual(
            cursor.fetchall(),
            [
                (
                    1,
                    bad_str,
                )
            ],
        )

    @with_cursor()
    def test_none_empty_query(self, cursor):
        self.assertRaises(ProgrammingError, lambda: cursor.execute(None))
        self.assertRaises(ProgrammingError, lambda: cursor.execute(""))

    @with_cursor()
    def test_invalid_params(self, cursor):
        self.assertRaises(
            TypeError,
            lambda: cursor.execute("SELECT * FROM one_row", {"foo": {"bar": 1}}),
        )

    def test_open_close(self):
        with contextlib.closing(self.connect()):
            pass
        with contextlib.closing(self.connect()) as conn:
            with conn.cursor():
                pass

    @with_cursor()
    def test_unicode(self, cursor):
        unicode_str = "王兢"
        cursor.execute("SELECT %(param)s FROM one_row", {"param": unicode_str})
        self.assertEqual(cursor.fetchall(), [(unicode_str,)])

    @with_cursor()
    def test_decimal(self, cursor):
        cursor.execute("SELECT %(decimal)s", {"decimal": Decimal("0.00000000001")})
        self.assertEqual(cursor.fetchall(), [(Decimal("0.00000000001"),)])

    @with_cursor()
    def test_null(self, cursor):
        cursor.execute("SELECT null FROM many_rows")
        self.assertEqual(cursor.fetchall(), [(None,)] * 10000)
        cursor.execute("SELECT IF(a % 11 = 0, null, a) FROM many_rows")
        self.assertEqual(
            cursor.fetchall(), [(None if a % 11 == 0 else a,) for a in range(10000)]
        )

    @with_cursor()
    def test_query_id(self, cursor):
        self.assertIsNone(cursor.query_id)
        cursor.execute("SELECT * from one_row")
        # query_id is UUID v4
        expected_pattern = (
            r"^[0-9a-f]{8}-[0-9a-f]{4}-[4][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
        )
        self.assertTrue(re.match(expected_pattern, cursor.query_id))

    @with_cursor()
    def test_output_location(self, cursor):
        self.assertIsNone(cursor.output_location)
        cursor.execute("SELECT * from one_row")
        self.assertEqual(
            cursor.output_location,
            "{0}{1}.csv".format(ENV.s3_staging_dir, cursor.query_id),
        )

    @with_cursor()
    def test_query_execution_initial(self, cursor):
        self.assertFalse(cursor.has_result_set)
        self.assertIsNone(cursor.rownumber)
        self.assertIsNone(cursor.query_id)
        self.assertIsNone(cursor.query)
        self.assertIsNone(cursor.state)
        self.assertIsNone(cursor.state_change_reason)
        self.assertIsNone(cursor.completion_date_time)
        self.assertIsNone(cursor.submission_date_time)
        self.assertIsNone(cursor.data_scanned_in_bytes)
        self.assertIsNone(cursor.engine_execution_time_in_millis)
        self.assertIsNone(cursor.query_queue_time_in_millis)
        self.assertIsNone(cursor.total_execution_time_in_millis)
        self.assertIsNone(cursor.query_planning_time_in_millis)
        self.assertIsNone(cursor.output_location)
        self.assertIsNone(cursor.data_manifest_location)
        self.assertIsNone(cursor.encryption_option)
        self.assertIsNone(cursor.kms_key)
        self.assertIsNone(cursor.work_group)

    @with_cursor()
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
        self.assertEqual(
            cursor.description,
            [
                ("col_boolean", "boolean", None, None, 0, 0, "UNKNOWN"),
                ("col_tinyint", "tinyint", None, None, 3, 0, "UNKNOWN"),
                ("col_smallint", "smallint", None, None, 5, 0, "UNKNOWN"),
                ("col_int", "integer", None, None, 10, 0, "UNKNOWN"),
                ("col_bigint", "bigint", None, None, 19, 0, "UNKNOWN"),
                ("col_float", "float", None, None, 17, 0, "UNKNOWN"),
                ("col_double", "double", None, None, 17, 0, "UNKNOWN"),
                ("col_string", "varchar", None, None, 2147483647, 0, "UNKNOWN"),
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
            ],
        )
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
        # catch unicode/str
        self.assertEqual(list(map(type, rows[0])), list(map(type, expected[0])))
        # compare dbapi type object
        self.assertEqual(
            [d[1] for d in cursor.description],
            [
                BOOLEAN,
                NUMBER,
                NUMBER,
                NUMBER,
                NUMBER,
                NUMBER,
                NUMBER,
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
            ],
        )

    @with_cursor()
    def test_cancel(self, cursor):
        def cancel(c):
            time.sleep(randint(5, 10))
            c.cancel()

        with ThreadPoolExecutor(max_workers=1) as executor:
            executor.submit(cancel, cursor)

            self.assertRaises(
                DatabaseError,
                lambda: cursor.execute(
                    """
                    SELECT a.a * rand(), b.a * rand()
                    FROM many_rows a
                    CROSS JOIN many_rows b
                    """
                ),
            )

    @with_cursor()
    def test_cancel_initial(self, cursor):
        self.assertRaises(ProgrammingError, cursor.cancel)

    def test_multiple_connection(self):
        def execute_other_thread():
            with contextlib.closing(connect(schema_name=SCHEMA)) as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT * FROM one_row")
                    return cursor.fetchall()

        with ThreadPoolExecutor(max_workers=2) as executor:
            fs = [executor.submit(execute_other_thread) for _ in range(2)]
            for f in futures.as_completed(fs):
                self.assertEqual(f.result(), [(1,)])

    def test_no_ops(self):
        conn = self.connect()
        cursor = conn.cursor()
        self.assertEqual(cursor.rowcount, -1)
        cursor.setinputsizes([])
        cursor.setoutputsize(1, "blah")
        conn.commit()
        self.assertRaises(NotSupportedError, lambda: conn.rollback())
        cursor.close()
        conn.close()

    @with_cursor()
    def test_show_partition(self, cursor):
        location = "{0}{1}/{2}/".format(
            ENV.s3_staging_dir, S3_PREFIX, "partition_table"
        )
        for i in range(10):
            cursor.execute(
                """
                ALTER TABLE partition_table ADD PARTITION (b=%(b)d)
                LOCATION %(location)s
                """,
                {"b": i, "location": location},
            )
        cursor.execute("SHOW PARTITIONS partition_table")
        self.assertEqual(
            sorted(cursor.fetchall()), [("b={0}".format(i),) for i in range(10)]
        )

    @with_cursor(work_group=WORK_GROUP)
    def test_workgroup(self, cursor):
        cursor.execute("SELECT * FROM one_row")
        self.assertEqual(cursor.work_group, WORK_GROUP)

    @with_cursor(work_group=WORK_GROUP)
    def test_no_s3_staging_dir(self, cursor):
        cursor._s3_staging_dir = None
        cursor.execute("SELECT * FROM one_row")
        self.assertNotEqual(cursor.output_location, None)

    @with_cursor()
    def test_executemany(self, cursor):
        cursor.executemany(
            "INSERT INTO execute_many (a) VALUES (%(a)s)",
            [{"a": i} for i in range(1, 3)],
        )
        cursor.execute("SELECT * FROM execute_many")
        self.assertEqual(sorted(cursor.fetchall()), [(i,) for i in range(1, 3)])

    @with_cursor()
    def test_executemany_fetch(self, cursor):
        cursor.executemany("SELECT %(x)d FROM one_row", [{"x": i} for i in range(1, 2)])
        # Operations that have result sets are not allowed with executemany.
        self.assertRaises(ProgrammingError, cursor.fetchall)
        self.assertRaises(ProgrammingError, cursor.fetchmany)
        self.assertRaises(ProgrammingError, cursor.fetchone)
