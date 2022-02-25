# -*- coding: utf-8 -*-
import re
import textwrap
import unittest
import uuid
from datetime import date, datetime
from decimal import Decimal
from urllib.parse import quote_plus

import numpy as np
import pandas as pd
import sqlalchemy
from sqlalchemy import String
from sqlalchemy.engine import create_engine
from sqlalchemy.exc import NoSuchTableError, OperationalError, ProgrammingError
from sqlalchemy.sql import expression
from sqlalchemy.sql.ddl import CreateTable
from sqlalchemy.sql.schema import Column, MetaData, Table
from sqlalchemy.sql.sqltypes import (
    BIGINT,
    BINARY,
    BOOLEAN,
    DATE,
    DECIMAL,
    FLOAT,
    INTEGER,
    STRINGTYPE,
    TIMESTAMP,
)

from pyathena.sqlalchemy_athena import AthenaDialect
from tests.conftest import ENV, S3_PREFIX, SCHEMA
from tests.util import with_engine


class TestSQLAlchemyAthena(unittest.TestCase):
    def create_engine(self, **kwargs):
        conn_str = (
            "awsathena+rest://athena.{region_name}.amazonaws.com:443/"
            + "{schema_name}?s3_staging_dir={s3_staging_dir}&s3_dir={s3_dir}"
            + "&compression=snappy"
        )
        if "verify" in kwargs:
            conn_str += "&verify={verify}"
        if "duration_seconds" in kwargs:
            conn_str += "&duration_seconds={duration_seconds}"
        if "poll_interval" in kwargs:
            conn_str += "&poll_interval={poll_interval}"
        if "kill_on_interrupt" in kwargs:
            conn_str += "&kill_on_interrupt={kill_on_interrupt}"
        return create_engine(
            conn_str.format(
                region_name=ENV.region_name,
                schema_name=SCHEMA,
                s3_staging_dir=quote_plus(ENV.s3_staging_dir),
                s3_dir=quote_plus(ENV.s3_staging_dir),
                **kwargs,
            )
        )

    @with_engine()
    def test_basic_query(self, engine, conn):
        rows = conn.execute("SELECT * FROM one_row").fetchall()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].number_of_rows, 1)
        self.assertEqual(len(rows[0]), 1)

    @with_engine()
    def test_reflect_no_such_table(self, engine, conn):
        self.assertRaises(
            NoSuchTableError,
            lambda: Table("this_does_not_exist", MetaData(), autoload_with=conn),
        )
        self.assertRaises(
            NoSuchTableError,
            lambda: Table(
                "this_does_not_exist",
                MetaData(),
                schema="also_does_not_exist",
                autoload_with=conn,
            ),
        )

    @with_engine()
    def test_reflect_table(self, engine, conn):
        one_row = Table("one_row", MetaData(), autoload_with=conn)
        self.assertEqual(len(one_row.c), 1)
        self.assertIsNotNone(one_row.c.number_of_rows)
        self.assertEqual(one_row.comment, "table comment")
        self.assertIn("location", one_row.dialect_options["awsathena"])
        self.assertIn("compression", one_row.dialect_options["awsathena"])
        self.assertIsNotNone(
            "location", one_row.dialect_options["awsathena"]["location"]
        )

    @with_engine()
    def test_reflect_table_with_schema(self, engine, conn):
        one_row = Table("one_row", MetaData(), schema=SCHEMA, autoload_with=conn)
        self.assertEqual(len(one_row.c), 1)
        self.assertIsNotNone(one_row.c.number_of_rows)
        self.assertEqual(one_row.comment, "table comment")
        self.assertIn("location", one_row.dialect_options["awsathena"])
        self.assertIn("compression", one_row.dialect_options["awsathena"])
        self.assertIsNotNone(one_row.dialect_options["awsathena"]["location"])

    @with_engine()
    def test_reflect_table_include_columns(self, engine, conn):
        one_row_complex = Table("one_row_complex", MetaData())
        version = float(
            re.search(r"^([\d]+\.[\d]+)\..+", sqlalchemy.__version__).group(1)
        )
        if version <= 1.2:
            engine.dialect.reflecttable(
                conn, one_row_complex, include_columns=["col_int"], exclude_columns=[]
            )
        elif version == 1.3:
            # https://docs.sqlalchemy.org/en/13/changelog/changelog_13.html#change-64ac776996da1a5c3e3460b4c0f0b257
            engine.dialect.reflecttable(
                conn,
                one_row_complex,
                include_columns=["col_int"],
                exclude_columns=[],
                resolve_fks=True,
            )
        else:  # version >= 1.4
            # https://docs.sqlalchemy.org/en/14/changelog/changelog_14.html#change-0215fae622c01f9409eb1ba2754f4792
            # https://docs.sqlalchemy.org/en/14/core/reflection.html#sqlalchemy.engine.reflection.Inspector.reflect_table
            insp = sqlalchemy.inspect(engine)
            insp.reflect_table(
                one_row_complex,
                include_columns=["col_int"],
                exclude_columns=[],
                resolve_fks=True,
            )
        self.assertEqual(len(one_row_complex.c), 1)
        self.assertIsNotNone(one_row_complex.c.col_int)
        self.assertRaises(AttributeError, lambda: one_row_complex.c.col_tinyint)

    @with_engine()
    def test_unicode(self, engine, conn):
        unicode_str = "密林"
        one_row = Table("one_row", MetaData())
        returned_str = conn.execute(
            sqlalchemy.select(
                [expression.bindparam("あまぞん", unicode_str, type_=String())],
                from_obj=one_row,
            )
        ).scalar()
        self.assertEqual(returned_str, unicode_str)

    @with_engine()
    def test_reflect_schemas(self, engine, conn):
        insp = sqlalchemy.inspect(engine)
        schemas = insp.get_schema_names()
        self.assertIn(SCHEMA, schemas)
        self.assertIn("default", schemas)

    @with_engine()
    def test_get_table_names(self, engine, conn):
        meta = MetaData()
        meta.reflect(bind=engine)
        self.assertIn("one_row", meta.tables)
        self.assertIn("one_row_complex", meta.tables)
        self.assertNotIn("view_one_row", meta.tables)

        insp = sqlalchemy.inspect(engine)
        self.assertIn(
            "many_rows",
            insp.get_table_names(schema=SCHEMA),
        )

    @with_engine()
    def test_get_view_names(self, engine, conn):
        meta = MetaData()
        meta.reflect(bind=engine, views=True)
        self.assertIn("one_row", meta.tables)
        self.assertIn("one_row_complex", meta.tables)
        self.assertIn("view_one_row", meta.tables)

        insp = sqlalchemy.inspect(engine)
        actual = insp.get_view_names(schema=SCHEMA)
        self.assertNotIn("one_row", actual)
        self.assertNotIn("one_row_complex", actual)
        self.assertIn("view_one_row", actual)

    @with_engine()
    def test_get_table_comment(self, engine, conn):
        insp = sqlalchemy.inspect(engine)
        actual = insp.get_table_comment("one_row", schema=SCHEMA)
        self.assertEqual(actual, {"text": "table comment"})

    @with_engine()
    def test_get_table_options(self, engine, conn):
        insp = sqlalchemy.inspect(engine)
        actual = insp.get_table_options("parquet_with_compression", schema=SCHEMA)
        self.assertEqual(
            actual,
            {
                "awsathena_location": f"{ENV.s3_staging_dir}{S3_PREFIX}/parquet_with_compression",
                "awsathena_compression": "SNAPPY",
            },
        )

    @with_engine()
    def test_has_table(self, engine, conn):
        insp = sqlalchemy.inspect(engine)
        self.assertTrue(insp.has_table("one_row", schema=SCHEMA))
        self.assertFalse(insp.has_table("this_table_does_not_exist", schema=SCHEMA))

    @with_engine()
    def test_get_columns(self, engine, conn):
        insp = sqlalchemy.inspect(engine)
        actual = insp.get_columns(table_name="one_row", schema=SCHEMA)[0]
        self.assertEqual(actual["name"], "number_of_rows")
        self.assertTrue(isinstance(actual["type"], INTEGER))
        self.assertTrue(actual["nullable"])
        self.assertIsNone(actual["default"])
        self.assertEqual(actual["ordinal_position"], 1)
        self.assertEqual(actual["comment"], "some comment")

    @with_engine()
    def test_char_length(self, engine, conn):
        one_row_complex = Table("one_row_complex", MetaData(), autoload_with=conn)
        result = conn.execute(
            sqlalchemy.select(
                [sqlalchemy.func.char_length(one_row_complex.c.col_string)]
            )
        ).scalar()
        self.assertEqual(result, len("a string"))

    @with_engine()
    def test_reflect_select(self, engine, conn):
        one_row_complex = Table("one_row_complex", MetaData(), autoload_with=conn)
        self.assertEqual(len(one_row_complex.c), 15)
        self.assertIsInstance(one_row_complex.c.col_string, Column)
        rows = conn.execute(one_row_complex.select()).fetchall()
        self.assertEqual(len(rows), 1)
        self.assertEqual(
            list(rows[0]),
            [
                True,
                127,
                32767,
                2147483647,
                9223372036854775807,
                0.5,
                0.25,
                "a string",
                datetime(2017, 1, 1, 0, 0, 0),
                date(2017, 1, 2),
                b"123",
                "[1, 2]",
                "{1=2, 3=4}",
                "{a=1, b=2}",
                Decimal("0.1"),
            ],
        )
        self.assertIsInstance(one_row_complex.c.col_boolean.type, BOOLEAN)
        self.assertIsInstance(one_row_complex.c.col_tinyint.type, INTEGER)
        self.assertIsInstance(one_row_complex.c.col_smallint.type, INTEGER)
        self.assertIsInstance(one_row_complex.c.col_int.type, INTEGER)
        self.assertIsInstance(one_row_complex.c.col_bigint.type, BIGINT)
        self.assertIsInstance(one_row_complex.c.col_float.type, FLOAT)
        self.assertIsInstance(one_row_complex.c.col_double.type, FLOAT)
        self.assertIsInstance(one_row_complex.c.col_string.type, type(STRINGTYPE))
        self.assertIsInstance(one_row_complex.c.col_timestamp.type, TIMESTAMP)
        self.assertIsInstance(one_row_complex.c.col_date.type, DATE)
        self.assertIsInstance(one_row_complex.c.col_binary.type, BINARY)
        self.assertIsInstance(one_row_complex.c.col_array.type, type(STRINGTYPE))
        self.assertIsInstance(one_row_complex.c.col_map.type, type(STRINGTYPE))
        self.assertIsInstance(one_row_complex.c.col_struct.type, type(STRINGTYPE))
        self.assertIsInstance(one_row_complex.c.col_decimal.type, DECIMAL)

    @with_engine()
    def test_select_offset_limit(self, engine, conn):
        many_rows = Table("many_rows", MetaData(), autoload_with=conn)
        rows = conn.execute(many_rows.select().offset(10).limit(5)).fetchall()
        self.assertEqual(rows, [(i,) for i in range(10, 15)])

    @with_engine()
    def test_reserved_words(self, engine, conn):
        """Presto uses double quotes, not backticks"""
        fake_table = Table(
            "select", MetaData(), Column("current_timestamp", STRINGTYPE)
        )
        query = str(fake_table.select(fake_table.c.current_timestamp == "a"))
        self.assertIn('"select"', query)
        self.assertIn('"current_timestamp"', query)
        self.assertNotIn("`select`", query)
        self.assertNotIn("`current_timestamp`", query)

    @with_engine()
    def test_retry_if_data_catalog_exception(self, engine, conn):
        dialect = engine.dialect
        exc = OperationalError(
            "", None, "Database does_not_exist not found. Please check your query."
        )
        self.assertFalse(
            dialect._retry_if_data_catalog_exception(
                exc, "does_not_exist", "does_not_exist"
            )
        )
        self.assertFalse(
            dialect._retry_if_data_catalog_exception(
                exc, "does_not_exist", "this_does_not_exist"
            )
        )
        self.assertTrue(
            dialect._retry_if_data_catalog_exception(
                exc, "this_does_not_exist", "does_not_exist"
            )
        )
        self.assertTrue(
            dialect._retry_if_data_catalog_exception(
                exc, "this_does_not_exist", "this_does_not_exist"
            )
        )

        exc = OperationalError(
            "", None, "Namespace does_not_exist not found. Please check your query."
        )
        self.assertFalse(
            dialect._retry_if_data_catalog_exception(
                exc, "does_not_exist", "does_not_exist"
            )
        )
        self.assertFalse(
            dialect._retry_if_data_catalog_exception(
                exc, "does_not_exist", "this_does_not_exist"
            )
        )
        self.assertTrue(
            dialect._retry_if_data_catalog_exception(
                exc, "this_does_not_exist", "does_not_exist"
            )
        )
        self.assertTrue(
            dialect._retry_if_data_catalog_exception(
                exc, "this_does_not_exist", "this_does_not_exist"
            )
        )

        exc = OperationalError(
            "", None, "Table does_not_exist not found. Please check your query."
        )
        self.assertFalse(
            dialect._retry_if_data_catalog_exception(
                exc, "does_not_exist", "does_not_exist"
            )
        )
        self.assertTrue(
            dialect._retry_if_data_catalog_exception(
                exc, "does_not_exist", "this_does_not_exist"
            )
        )
        self.assertFalse(
            dialect._retry_if_data_catalog_exception(
                exc, "this_does_not_exist", "does_not_exist"
            )
        )
        self.assertTrue(
            dialect._retry_if_data_catalog_exception(
                exc, "this_does_not_exist", "this_does_not_exist"
            )
        )

        exc = OperationalError("", None, "foobar.")
        self.assertTrue(
            dialect._retry_if_data_catalog_exception(exc, "foobar", "foobar")
        )

        exc = ProgrammingError(
            "", None, "Database does_not_exist not found. Please check your query."
        )
        self.assertFalse(
            dialect._retry_if_data_catalog_exception(
                exc, "does_not_exist", "does_not_exist"
            )
        )
        self.assertFalse(
            dialect._retry_if_data_catalog_exception(
                exc, "does_not_exist", "this_does_not_exist"
            )
        )
        self.assertFalse(
            dialect._retry_if_data_catalog_exception(
                exc, "this_does_not_exist", "does_not_exist"
            )
        )
        self.assertFalse(
            dialect._retry_if_data_catalog_exception(
                exc, "this_does_not_exist", "this_does_not_exist"
            )
        )

    @with_engine()
    def test_get_column_type(self, engine, conn):
        dialect = engine.dialect
        self.assertEqual(dialect._get_column_type("boolean"), "boolean")
        self.assertEqual(dialect._get_column_type("tinyint"), "tinyint")
        self.assertEqual(dialect._get_column_type("smallint"), "smallint")
        self.assertEqual(dialect._get_column_type("integer"), "integer")
        self.assertEqual(dialect._get_column_type("bigint"), "bigint")
        self.assertEqual(dialect._get_column_type("real"), "real")
        self.assertEqual(dialect._get_column_type("double"), "double")
        self.assertEqual(dialect._get_column_type("varchar"), "varchar")
        self.assertEqual(dialect._get_column_type("timestamp"), "timestamp")
        self.assertEqual(dialect._get_column_type("date"), "date")
        self.assertEqual(dialect._get_column_type("varbinary"), "varbinary")
        self.assertEqual(dialect._get_column_type("array(integer)"), "array")
        self.assertEqual(dialect._get_column_type("map(integer, integer)"), "map")
        self.assertEqual(dialect._get_column_type("row(a integer, b integer)"), "row")
        self.assertEqual(dialect._get_column_type("decimal(10,1)"), "decimal")

    @with_engine()
    def test_contain_percents_character_query(self, engine, conn):
        select = sqlalchemy.sql.text(
            """
            SELECT date_parse('20191030', '%Y%m%d')
            """
        )
        table_expression = sqlalchemy.sql.selectable.TextAsFrom(select, []).cte()

        query = sqlalchemy.select(["*"]).select_from(table_expression)
        result = engine.execute(query)
        self.assertEqual(result.fetchall(), [(datetime(2019, 10, 30),)])

        query_with_limit = (
            sqlalchemy.sql.select(["*"]).select_from(table_expression).limit(1)
        )
        result_with_limit = engine.execute(query_with_limit)
        self.assertEqual(result_with_limit.fetchall(), [(datetime(2019, 10, 30),)])

    @with_engine()
    def test_query_with_parameter(self, engine, conn):
        select = sqlalchemy.sql.text(
            """
            SELECT :word
            """
        )
        table_expression = sqlalchemy.sql.selectable.TextAsFrom(select, []).cte()

        query = sqlalchemy.select(["*"]).select_from(table_expression)
        result = engine.execute(query, word="cat")
        self.assertEqual(result.fetchall(), [("cat",)])

        query_with_limit = (
            sqlalchemy.select(["*"]).select_from(table_expression).limit(1)
        )
        result_with_limit = engine.execute(query_with_limit, word="cat")
        self.assertEqual(result_with_limit.fetchall(), [("cat",)])

    @with_engine()
    def test_contain_percents_character_query_with_parameter(self, engine, conn):
        select1 = sqlalchemy.sql.text(
            """
            SELECT date_parse('20191030', '%Y%m%d'), :word
            """
        )
        table_expression1 = sqlalchemy.sql.selectable.TextAsFrom(select1, []).cte()

        query1 = sqlalchemy.select(["*"]).select_from(table_expression1)
        result1 = engine.execute(query1, word="cat")
        self.assertEqual(result1.fetchall(), [(datetime(2019, 10, 30), "cat")])

        query_with_limit1 = (
            sqlalchemy.select(["*"]).select_from(table_expression1).limit(1)
        )
        result_with_limit1 = engine.execute(query_with_limit1, word="cat")
        self.assertEqual(
            result_with_limit1.fetchall(), [(datetime(2019, 10, 30), "cat")]
        )

        select2 = sqlalchemy.sql.text(
            """
            SELECT col_string, :param FROM one_row_complex
            WHERE col_string LIKE 'a%' OR col_string LIKE :param
            """
        )
        table_expression2 = sqlalchemy.sql.selectable.TextAsFrom(select2, []).cte()

        query2 = sqlalchemy.select(["*"]).select_from(table_expression2)
        result2 = engine.execute(query2, param="b%")
        self.assertEqual(result2.fetchall(), [("a string", "b%")])

        query_with_limit2 = (
            sqlalchemy.select(["*"]).select_from(table_expression2).limit(1)
        )
        result_with_limit2 = engine.execute(query_with_limit2, param="b%")
        self.assertEqual(result_with_limit2.fetchall(), [("a string", "b%")])

    @with_engine()
    def test_nan_checks(self, engine, conn):
        dialect = engine.dialect
        self.assertFalse(dialect._is_nan("string"))
        self.assertFalse(dialect._is_nan(1))
        self.assertTrue(dialect._is_nan(float("nan")))

    @with_engine()
    def test_to_sql(self, engine, conn):
        # TODO pyathena.error.OperationalError: SYNTAX_ERROR: line 1:305:
        #      Column 'foobar' cannot be resolved.
        #      def _format_bytes(formatter, escaper, val):
        #          return val.decode()
        table_name = "to_sql_{0}".format(str(uuid.uuid4()).replace("-", ""))
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
                # "col_binary": "foobar".encode(),
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
                # "col_binary",
            ]
        ]
        df.to_sql(
            table_name,
            engine,
            schema=SCHEMA,
            index=False,
            if_exists="replace",
            method="multi",
        )

        table = Table(table_name, MetaData(), autoload_with=conn)
        self.assertEqual(
            conn.execute(table.select()).fetchall(),
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
                    # "foobar".encode(),
                )
            ],
        )

    @with_engine(verify="false")
    def test_conn_str_verify(self, engine, conn):
        kwargs = conn.connection._kwargs
        self.assertFalse(kwargs["verify"])

    @with_engine(duration_seconds="1800")
    def test_conn_str_duration_seconds(self, engine, conn):
        kwargs = conn.connection._kwargs
        self.assertEqual(kwargs["duration_seconds"], 1800)

    @with_engine(poll_interval="5")
    def test_conn_str_poll_interval(self, engine, conn):
        self.assertEqual(conn.connection.poll_interval, 5)

    @with_engine(kill_on_interrupt="false")
    def test_conn_str_kill_on_interrupt(self, engine, conn):
        self.assertFalse(conn.connection.kill_on_interrupt)

    @with_engine()
    def test_create_table(self, engine, conn):
        table_name = "test_create_table"
        table = Table(
            table_name,
            MetaData(),
            Column("c", String(10)),
            schema=SCHEMA,
            awsathena_location=f"{ENV.s3_staging_dir}/{SCHEMA}/{table_name}",
            awsathena_compression=None,
        )
        insp = sqlalchemy.inspect(engine)
        table.create(bind=conn)
        self.assertTrue(insp.has_table(table_name, schema=SCHEMA))

    def test_create_table_location(self):
        dialect = AthenaDialect()
        table = Table(
            "test_create_table_location",
            MetaData(),
            Column("column_name", String),
            schema="test_schema",
            awsathena_location="s3://path/to/test_schema/test_create_table_location",
            awsathena_compression="SNAPPY",
        )
        actual = CreateTable(table).compile(dialect=dialect)
        # If there is no `/` at the end of the `awsathena_location`, it will be appended.
        self.assertEqual(
            str(actual),
            textwrap.dedent(
                """
                CREATE EXTERNAL TABLE test_schema.test_create_table_location (
                \tcolumn_name VARCHAR
                )
                STORED AS PARQUET
                LOCATION 's3://path/to/test_schema/test_create_table_location/'
                TBLPROPERTIES ('parquet.compress'='SNAPPY')\n\n
                """
            ),
        )

    def test_create_table_with_comments_compilation(self):
        dialect = AthenaDialect()
        table_name = "test_create_table_with_comments_compilation"
        column_name = "c"
        table = Table(
            table_name,
            MetaData(),
            Column(column_name, String(10), comment="some descriptive comment"),
            schema=SCHEMA,
            awsathena_location="s3://path/to/schema/test_create_table_with_comments_compilation/",
            comment=textwrap.dedent(
                """
                Some table comment

                a multiline one that should stay as is.
                """
            ),
        )
        actual = CreateTable(table).compile(dialect=dialect)
        self.assertEqual(
            str(actual),
            textwrap.dedent(
                f"""
                CREATE EXTERNAL TABLE {SCHEMA}.{table_name} (
                \tc VARCHAR(10) COMMENT 'some descriptive comment'
                )
                COMMENT '
                Some table comment

                a multiline one that should stay as is.
                '
                STORED AS PARQUET
                LOCATION 's3://path/to/schema/test_create_table_with_comments_compilation/'\n\n
                """
            ),
        )

    @with_engine()
    def test_create_table_with_comments(self, engine, conn):
        table_name = "test_create_table_with_comments"
        column_name = "c"
        comment = textwrap.dedent(
            """
            Some table comment

            a multiline one that should stay as is.
            """
        )
        table = Table(
            table_name,
            MetaData(),
            Column(column_name, String(10), comment="some descriptive comment"),
            schema=SCHEMA,
            awsathena_location=f"{ENV.s3_staging_dir}/{SCHEMA}/{table_name}",
            comment=comment,
        )
        table.create(bind=conn)
        actual = Table(table_name, MetaData(schema=SCHEMA), autoload_with=conn)
        self.assertIsNot(actual, table)
        self.assertIsNot(actual.metadata, table.metadata)
        self.assertEqual(actual.c[column_name].comment, table.c[column_name].comment)
        # The AWS API seems to return comments with squashed whitespace and line breaks.
        # self.assertEqual(actual.comment, table.comment)
        self.assertIsNot(actual.comment, None)
        self.assertEqual(
            actual.comment, "\n{}\n".format(re.sub(r"\s+", " ", comment[1:-1]))
        )

    @with_engine()
    def test_column_comment_containing_single_quotes(self, engine, conn):
        table_name = "table_name_column_comment_single_quotes"
        column_name = "c"
        comment = "let's make sure quotes ain\\'t a problem"
        table = Table(
            table_name,
            MetaData(),
            Column(column_name, String(10), comment=comment),
            schema=SCHEMA,
            awsathena_location=f"{ENV.s3_staging_dir}/{SCHEMA}/{table_name}",
        )
        conn.execute(CreateTable(table), parameter="some value")
        actual = Table(table_name, MetaData(), autoload_with=conn)
        self.assertIsNot(actual, table)
        self.assertIsNot(actual.metadata, table.metadata)
        self.assertEqual(actual.c[column_name].comment, comment)

    @with_engine()
    def test_column_comment_containing_placeholder(self, engine, conn):
        table_name = "table_name_placeholder_in_column_comment"
        column_name = "c"
        comment = "the %(parameter)s ratio (in %)"
        table = Table(
            table_name,
            MetaData(),
            Column(column_name, String(10), comment=comment),
            schema=SCHEMA,
            awsathena_location=f"{ENV.s3_staging_dir}/{SCHEMA}/{table_name}",
        )
        conn.execute(CreateTable(table), parameter="some value")
        actual = Table(table_name, MetaData(), autoload_with=conn)
        self.assertIsNot(actual, table)
        self.assertIsNot(actual.metadata, table.metadata)
        self.assertEqual(actual.c[column_name].comment, comment)
