# -*- coding: utf-8 -*-
import re
import textwrap
import uuid
from datetime import date, datetime
from decimal import Decimal

import numpy as np
import pandas as pd
import pytest
import sqlalchemy
from sqlalchemy import types
from sqlalchemy.exc import NoSuchTableError
from sqlalchemy.sql import expression
from sqlalchemy.sql.ddl import CreateTable
from sqlalchemy.sql.schema import Column, MetaData, Table
from sqlalchemy.sql.selectable import TextualSelect

from pyathena.sqlalchemy_athena import AthenaDialect
from tests.conftest import ENV


class TestSQLAlchemyAthena:
    def test_basic_query(self, engine):
        engine, conn = engine
        rows = conn.execute("SELECT * FROM one_row").fetchall()
        assert len(rows) == 1
        assert rows[0].number_of_rows == 1
        assert len(rows[0]) == 1

    def test_reflect_no_such_table(self, engine):
        engine, conn = engine
        pytest.raises(
            NoSuchTableError,
            lambda: Table("this_does_not_exist", MetaData(), autoload_with=conn),
        )
        pytest.raises(
            NoSuchTableError,
            lambda: Table(
                "this_does_not_exist",
                MetaData(schema="also_does_not_exist"),
                autoload_with=conn,
            ),
        )

    def test_reflect_table(self, engine):
        engine, conn = engine
        one_row = Table("one_row", MetaData(), autoload_with=conn)
        assert len(one_row.c) == 1
        assert one_row.c.number_of_rows is not None
        assert one_row.comment == "table comment"
        assert "location" in one_row.dialect_options["awsathena"]
        assert "compression" in one_row.dialect_options["awsathena"]
        assert one_row.dialect_options["awsathena"]["location"] is not None

    def test_reflect_table_with_schema(self, engine):
        engine, conn = engine
        one_row = Table("one_row", MetaData(schema=ENV.schema), autoload_with=conn)
        assert len(one_row.c) == 1
        assert one_row.c.number_of_rows is not None
        assert one_row.comment == "table comment"
        assert "location" in one_row.dialect_options["awsathena"]
        assert "compression" in one_row.dialect_options["awsathena"]
        assert one_row.dialect_options["awsathena"]["location"] is not None

    def test_reflect_table_include_columns(self, engine):
        engine, conn = engine
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
        assert len(one_row_complex.c) == 1
        assert one_row_complex.c.col_int is not None
        pytest.raises(AttributeError, lambda: one_row_complex.c.col_tinyint)

    def test_partition_table_columns(self, engine):
        engine, conn = engine
        partition_table = Table("partition_table", MetaData(), autoload_with=conn)
        assert len(partition_table.columns) == 2
        assert "a" in partition_table.columns
        assert "b" in partition_table.columns

    def test_unicode(self, engine):
        engine, conn = engine
        unicode_str = "密林"
        one_row = Table("one_row", MetaData(schema=ENV.schema))
        returned_str = conn.execute(
            sqlalchemy.select(
                [expression.bindparam("あまぞん", unicode_str, type_=types.String())],
                from_obj=one_row,
            )
        ).scalar()
        assert returned_str == unicode_str

    def test_reflect_schemas(self, engine):
        engine, conn = engine
        insp = sqlalchemy.inspect(engine)
        schemas = insp.get_schema_names()
        assert ENV.schema in schemas
        assert "default" in schemas

    def test_get_table_names(self, engine):
        engine, conn = engine
        meta = MetaData()
        meta.reflect(bind=engine)
        assert "one_row" in meta.tables
        assert "one_row_complex" in meta.tables
        assert "view_one_row" not in meta.tables

        insp = sqlalchemy.inspect(engine)
        assert "many_rows" in insp.get_table_names(schema=ENV.schema)

    def test_get_view_names(self, engine):
        engine, conn = engine
        meta = MetaData()
        meta.reflect(bind=engine, views=True)
        assert "one_row" in meta.tables
        assert "one_row_complex" in meta.tables
        assert "view_one_row" in meta.tables

        insp = sqlalchemy.inspect(engine)
        actual = insp.get_view_names(schema=ENV.schema)
        assert "one_row" not in actual
        assert "one_row_complex" not in actual
        assert "view_one_row" in actual

    def test_get_table_comment(self, engine):
        engine, conn = engine
        insp = sqlalchemy.inspect(engine)
        actual = insp.get_table_comment("one_row", schema=ENV.schema)
        assert actual == {"text": "table comment"}

    def test_get_table_options(self, engine):
        engine, conn = engine
        insp = sqlalchemy.inspect(engine)
        actual = insp.get_table_options("parquet_with_compression", schema=ENV.schema)
        assert actual == {
            "awsathena_location": f"{ENV.s3_staging_dir}{ENV.schema}/parquet_with_compression",
            "awsathena_compression": "SNAPPY",
        }

    def test_has_table(self, engine):
        engine, conn = engine
        insp = sqlalchemy.inspect(engine)
        assert insp.has_table("one_row", schema=ENV.schema)
        assert not insp.has_table("this_table_does_not_exist", schema=ENV.schema)

    def test_get_columns(self, engine):
        engine, conn = engine
        insp = sqlalchemy.inspect(engine)
        actual = insp.get_columns(table_name="one_row", schema=ENV.schema)[0]
        assert actual["name"] == "number_of_rows"
        assert isinstance(actual["type"], types.INTEGER)
        assert actual["nullable"]
        assert actual["default"] is None
        assert not actual["autoincrement"]
        assert actual["comment"] == "some comment"

    def test_char_length(self, engine):
        engine, conn = engine
        one_row_complex = Table(
            "one_row_complex", MetaData(schema=ENV.schema), autoload_with=conn
        )
        result = conn.execute(
            sqlalchemy.select(
                [sqlalchemy.func.char_length(one_row_complex.c.col_string)]
            )
        ).scalar()
        assert result == len("a string")

    def test_reflect_select(self, engine):
        engine, conn = engine
        one_row_complex = Table(
            "one_row_complex", MetaData(schema=ENV.schema), autoload_with=conn
        )
        assert len(one_row_complex.c) == 16
        assert isinstance(one_row_complex.c.col_string, Column)
        rows = conn.execute(one_row_complex.select()).fetchall()
        assert len(rows) == 1
        assert list(rows[0]) == [
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
            date(2017, 1, 2),
            b"123",
            "[1, 2]",
            "{1=2, 3=4}",
            "{a=1, b=2}",
            Decimal("0.1"),
        ]
        assert isinstance(one_row_complex.c.col_boolean.type, types.BOOLEAN)
        assert isinstance(one_row_complex.c.col_tinyint.type, types.INTEGER)
        assert isinstance(one_row_complex.c.col_smallint.type, types.INTEGER)
        assert isinstance(one_row_complex.c.col_int.type, types.INTEGER)
        assert isinstance(one_row_complex.c.col_bigint.type, types.BIGINT)
        assert isinstance(one_row_complex.c.col_float.type, types.FLOAT)
        assert isinstance(one_row_complex.c.col_double.type, types.FLOAT)
        assert isinstance(one_row_complex.c.col_string.type, types.String)
        assert isinstance(one_row_complex.c.col_varchar.type, types.VARCHAR)
        assert one_row_complex.c.col_varchar.type.length == 10
        assert isinstance(one_row_complex.c.col_timestamp.type, types.TIMESTAMP)
        assert isinstance(one_row_complex.c.col_date.type, types.DATE)
        assert isinstance(one_row_complex.c.col_binary.type, types.BINARY)
        assert isinstance(one_row_complex.c.col_array.type, types.String)
        assert isinstance(one_row_complex.c.col_map.type, types.String)
        assert isinstance(one_row_complex.c.col_struct.type, types.String)
        assert isinstance(
            one_row_complex.c.col_decimal.type,
            types.DECIMAL,
        )
        assert one_row_complex.c.col_decimal.type.precision == 10
        assert one_row_complex.c.col_decimal.type.scale == 1

    def test_select_offset_limit(self, engine):
        engine, conn = engine
        many_rows = Table("many_rows", MetaData(schema=ENV.schema), autoload_with=conn)
        rows = conn.execute(many_rows.select().offset(10).limit(5)).fetchall()
        assert rows == [(i,) for i in range(10, 15)]

    def test_reserved_words(self):
        """Presto uses double quotes, not backticks"""
        fake_table = Table(
            "select", MetaData(), Column("current_timestamp", types.String())
        )
        query = str(fake_table.select(fake_table.c.current_timestamp == "a"))
        assert '"select"' in query
        assert '"current_timestamp"' in query
        assert "`select`" not in query
        assert "`current_timestamp`" not in query

    def test_get_column_type(self, engine):
        engine, conn = engine
        dialect = engine.dialect
        assert isinstance(dialect._get_column_type("boolean"), types.BOOLEAN)
        assert isinstance(dialect._get_column_type("tinyint"), types.INTEGER)
        assert isinstance(dialect._get_column_type("smallint"), types.INTEGER)
        assert isinstance(dialect._get_column_type("integer"), types.INTEGER)
        assert isinstance(dialect._get_column_type("int"), types.INTEGER)
        assert isinstance(dialect._get_column_type("bigint"), types.BIGINT)
        assert isinstance(dialect._get_column_type("float"), types.FLOAT)
        assert isinstance(dialect._get_column_type("double"), types.FLOAT)
        assert isinstance(dialect._get_column_type("real"), types.FLOAT)
        assert isinstance(dialect._get_column_type("string"), types.String)
        assert isinstance(dialect._get_column_type("varchar"), types.VARCHAR)
        varchar_with_args = dialect._get_column_type("varchar(10)")
        assert isinstance(varchar_with_args, types.VARCHAR)
        assert varchar_with_args.length == 10
        assert isinstance(dialect._get_column_type("timestamp"), types.TIMESTAMP)
        assert isinstance(dialect._get_column_type("date"), types.DATE)
        assert isinstance(dialect._get_column_type("binary"), types.BINARY)
        assert isinstance(dialect._get_column_type("array<integer>"), types.String)
        assert isinstance(dialect._get_column_type("map<int, int>"), types.String)
        assert isinstance(
            dialect._get_column_type("struct<a: int, b: int>"), types.String
        )
        decimal_with_args = dialect._get_column_type("decimal(10,1)")
        assert isinstance(decimal_with_args, types.DECIMAL)
        assert decimal_with_args.precision == 10
        assert decimal_with_args.scale == 1

    def test_contain_percents_character_query(self, engine):
        engine, conn = engine
        select = sqlalchemy.sql.text(
            """
            SELECT date_parse('20191030', '%Y%m%d')
            """
        )
        table_expression = TextualSelect(select, []).cte()

        query = sqlalchemy.select(["*"]).select_from(table_expression)
        result = engine.execute(query)
        assert result.fetchall() == [(datetime(2019, 10, 30),)]

        query_with_limit = (
            sqlalchemy.sql.select(["*"]).select_from(table_expression).limit(1)
        )
        result_with_limit = engine.execute(query_with_limit)
        assert result_with_limit.fetchall() == [(datetime(2019, 10, 30),)]

    def test_query_with_parameter(self, engine):
        engine, conn = engine
        select = sqlalchemy.sql.text(
            """
            SELECT :word
            """
        )
        table_expression = TextualSelect(select, []).cte()

        query = sqlalchemy.select(["*"]).select_from(table_expression)
        result = engine.execute(query, word="cat")
        assert result.fetchall() == [("cat",)]

        query_with_limit = (
            sqlalchemy.select(["*"]).select_from(table_expression).limit(1)
        )
        result_with_limit = engine.execute(query_with_limit, word="cat")
        assert result_with_limit.fetchall() == [("cat",)]

    def test_contain_percents_character_query_with_parameter(self, engine):
        engine, conn = engine
        select1 = sqlalchemy.sql.text(
            """
            SELECT date_parse('20191030', '%Y%m%d'), :word
            """
        )
        table_expression1 = TextualSelect(select1, []).cte()

        query1 = sqlalchemy.select(["*"]).select_from(table_expression1)
        result1 = engine.execute(query1, word="cat")
        assert result1.fetchall() == [(datetime(2019, 10, 30), "cat")]

        query_with_limit1 = (
            sqlalchemy.select(["*"]).select_from(table_expression1).limit(1)
        )
        result_with_limit1 = engine.execute(query_with_limit1, word="cat")
        assert result_with_limit1.fetchall() == [(datetime(2019, 10, 30), "cat")]

        select2 = sqlalchemy.sql.text(
            """
            SELECT col_string, :param FROM one_row_complex
            WHERE col_string LIKE 'a%' OR col_string LIKE :param
            """
        )
        table_expression2 = TextualSelect(select2, []).cte()

        query2 = sqlalchemy.select(["*"]).select_from(table_expression2)
        result2 = engine.execute(query2, param="b%")
        assert result2.fetchall() == [("a string", "b%")]

        query_with_limit2 = (
            sqlalchemy.select(["*"]).select_from(table_expression2).limit(1)
        )
        result_with_limit2 = engine.execute(query_with_limit2, param="b%")
        assert result_with_limit2.fetchall() == [("a string", "b%")]

    def test_to_sql(self, engine):
        # TODO pyathena.error.OperationalError: SYNTAX_ERROR: line 1:305:
        #      Column 'foobar' cannot be resolved.
        #      def _format_bytes(formatter, escaper, val):
        #          return val.decode()
        engine, conn = engine
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
            schema=ENV.schema,
            index=False,
            if_exists="replace",
            method="multi",
        )

        table = Table(table_name, MetaData(schema=ENV.schema), autoload_with=conn)
        assert conn.execute(table.select()).fetchall() == [
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
        ]

    @pytest.mark.parametrize("engine", [{"verify": "false"}], indirect=True)
    def test_conn_str_verify(self, engine):
        engine, conn = engine
        kwargs = conn.connection._kwargs
        assert not kwargs["verify"]

    @pytest.mark.parametrize("engine", [{"duration_seconds": "1800"}], indirect=True)
    def test_conn_str_duration_seconds(self, engine):
        engine, conn = engine
        kwargs = conn.connection._kwargs
        assert kwargs["duration_seconds"] == 1800

    @pytest.mark.parametrize("engine", [{"poll_interval": "5"}], indirect=True)
    def test_conn_str_poll_interval(self, engine):
        engine, conn = engine
        assert conn.connection.poll_interval == 5

    @pytest.mark.parametrize("engine", [{"kill_on_interrupt": "false"}], indirect=True)
    def test_conn_str_kill_on_interrupt(self, engine):
        engine, conn = engine
        assert not conn.connection.kill_on_interrupt

    def test_create_table(self, engine):
        engine, conn = engine
        table_name = "test_create_table"
        column_name = "col"
        table = Table(
            table_name,
            MetaData(),
            Column(column_name, types.String(10)),
            schema=ENV.schema,
            awsathena_location=f"{ENV.s3_staging_dir}{ENV.schema}/{table_name}",
            awsathena_compression=None,
        )
        insp = sqlalchemy.inspect(engine)
        table.create(bind=conn)
        assert insp.has_table(table_name, schema=ENV.schema)

    def test_create_table_location(self):
        dialect = AthenaDialect()
        table_name = "test_create_table_location"
        column_name = "col"
        table = Table(
            table_name,
            MetaData(schema=ENV.schema),
            Column(column_name, types.String(10)),
            awsathena_location=f"s3://path/to/{ENV.schema}/{table_name}",
            awsathena_compression="SNAPPY",
        )
        actual = CreateTable(table).compile(dialect=dialect)
        # If there is no `/` at the end of the `awsathena_location`, it will be appended.
        assert str(actual) == textwrap.dedent(
            f"""
            CREATE EXTERNAL TABLE {ENV.schema}.{table_name} (
            \t{column_name} VARCHAR(10)
            )
            STORED AS PARQUET
            LOCATION 's3://path/to/{ENV.schema}/{table_name}/'
            TBLPROPERTIES ('parquet.compress'='SNAPPY')\n\n
            """
        )

    def test_create_table_with_comments_compilation(self):
        dialect = AthenaDialect()
        table_name = "test_create_table_with_comments_compilation"
        column_name = "col"
        table = Table(
            table_name,
            MetaData(schema=ENV.schema),
            Column(column_name, types.String(10), comment="some descriptive comment"),
            awsathena_location=f"s3://path/to/{ENV.schema}/{table_name}/",
            comment=textwrap.dedent(
                """
                Some table comment

                a multiline one that should stay as is.
                """
            ),
        )
        actual = CreateTable(table).compile(dialect=dialect)
        assert str(actual) == textwrap.dedent(
            f"""
            CREATE EXTERNAL TABLE {ENV.schema}.{table_name} (
            \t{column_name} VARCHAR(10) COMMENT 'some descriptive comment'
            )
            COMMENT '
            Some table comment

            a multiline one that should stay as is.
            '
            STORED AS PARQUET
            LOCATION 's3://path/to/{ENV.schema}/{table_name}/'\n\n
            """
        )

    def test_create_table_with_comments(self, engine):
        engine, conn = engine
        table_name = "test_create_table_with_comments"
        column_name = "col"
        comment = textwrap.dedent(
            """
            Some table comment

            a multiline one that should stay as is.
            """
        )
        table = Table(
            table_name,
            MetaData(schema=ENV.schema),
            Column(column_name, types.String(10), comment="some descriptive comment"),
            awsathena_location=f"{ENV.s3_staging_dir}{ENV.schema}/{table_name}",
            comment=comment,
        )
        table.create(bind=conn)
        actual = Table(table_name, MetaData(schema=ENV.schema), autoload_with=conn)
        assert actual.c[column_name].comment == table.c[column_name].comment
        # The AWS API seems to return comments with squashed whitespace and line breaks.
        # assert actual.comment == table.comment
        assert actual.comment
        assert actual.comment == "\n{}\n".format(re.sub(r"\s+", " ", comment[1:-1]))

    def test_column_comment_containing_single_quotes(self, engine):
        engine, conn = engine
        table_name = "table_name_column_comment_single_quotes"
        column_name = "col"
        comment = "let's make sure quotes ain\\'t a problem"
        table = Table(
            table_name,
            MetaData(schema=ENV.schema),
            Column(column_name, types.String(10), comment=comment),
            awsathena_location=f"{ENV.s3_staging_dir}{ENV.schema}/{table_name}",
        )
        conn.execute(CreateTable(table), parameter="some value")
        actual = Table(table_name, MetaData(schema=ENV.schema), autoload_with=conn)
        assert actual.c[column_name].comment == comment

    def test_column_comment_containing_placeholder(self, engine):
        engine, conn = engine
        table_name = "table_name_placeholder_in_column_comment"
        column_name = "col"
        comment = "the %(parameter)s ratio (in %)"
        table = Table(
            table_name,
            MetaData(schema=ENV.schema),
            Column(column_name, types.String(10), comment=comment),
            awsathena_location=f"{ENV.s3_staging_dir}{ENV.schema}/{table_name}",
        )
        conn.execute(CreateTable(table), parameter="some value")
        actual = Table(table_name, MetaData(schema=ENV.schema), autoload_with=conn)
        assert actual.c[column_name].comment == comment

    def test_create_table_with_primary_key(self, engine):
        engine, conn = engine
        dialect = AthenaDialect()
        table_name = "test_create_table_with_primary_key"
        table = Table(
            table_name,
            MetaData(schema=ENV.schema),
            Column("pk", types.Integer, primary_key=True),
            awsathena_location=f"{ENV.s3_staging_dir}{ENV.schema}/{table_name}",
        )
        # The table will be created, but Athena does not support primary keys.
        table.create(bind=conn)
        actual = Table(table_name, MetaData(schema=ENV.schema), autoload_with=conn)
        ddl = CreateTable(actual).compile(dialect=dialect)
        assert str(ddl) == textwrap.dedent(
            f"""
            CREATE EXTERNAL TABLE {ENV.schema}.{table_name} (
            \tpk INT
            )
            STORED AS PARQUET
            LOCATION '{ENV.s3_staging_dir}{ENV.schema}/{table_name}/'\n\n
            """
        )
        assert len(actual.primary_key.columns) == 0

    def test_create_table_with_varchar_text_column(self, engine):
        engine, conn = engine
        dialect = AthenaDialect()
        table_name = "test_create_table_with_varchar_text_column"
        table = Table(
            table_name,
            MetaData(schema=ENV.schema),
            Column("col_varchar", types.String()),
            Column("col_varchar_length", types.String(10)),
            Column("col_varchar_type", types.String),
            Column("col_text", types.Text),
            awsathena_location=f"{ENV.s3_staging_dir}{ENV.schema}/{table_name}/",
            awsathena_compression="SNAPPY",
        )
        table.create(bind=conn)
        actual = Table(table_name, MetaData(schema=ENV.schema), autoload_with=conn)

        ddl = CreateTable(actual).compile(dialect=dialect)
        assert str(ddl) == textwrap.dedent(
            f"""
            CREATE EXTERNAL TABLE {ENV.schema}.{table_name} (
            \tcol_varchar STRING,
            \tcol_varchar_length VARCHAR(10),
            \tcol_varchar_type STRING,
            \tcol_text STRING
            )
            STORED AS PARQUET
            LOCATION '{ENV.s3_staging_dir}{ENV.schema}/{table_name}/'
            TBLPROPERTIES ('parquet.compress'='SNAPPY')\n\n
            """
        )

        assert isinstance(actual.c.col_varchar.type, types.String)
        assert not isinstance(actual.c.col_varchar.type, types.VARCHAR)
        assert actual.c.col_varchar.type.length is None

        assert isinstance(actual.c.col_varchar_length.type, types.String)
        assert isinstance(actual.c.col_varchar_length.type, types.VARCHAR)
        assert actual.c.col_varchar_length.type.length == 10

        assert isinstance(actual.c.col_varchar_type.type, types.String)
        assert not isinstance(actual.c.col_varchar_type.type, types.VARCHAR)
        assert actual.c.col_varchar_type.type.length is None

        assert isinstance(actual.c.col_text.type, types.String)
        assert not isinstance(actual.c.col_text.type, types.VARCHAR)
        assert actual.c.col_text.type.length is None

    def test_cast_as_varchar(self, engine):
        engine, conn = engine

        # varchar without length
        one_row = Table("one_row", MetaData(schema=ENV.schema), autoload_with=conn)
        actual = conn.execute(
            sqlalchemy.select(
                [expression.cast(one_row.c.number_of_rows, types.VARCHAR)],
                from_obj=one_row,
            )
        ).scalar()
        assert actual == "1"

        # varchar with length
        actual = conn.execute(
            sqlalchemy.select(
                [expression.cast(one_row.c.number_of_rows, types.VARCHAR(10))],
                from_obj=one_row,
            )
        ).scalar()
        assert actual == "1"
