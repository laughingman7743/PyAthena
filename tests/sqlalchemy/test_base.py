# -*- coding: utf-8 -*-
import re
import sys
import textwrap
import uuid
from datetime import date, datetime
from decimal import Decimal
from urllib.parse import quote_plus

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

from tests.conftest import ENV


class TestSQLAlchemyAthena:
    def test_basic_query(self, engine):
        engine, conn = engine
        rows = conn.execute(sqlalchemy.text("SELECT * FROM one_row")).fetchall()
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
        dialect_opts = one_row.dialect_options["awsathena"]
        assert "location" in dialect_opts
        assert "compression" in dialect_opts
        assert "row_format" in dialect_opts
        assert "file_format" in dialect_opts
        assert "serdeproperties" in dialect_opts
        assert "tblproperties" in dialect_opts
        assert dialect_opts["location"] == f"{ENV.s3_staging_dir}{ENV.schema}/one_row"
        assert (
            dialect_opts["row_format"]
            == "SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'"
        )
        assert (
            dialect_opts["file_format"] == "INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' "
            "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'"
        )
        assert dialect_opts["serdeproperties"] == {
            "field.delim": "\t",
            "line.delim": "\n",
            "serialization.format": "\t",
        }
        assert dialect_opts["tblproperties"] is not None

    def test_reflect_table_with_schema(self, engine):
        engine, conn = engine
        one_row = Table("one_row", MetaData(schema=ENV.schema), autoload_with=conn)
        assert len(one_row.c) == 1
        assert one_row.c.number_of_rows is not None
        assert one_row.comment == "table comment"
        dialect_opts = one_row.dialect_options["awsathena"]
        assert "location" in dialect_opts
        assert "compression" in dialect_opts
        assert "row_format" in dialect_opts
        assert "file_format" in dialect_opts
        assert "serdeproperties" in dialect_opts
        assert "tblproperties" in dialect_opts
        assert dialect_opts["location"] == f"{ENV.s3_staging_dir}{ENV.schema}/one_row"
        assert (
            dialect_opts["row_format"]
            == "SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'"
        )
        assert (
            dialect_opts["file_format"] == "INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' "
            "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'"
        )
        assert dialect_opts["serdeproperties"] == {
            "field.delim": "\t",
            "line.delim": "\n",
            "serialization.format": "\t",
        }
        assert dialect_opts["tblproperties"] is not None

    def test_reflect_table_include_columns(self, engine):
        engine, conn = engine
        one_row_complex = Table("one_row_complex", MetaData())
        version = float(re.search(r"^([\d]+\.[\d]+)\..+", sqlalchemy.__version__).group(1))
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
        returned_str = conn.execute(
            sqlalchemy.select(expression.bindparam("あまぞん", unicode_str, type_=types.String()))
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
        assert (
            actual["awsathena_location"]
            == f"{ENV.s3_staging_dir}{ENV.schema}/parquet_with_compression"
        )
        assert actual["awsathena_compression"] == "SNAPPY"
        assert (
            actual["awsathena_row_format"]
            == "SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'"
        )
        assert (
            actual["awsathena_file_format"]
            == "INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' "
            "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'"
        )
        assert actual["awsathena_serdeproperties"] == {
            "serialization.format": "1",
        }
        assert actual["awsathena_tblproperties"] is not None

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
        one_row_complex = Table("one_row_complex", MetaData(schema=ENV.schema), autoload_with=conn)
        result = conn.execute(
            sqlalchemy.select(sqlalchemy.func.char_length(one_row_complex.c.col_string))
        ).scalar()
        assert result == len("a string")

    def test_reflect_select(self, engine):
        engine, conn = engine
        one_row_complex = Table("one_row_complex", MetaData(schema=ENV.schema), autoload_with=conn)
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
        fake_table = Table("select", MetaData(), Column("current_timestamp", types.String()))
        query = str(fake_table.select().where(fake_table.c.current_timestamp == "a"))
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
        assert isinstance(dialect._get_column_type("struct<a: int, b: int>"), types.String)
        decimal_with_args = dialect._get_column_type("decimal(10,1)")
        assert isinstance(decimal_with_args, types.DECIMAL)
        assert decimal_with_args.precision == 10
        assert decimal_with_args.scale == 1

    def test_contain_percents_character_query(self, engine):
        engine, conn = engine
        select = sqlalchemy.text(
            """
            SELECT date_parse('20191030', '%Y%m%d')
            """
        )
        table_expression = TextualSelect(select, []).cte()

        query = sqlalchemy.select("*").select_from(table_expression)
        result = conn.execute(query)
        assert result.fetchall() == [(datetime(2019, 10, 30),)]

        query_with_limit = sqlalchemy.select("*").select_from(table_expression).limit(1)
        result_with_limit = conn.execute(query_with_limit)
        assert result_with_limit.fetchall() == [(datetime(2019, 10, 30),)]

    def test_query_with_parameter(self, engine):
        engine, conn = engine
        select = sqlalchemy.text(
            """
            SELECT :word
            """
        )
        table_expression = TextualSelect(select.bindparams(word="cat"), []).cte()

        query = sqlalchemy.select("*").select_from(table_expression)
        result = conn.execute(query)
        assert result.fetchall() == [("cat",)]

        query_with_limit = sqlalchemy.select("*").select_from(table_expression).limit(1)
        result_with_limit = conn.execute(query_with_limit)
        assert result_with_limit.fetchall() == [("cat",)]

    def test_contain_percents_character_query_with_parameter(self, engine):
        engine, conn = engine
        select1 = sqlalchemy.text(
            """
            SELECT date_parse('20191030', '%Y%m%d'), :word
            """
        )
        table_expression1 = TextualSelect(select1.bindparams(word="cat"), []).cte()

        query1 = sqlalchemy.select("*").select_from(table_expression1)
        result1 = conn.execute(query1)
        assert result1.fetchall() == [(datetime(2019, 10, 30), "cat")]

        query_with_limit1 = sqlalchemy.select("*").select_from(table_expression1).limit(1)
        result_with_limit1 = conn.execute(query_with_limit1)
        assert result_with_limit1.fetchall() == [(datetime(2019, 10, 30), "cat")]

        select2 = sqlalchemy.text(
            """
            SELECT col_string, :param FROM one_row_complex
            WHERE col_string LIKE 'a%' OR col_string LIKE :param
            """
        )
        table_expression2 = TextualSelect(select2.bindparams(param="b%"), []).cte()

        query2 = sqlalchemy.select("*").select_from(table_expression2)
        result2 = conn.execute(query2)
        assert result2.fetchall() == [("a string", "b%")]

        query_with_limit2 = sqlalchemy.select("*").select_from(table_expression2).limit(1)
        result_with_limit2 = conn.execute(query_with_limit2)
        assert result_with_limit2.fetchall() == [("a string", "b%")]

    @pytest.mark.skipif(
        # TODO: Python 3.7 EOL 2023-06-27
        sys.version_info < (3, 8),
        reason="TypeError: __init__() got multiple values for argument 'schema'",
    )
    @pytest.mark.parametrize(
        "engine",
        [{"file_format": "parquet", "compression": "snappy"}],
        indirect=["engine"],
    )
    def test_to_sql_parquet(self, engine):
        engine, conn = engine
        table_name = f"""to_sql_{str(uuid.uuid4()).replace("-", "")}"""
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
            )
        ]

    @pytest.mark.skipif(
        # TODO: Python 3.7 EOL 2023-06-27
        sys.version_info < (3, 8),
        reason="TypeError: __init__() got multiple values for argument 'schema'",
    )
    @pytest.mark.parametrize(
        "engine",
        [
            {
                "row_format": "SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'",
                "serdeproperties": quote_plus("'ignore.malformed.json'='1'"),
            }
        ],
        indirect=["engine"],
    )
    def test_to_sql_json(self, engine):
        engine, conn = engine
        table_name = f"""to_sql_{str(uuid.uuid4()).replace("-", "")}"""
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
            )
        ]

    @pytest.mark.skipif(
        # TODO: Python 3.7 EOL 2023-06-27
        sys.version_info < (3, 8),
        reason="TypeError: __init__() got multiple values for argument 'schema'",
    )
    @pytest.mark.parametrize(
        "engine",
        [
            {
                "file_format": "parquet",
                "compression": "snappy",
                "bucket_count": 5,
                "partition": "col_int%2Ccol_string"
                # INSERT is not supported for bucketed tables
            }
        ],
        indirect=["engine"],
    )
    def test_to_sql_column_options(self, engine):
        engine, conn = engine
        table_name = f"""to_sql_{str(uuid.uuid4()).replace("-", "")}"""
        df = pd.DataFrame(
            {
                "col_bigint": np.int64([12345]),
                "col_float": np.float32([1.0]),
                "col_double": np.float64([1.2345]),
                "col_boolean": np.bool_([True]),
                "col_timestamp": [datetime(2020, 1, 1, 0, 0, 0)],
                "col_date": [date(2020, 12, 31)],
                # partitions
                "col_int": np.int32([1]),
                "col_string": ["a"],
            }
        )
        # Explicitly specify column order
        df = df[
            [
                "col_bigint",
                "col_float",
                "col_double",
                "col_boolean",
                "col_timestamp",
                "col_date",
                # partitions
                "col_int",
                "col_string",
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
                12345,
                1.0,
                1.2345,
                True,
                datetime(2020, 1, 1, 0, 0, 0),
                date(2020, 12, 31),
                # partitions
                1,
                "a",
            )
        ]

    @pytest.mark.parametrize("engine", [{"verify": "false"}], indirect=["engine"])
    def test_conn_str_verify(self, engine):
        engine, conn = engine
        kwargs = conn.connection._kwargs
        assert not kwargs["verify"]

    @pytest.mark.parametrize("engine", [{"duration_seconds": "1800"}], indirect=["engine"])
    def test_conn_str_duration_seconds(self, engine):
        engine, conn = engine
        kwargs = conn.connection._kwargs
        assert kwargs["duration_seconds"] == 1800

    @pytest.mark.parametrize("engine", [{"poll_interval": "5"}], indirect=["engine"])
    def test_conn_str_poll_interval(self, engine):
        engine, conn = engine
        assert conn.connection.poll_interval == 5

    @pytest.mark.parametrize("engine", [{"kill_on_interrupt": "false"}], indirect=["engine"])
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
        )
        insp = sqlalchemy.inspect(engine)
        table.create(bind=conn)
        assert insp.has_table(table_name, schema=ENV.schema)

    def test_create_table_location(self, engine):
        engine, conn = engine
        table_name = "test_create_table_location"
        column_name = "col"
        table = Table(
            table_name,
            MetaData(schema=ENV.schema),
            Column(column_name, types.String(10)),
            awsathena_location=f"s3://path/to/{ENV.schema}/{table_name}",
            awsathena_file_format="PARQUET",
            awsathena_compression="SNAPPY",
        )
        actual = CreateTable(table).compile(bind=conn)
        # If there is no `/` at the end of the `awsathena_location`, it will be appended.
        assert str(actual) == textwrap.dedent(
            f"""
            CREATE EXTERNAL TABLE {ENV.schema}.{table_name} (
            \t{column_name} VARCHAR(10)
            )
            STORED AS PARQUET
            LOCATION 's3://path/to/{ENV.schema}/{table_name}/'
            TBLPROPERTIES (
            \t'parquet.compress' = 'SNAPPY'
            )
            """
        )

    def test_create_table_bucketing(self, engine):
        engine, conn = engine
        table_name = "test_create_table_bucketing"
        table = Table(
            table_name,
            MetaData(schema=ENV.schema),
            Column("col_1", types.String(10), awsathena_cluster=True),
            Column("col_2", types.Integer, awsathena_cluster=True),
            Column("col_3", types.String, awsathena_partition=True),
            awsathena_location=f"{ENV.s3_staging_dir}{ENV.schema}/{table_name}/",
            awsathena_bucket_count=5,
        )
        ddl = CreateTable(table).compile(bind=conn)
        table.create(bind=conn)
        actual = Table(table_name, MetaData(schema=ENV.schema), autoload_with=conn)

        assert str(ddl) == textwrap.dedent(
            f"""
            CREATE EXTERNAL TABLE {ENV.schema}.{table_name} (
            \tcol_1 VARCHAR(10),
            \tcol_2 INT
            )
            PARTITIONED BY (
            \tcol_3 STRING
            )
            CLUSTERED BY (
            \tcol_1,
            \tcol_2
            ) INTO 5 BUCKETS
            LOCATION '{ENV.s3_staging_dir}{ENV.schema}/{table_name}/'
            """
        )
        dialect_opts = actual.dialect_options["awsathena"]
        assert (
            dialect_opts["row_format"]
            == "SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'"
        )
        assert (
            dialect_opts["file_format"] == "INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' "
            "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'"
        )
        # TODO The metadata retrieved from the API does not seem to include bucketing information.

    @pytest.mark.parametrize(
        "engine",
        [
            {
                "file_format": "PARQUET",
                "compression": "SNAPPY",
                "bucket_count": 5,
                "partition": "test_create_table_conn_str.col_3",
                "cluster": "col_1%2Ctest_create_table_conn_str.col_2",
            }
        ],
        indirect=["engine"],
    )
    def test_create_table_conn_str(self, engine):
        engine, conn = engine
        table_name = "test_create_table_conn_str"
        table = Table(
            table_name,
            MetaData(schema=ENV.schema),
            Column("col_1", types.String(10)),
            Column("col_2", types.Integer),
            Column("col_3", types.String),
        )
        ddl = CreateTable(table).compile(bind=conn)
        table.create(bind=conn)
        actual = Table(table_name, MetaData(schema=ENV.schema), autoload_with=conn)

        assert str(ddl) == textwrap.dedent(
            f"""
            CREATE EXTERNAL TABLE {ENV.schema}.{table_name} (
            \tcol_1 VARCHAR(10),
            \tcol_2 INT
            )
            PARTITIONED BY (
            \tcol_3 STRING
            )
            CLUSTERED BY (
            \tcol_1,
            \tcol_2
            ) INTO 5 BUCKETS
            STORED AS PARQUET
            LOCATION '{ENV.s3_staging_dir}{ENV.schema}/{table_name}/'
            TBLPROPERTIES (
            \t'parquet.compress' = 'SNAPPY'
            )
            """
        )
        dialect_opts = actual.dialect_options["awsathena"]
        assert dialect_opts["compression"] == "SNAPPY"
        assert (
            dialect_opts["row_format"]
            == "SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'"
        )
        assert (
            dialect_opts["file_format"]
            == "INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' "
            "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'"
        )
        assert actual.c.col_3.dialect_options["awsathena"]["partition"]

    def test_create_table_csv(self, engine):
        engine, conn = engine
        table_name = "test_create_table_csv"
        column_name = "col"
        table = Table(
            table_name,
            MetaData(schema=ENV.schema),
            Column(column_name, types.String(10)),
            awsathena_location=f"{ENV.s3_staging_dir}{ENV.schema}/{table_name}/",
            awsathena_row_format="SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'",
            awsathena_serdeproperties={
                "separatorChar": ",",
                "escapeChar": "\\\\",
            },
        )
        ddl = CreateTable(table).compile(bind=conn)
        table.create(bind=conn)
        actual = Table(table_name, MetaData(schema=ENV.schema), autoload_with=conn)

        assert str(ddl) == textwrap.dedent(
            f"""
            CREATE EXTERNAL TABLE {ENV.schema}.{table_name} (
            \t{column_name} VARCHAR(10)
            )
            ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
            WITH SERDEPROPERTIES (
            \t'separatorChar' = ',',
            \t'escapeChar' = '\\\\'
            )
            LOCATION '{ENV.s3_staging_dir}{ENV.schema}/{table_name}/'
            """
        )
        dialect_opts = actual.dialect_options["awsathena"]
        assert dialect_opts["row_format"] == "SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'"
        assert (
            dialect_opts["file_format"] == "INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' "
            "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'"
        )
        assert dialect_opts["serdeproperties"]["separatorChar"] == ","
        assert dialect_opts["serdeproperties"]["escapeChar"] == "\\"
        assert dialect_opts["tblproperties"] is not None

    def test_create_table_grok(self, engine):
        engine, conn = engine
        table_name = "test_create_table_grok"
        column_name = "col"
        table = Table(
            table_name,
            MetaData(schema=ENV.schema),
            Column(column_name, types.String(10)),
            awsathena_location=f"{ENV.s3_staging_dir}{ENV.schema}/{table_name}/",
            awsathena_row_format="SERDE 'com.amazonaws.glue.serde.GrokSerDe'",
            awsathena_serdeproperties={
                "input.grokCustomPatterns": "POSTFIX_QUEUEID [0-9A-F]{7,12}",
                "input.format": "%%{SYSLOGBASE} %%{POSTFIX_QUEUEID:queue_id}: "
                "%%{GREEDYDATA:syslog_message}",
            },
            awsathena_file_format="INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' "
            "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'",
        )
        ddl = CreateTable(table).compile(bind=conn)
        table.create(bind=conn)
        actual = Table(table_name, MetaData(schema=ENV.schema), autoload_with=conn)

        assert str(ddl) == textwrap.dedent(
            f"""
            CREATE EXTERNAL TABLE {ENV.schema}.{table_name} (
            \t{column_name} VARCHAR(10)
            )
            ROW FORMAT SERDE 'com.amazonaws.glue.serde.GrokSerDe'
            WITH SERDEPROPERTIES (
            \t'input.grokCustomPatterns' = 'POSTFIX_QUEUEID [0-9A-F]{{7,12}}',
            \t'input.format' = '%%{{SYSLOGBASE}} %%{{POSTFIX_QUEUEID:queue_id}}: \
%%{{GREEDYDATA:syslog_message}}'
            )
            STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' \
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
            LOCATION '{ENV.s3_staging_dir}{ENV.schema}/{table_name}/'
            """
        )
        dialect_opts = actual.dialect_options["awsathena"]
        assert dialect_opts["row_format"] == "SERDE 'com.amazonaws.glue.serde.GrokSerDe'"
        assert (
            dialect_opts["file_format"] == "INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' "
            "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'"
        )
        assert (
            dialect_opts["serdeproperties"]["input.grokCustomPatterns"]
            == "POSTFIX_QUEUEID [0-9A-F]{7,12}"
        )
        assert (
            dialect_opts["serdeproperties"]["input.format"]
            == "%{SYSLOGBASE} %{POSTFIX_QUEUEID:queue_id}: "
            "%{GREEDYDATA:syslog_message}"
        )
        assert dialect_opts["tblproperties"] is not None

    def test_create_table_json(self, engine):
        engine, conn = engine
        table_name = "test_create_table_json"
        column_name = "col"
        table = Table(
            table_name,
            MetaData(schema=ENV.schema),
            Column(column_name, types.String(10)),
            awsathena_location=f"{ENV.s3_staging_dir}{ENV.schema}/{table_name}/",
            awsathena_row_format="SERDE 'org.openx.data.jsonserde.JsonSerDe'",
            awsathena_serdeproperties={
                "ignore.malformed.json": "1",
            },
        )
        ddl = CreateTable(table).compile(bind=conn)
        table.create(bind=conn)
        actual = Table(table_name, MetaData(schema=ENV.schema), autoload_with=conn)

        assert str(ddl) == textwrap.dedent(
            f"""
            CREATE EXTERNAL TABLE {ENV.schema}.{table_name} (
            \t{column_name} VARCHAR(10)
            )
            ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
            WITH SERDEPROPERTIES (
            \t'ignore.malformed.json' = '1'
            )
            LOCATION '{ENV.s3_staging_dir}{ENV.schema}/{table_name}/'
            """
        )
        dialect_opts = actual.dialect_options["awsathena"]
        assert dialect_opts["row_format"] == "SERDE 'org.openx.data.jsonserde.JsonSerDe'"
        assert (
            dialect_opts["file_format"] == "INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' "
            "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat'"
        )
        assert dialect_opts["serdeproperties"]["ignore.malformed.json"] == "1"
        assert dialect_opts["tblproperties"] is not None

    def test_create_table_parquet(self, engine):
        engine, conn = engine
        table_name = "test_create_table_parquet"
        column_name = "col"
        table = Table(
            table_name,
            MetaData(schema=ENV.schema),
            Column(column_name, types.String(10)),
            Column("year", types.String, awsathena_partition=True),
            awsathena_location=f"{ENV.s3_staging_dir}{ENV.schema}/{table_name}/",
            awsathena_file_format="PARQUET",
            awsathena_compression="ZSTD",
        )
        ddl = CreateTable(table).compile(bind=conn)
        table.create(bind=conn)
        actual = Table(table_name, MetaData(schema=ENV.schema), autoload_with=conn)

        assert str(ddl) == textwrap.dedent(
            f"""
            CREATE EXTERNAL TABLE {ENV.schema}.{table_name} (
            \t{column_name} VARCHAR(10)
            )
            PARTITIONED BY (
            \tyear STRING
            )
            STORED AS PARQUET
            LOCATION '{ENV.s3_staging_dir}{ENV.schema}/{table_name}/'
            TBLPROPERTIES (
            \t'parquet.compress' = 'ZSTD'
            )
            """
        )
        dialect_opts = actual.dialect_options["awsathena"]
        assert dialect_opts["compression"] == "ZSTD"
        assert (
            dialect_opts["row_format"]
            == "SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'"
        )
        assert (
            dialect_opts["file_format"]
            == "INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' "
            "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'"
        )
        assert dialect_opts["serdeproperties"] is not None
        assert dialect_opts["tblproperties"] is not None

    def test_create_table_orc(self, engine):
        engine, conn = engine
        table_name = "test_create_table_orc"
        column_name = "col"
        table = Table(
            table_name,
            MetaData(schema=ENV.schema),
            Column(column_name, types.String(10)),
            Column("year", types.String, awsathena_partition=True),
            awsathena_location=f"{ENV.s3_staging_dir}{ENV.schema}/{table_name}/",
            awsathena_file_format="ORC",
            awsathena_tblproperties={
                "orc.compress": "ZLIB",
            },
        )
        ddl = CreateTable(table).compile(bind=conn)
        table.create(bind=conn)
        actual = Table(table_name, MetaData(schema=ENV.schema), autoload_with=conn)

        assert str(ddl) == textwrap.dedent(
            f"""
            CREATE EXTERNAL TABLE {ENV.schema}.{table_name} (
            \t{column_name} VARCHAR(10)
            )
            PARTITIONED BY (
            \tyear STRING
            )
            STORED AS ORC
            LOCATION '{ENV.s3_staging_dir}{ENV.schema}/{table_name}/'
            TBLPROPERTIES (
            \t'orc.compress' = 'ZLIB'
            )
            """
        )
        dialect_opts = actual.dialect_options["awsathena"]
        assert dialect_opts["compression"] == "ZLIB"
        assert dialect_opts["row_format"] == "SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'"
        assert (
            dialect_opts["file_format"]
            == "INPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' "
            "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'"
        )
        assert dialect_opts["serdeproperties"] is not None
        assert dialect_opts["tblproperties"] is not None

    def test_create_table_avro(self, engine):
        engine, conn = engine
        table_name = "test_create_table_avro"
        column_name = "col"
        table = Table(
            table_name,
            MetaData(schema=ENV.schema),
            Column(column_name, types.String(10)),
            Column("year", types.String, awsathena_partition=True),
            awsathena_location=f"{ENV.s3_staging_dir}{ENV.schema}/{table_name}/",
            awsathena_file_format="AVRO",
            awsathena_row_format="SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'",
            awsathena_serdeproperties={
                "avro.schema.literal": textwrap.dedent(
                    """
                    {
                     "type" : "record",
                     "name" : "test_create_table_avro",
                     "namespace" : "default",
                     "fields" : [ {
                      "name" : "col",
                      "type" : [ "null", "string" ],
                      "default" : null
                     } ]
                    }
                    """
                ),
            },
        )
        ddl = CreateTable(table).compile(bind=conn)
        table.create(bind=conn)
        actual = Table(table_name, MetaData(schema=ENV.schema), autoload_with=conn)

        assert str(ddl) == textwrap.dedent(
            f"""
            CREATE EXTERNAL TABLE {ENV.schema}.{table_name} (
            \t{column_name} VARCHAR(10)
            )
            PARTITIONED BY (
            \tyear STRING
            )
            ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
            WITH SERDEPROPERTIES (
            \t'avro.schema.literal' = '
            {{
             "type" : "record",
             "name" : "test_create_table_avro",
             "namespace" : "default",
             "fields" : [ {{
              "name" : "col",
              "type" : [ "null", "string" ],
              "default" : null
             }} ]
            }}
            '
            )
            STORED AS AVRO
            LOCATION '{ENV.s3_staging_dir}{ENV.schema}/{table_name}/'
            """
        )
        dialect_opts = actual.dialect_options["awsathena"]
        assert dialect_opts["row_format"] == "SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'"
        assert (
            dialect_opts["file_format"]
            == "INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat' "
            "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'"
        )
        assert (
            dialect_opts["serdeproperties"]["avro.schema.literal"].replace("\n", "")
            == textwrap.dedent(
                """
                {
                 "type" : "record",
                 "name" : "test_create_table_avro",
                 "namespace" : "default",
                 "fields" : [ {
                 "name" : "col",
                 "type" : [ "null", "string" ],
                 "default" : null
                 } ]
                }
                """
            ).replace("\n", "")
        )
        assert dialect_opts["tblproperties"] is not None

    def test_create_table_with_comments(self, engine):
        engine, conn = engine
        table_name = "test_create_table_with_comments"
        table_comment = textwrap.dedent(
            """
            table comment

            multiline table comment
            """
        )
        column_name = "col"
        column_comment = "column comment"
        table = Table(
            table_name,
            MetaData(schema=ENV.schema),
            Column(column_name, types.String(10), comment=column_comment),
            awsathena_location=f"{ENV.s3_staging_dir}{ENV.schema}/{table_name}/",
            awsathena_file_format="PARQUET",
            comment=table_comment,
        )
        ddl = CreateTable(table).compile(bind=conn)
        table.create(bind=conn)
        actual = Table(table_name, MetaData(schema=ENV.schema), autoload_with=conn)

        assert str(ddl) == textwrap.dedent(
            f"""
            CREATE EXTERNAL TABLE {ENV.schema}.{table_name} (
            \t{column_name} VARCHAR(10) COMMENT '{column_comment}'
            )
            COMMENT '
            table comment

            multiline table comment
            '
            STORED AS PARQUET
            LOCATION '{ENV.s3_staging_dir}{ENV.schema}/{table_name}/'
            """
        )
        assert actual.c[column_name].comment == column_comment
        # The AWS API seems to return comments with squashed whitespace and line breaks.
        # assert actual.comment == table.comment
        assert actual.comment
        assert actual.comment.strip() == re.sub(r"\s+", " ", table_comment.strip())

    def test_create_table_with_special_character_comments(self, engine):
        engine, conn = engine
        table_name = "test_create_table_with_special_character_comments"
        column_name = "col"
        comment = "%%str%% %str% %(parameter)s \"s''t'r\""
        table = Table(
            table_name,
            MetaData(schema=ENV.schema),
            Column(column_name, types.String(10), comment=comment),
            awsathena_location=f"{ENV.s3_staging_dir}{ENV.schema}/{table_name}/",
            comment=comment,
        )
        table.create(bind=conn)
        actual = Table(table_name, MetaData(schema=ENV.schema), autoload_with=conn)
        assert actual.comment == comment
        assert actual.c[column_name].comment == comment

    def test_create_table_with_primary_key(self, engine):
        engine, conn = engine
        table_name = "test_create_table_with_primary_key"
        table = Table(
            table_name,
            MetaData(schema=ENV.schema),
            Column("pk", types.Integer, primary_key=True),
            awsathena_file_format="PARQUET",
            awsathena_location=f"{ENV.s3_staging_dir}{ENV.schema}/{table_name}/",
        )
        ddl = CreateTable(table).compile(bind=conn)
        # The table will be created, but Athena does not support primary keys.
        table.create(bind=conn)
        actual = Table(table_name, MetaData(schema=ENV.schema), autoload_with=conn)

        assert str(ddl) == textwrap.dedent(
            f"""
            CREATE EXTERNAL TABLE {ENV.schema}.{table_name} (
            \tpk INT
            )
            STORED AS PARQUET
            LOCATION '{ENV.s3_staging_dir}{ENV.schema}/{table_name}/'
            """
        )
        assert len(actual.primary_key.columns) == 0

    def test_create_table_with_varchar_text_column(self, engine):
        engine, conn = engine
        table_name = "test_create_table_with_varchar_text_column"
        table = Table(
            table_name,
            MetaData(schema=ENV.schema),
            Column("col_varchar", types.String()),
            Column("col_varchar_length", types.String(10)),
            Column("col_varchar_type", types.String),
            Column("col_text", types.Text),
            awsathena_location=f"{ENV.s3_staging_dir}{ENV.schema}/{table_name}/",
            awsathena_file_format="PARQUET",
            awsathena_compression="SNAPPY",
        )
        ddl = CreateTable(table).compile(bind=conn)
        table.create(bind=conn)
        actual = Table(table_name, MetaData(schema=ENV.schema), autoload_with=conn)

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
            TBLPROPERTIES (
            \t'parquet.compress' = 'SNAPPY'
            )
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
            sqlalchemy.select(expression.cast(one_row.c.number_of_rows, types.VARCHAR))
        ).scalar()
        assert actual == "1"

        # varchar with length
        actual = conn.execute(
            sqlalchemy.select(expression.cast(one_row.c.number_of_rows, types.VARCHAR(10)))
        ).scalar()
        assert actual == "1"

    def test_cast_as_binary(self, engine):
        engine, conn = engine
        one_row_complex = Table("one_row_complex", MetaData(schema=ENV.schema), autoload_with=conn)
        actual = conn.execute(
            sqlalchemy.select(
                expression.cast(one_row_complex.c.col_string, types.BINARY),
                expression.cast(one_row_complex.c.col_varchar, types.VARBINARY),
            )
        ).one()
        assert actual[0] == b"a string"
        assert actual[1] == b"varchar"

    def test_create_table_with_partition(self, engine):
        engine, conn = engine
        table_name = "test_create_table_with_partition"
        table_comment = "table comment"
        column_comment = "column comment"
        table = Table(
            table_name,
            MetaData(schema=ENV.schema),
            Column("col_1", types.String(10), comment=column_comment),
            Column(
                "col_partition_1",
                types.String(10),
                awsathena_partition=True,
                comment=column_comment,
            ),
            Column("col_partition_2", types.Integer, awsathena_partition=True),
            Column("col_2", types.Integer),
            awsathena_location=f"{ENV.s3_staging_dir}{ENV.schema}/{table_name}/",
            awsathena_file_format="PARQUET",
            awsathena_compression="SNAPPY",
            comment=table_comment,
        )
        ddl = CreateTable(table).compile(bind=conn)
        table.create(bind=conn)
        actual = Table(table_name, MetaData(schema=ENV.schema), autoload_with=conn)

        assert str(ddl) == textwrap.dedent(
            f"""
            CREATE EXTERNAL TABLE {ENV.schema}.{table_name} (
            \tcol_1 VARCHAR(10) COMMENT '{column_comment}',
            \tcol_2 INT
            )
            COMMENT '{table_comment}'
            PARTITIONED BY (
            \tcol_partition_1 VARCHAR(10) COMMENT '{column_comment}',
            \tcol_partition_2 INT
            )
            STORED AS PARQUET
            LOCATION '{ENV.s3_staging_dir}{ENV.schema}/{table_name}/'
            TBLPROPERTIES (
            \t'parquet.compress' = 'SNAPPY'
            )
            """
        )
        assert not actual.c.col_1.dialect_options["awsathena"]["partition"]
        assert not actual.c.col_2.dialect_options["awsathena"]["partition"]
        assert actual.c.col_partition_1.dialect_options["awsathena"]["partition"]
        assert actual.c.col_partition_2.dialect_options["awsathena"]["partition"]

    def test_create_table_with_date_partition(self, engine):
        engine, conn = engine
        table_name = "test_create_table_with_date_partition"
        table = Table(
            table_name,
            MetaData(schema=ENV.schema),
            Column("col_1", types.String(10)),
            Column("col_2", types.Integer),
            Column("dt", types.String, awsathena_partition=True),
            awsathena_location=f"{ENV.s3_staging_dir}{ENV.schema}/{table_name}/",
            awsathena_file_format="PARQUET",
            awsathena_compression="SNAPPY",
            awsathena_tblproperties={
                "projection.enabled": "true",
                "projection.dt.type": "date",
                "projection.dt.range": "NOW-1YEARS,NOW",
                "projection.dt.format": "yyyy-MM-dd",
            },
        )
        ddl = CreateTable(table).compile(bind=conn)
        table.create(bind=conn)
        actual = Table(table_name, MetaData(schema=ENV.schema), autoload_with=conn)

        assert str(ddl) == textwrap.dedent(
            f"""
            CREATE EXTERNAL TABLE {ENV.schema}.{table_name} (
            \tcol_1 VARCHAR(10),
            \tcol_2 INT
            )
            PARTITIONED BY (
            \tdt STRING
            )
            STORED AS PARQUET
            LOCATION '{ENV.s3_staging_dir}{ENV.schema}/{table_name}/'
            TBLPROPERTIES (
            \t'projection.enabled' = 'true',
            \t'projection.dt.type' = 'date',
            \t'projection.dt.range' = 'NOW-1YEARS,NOW',
            \t'projection.dt.format' = 'yyyy-MM-dd',
            \t'parquet.compress' = 'SNAPPY'
            )
            """
        )
        assert actual.c.dt.dialect_options["awsathena"]["partition"]
        tblproperties = actual.dialect_options["awsathena"]["tblproperties"]
        assert tblproperties["projection.enabled"] == "true"
        assert tblproperties["projection.dt.type"] == "date"
        assert tblproperties["projection.dt.range"] == "NOW-1YEARS,NOW"
        assert tblproperties["projection.dt.format"] == "yyyy-MM-dd"

    def test_insert_from_select_cte_follows_insert_one(self, engine):
        engine, conn = engine
        metadata = MetaData(schema=ENV.schema)
        table_name = "select_cte_insert_one_1"
        table = Table(
            table_name,
            metadata,
            Column("id", types.Integer),
            Column("name", types.String(30)),
            Column("description", types.String(30)),
            awsathena_location=f"{ENV.s3_staging_dir}{ENV.schema}/{table_name}/",
            awsathena_file_format="PARQUET",
            awsathena_compression="SNAPPY",
        )
        other_table_name = "select_cte_insert_one_2"
        other_table = Table(
            other_table_name,
            metadata,
            Column("id", types.Integer, primary_key=True),
            Column("name", types.String(30)),
            awsathena_location=f"{ENV.s3_staging_dir}{ENV.schema}/{other_table_name}/",
            awsathena_file_format="PARQUET",
            awsathena_compression="SNAPPY",
        )

        cte = sqlalchemy.select(table.c.name).where(table.c.name == "bar").cte()
        sel = sqlalchemy.select(table.c.id, table.c.name).where(table.c.name == cte.c.name)
        ins = other_table.insert().from_select(("id", "name"), sel)

        table.create(bind=conn)
        other_table.create(bind=conn)
        conn.execute(
            table.insert(),
            [
                {"id": 1, "name": "foo", "description": "description foo"},
                {"id": 2, "name": "bar", "description": "description bar"},
            ],
        )
        conn.execute(ins)
        actual = conn.execute(sqlalchemy.select(other_table)).fetchall()

        assert (
            str(ins)
            == textwrap.dedent(
                f"""
                WITH anon_1 AS \n\
                (SELECT {ENV.schema}.{table_name}.name AS name \n\
                FROM {ENV.schema}.{table_name} \n\
                WHERE {ENV.schema}.{table_name}.name = :name_1)
                 INSERT INTO {ENV.schema}.{other_table_name} (id, name) \
SELECT {ENV.schema}.{table_name}.id, {ENV.schema}.{table_name}.name \n\
                FROM {ENV.schema}.{table_name}, anon_1 \n\
                WHERE {ENV.schema}.{table_name}.name = anon_1.name \n\
                """
            ).strip()
        )
        assert actual == [(2, "bar")]
