# -*- coding: utf-8 -*-
import re
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
from sqlalchemy.exc import NoSuchTableError, OperationalError
from sqlalchemy.sql import expression
from sqlalchemy.sql.ddl import CreateTable
from sqlalchemy.sql.schema import Column, MetaData, Table
from sqlalchemy.sql.selectable import TextualSelect

from tests.pyathena.conftest import ENV


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
        actual = insp.get_view_definition(schema=ENV.schema, view_name="v_one_row")
        assert ([r for r in actual] == [
            f"CREATE VIEW {ENV.schema}.v_one_row AS",
            "SELECT number_of_rows",
            "FROM",
            f"  {ENV.schema}.one_row",
        ]
                )

    def test_get_view_definition_missing_view(self, engine):
        engine, conn = engine
        insp = sqlalchemy.inspect(engine)
        pytest.raises(
            OperationalError,
            lambda: insp.get_view_definition(schema=ENV.schema, view_name="test-view")
        )
