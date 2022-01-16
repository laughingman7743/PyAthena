# -*- coding: utf-8 -*-
import math
import numbers
import re
from distutils.util import strtobool

import tenacity
from sqlalchemy import exc, schema, util
from sqlalchemy.engine import Engine, reflection
from sqlalchemy.engine.default import DefaultDialect
from sqlalchemy.exc import NoSuchTableError, OperationalError
from sqlalchemy.sql.compiler import (
    DDLCompiler,
    GenericTypeCompiler,
    IdentifierPreparer,
    SQLCompiler,
)
from sqlalchemy.sql.sqltypes import (
    BIGINT,
    BINARY,
    BOOLEAN,
    DATE,
    DECIMAL,
    FLOAT,
    INTEGER,
    NULLTYPE,
    STRINGTYPE,
    TIMESTAMP,
)
from tenacity import retry_if_exception, stop_after_attempt, wait_exponential

import pyathena


class UniversalSet(object):
    """UniversalSet

    https://github.com/dropbox/PyHive/blob/master/pyhive/common.py"""

    def __contains__(self, item):
        return True


class AthenaDMLIdentifierPreparer(IdentifierPreparer):
    """PrestoIdentifierPreparer

    https://github.com/dropbox/PyHive/blob/master/pyhive/sqlalchemy_presto.py"""

    reserved_words = UniversalSet()


class AthenaDDLIdentifierPreparer(IdentifierPreparer):
    def __init__(
        self,
        dialect,
        initial_quote="`",
        final_quote=None,
        escape_quote="`",
        quote_case_sensitive_collations=True,
        omit_schema=False,
    ):
        super(AthenaDDLIdentifierPreparer, self).__init__(
            dialect=dialect,
            initial_quote=initial_quote,
            final_quote=final_quote,
            escape_quote=escape_quote,
            quote_case_sensitive_collations=quote_case_sensitive_collations,
            omit_schema=omit_schema,
        )


class AthenaStatementCompiler(SQLCompiler):
    """PrestoCompiler

    https://github.com/dropbox/PyHive/blob/master/pyhive/sqlalchemy_presto.py"""

    def visit_char_length_func(self, fn, **kw):
        return "length{0}".format(self.function_argspec(fn, **kw))


class AthenaTypeCompiler(GenericTypeCompiler):
    def visit_FLOAT(self, type_, **kw):
        return self.visit_REAL(type_, **kw)

    def visit_REAL(self, type_, **kw):
        return "DOUBLE"

    def visit_NUMERIC(self, type_, **kw):
        return self.visit_DECIMAL(type_, **kw)

    def visit_DECIMAL(self, type_, **kw):
        if type_.precision is None:
            return "DECIMAL"
        elif type_.scale is None:
            return "DECIMAL(%(precision)s)" % {"precision": type_.precision}
        else:
            return "DECIMAL(%(precision)s, %(scale)s)" % {
                "precision": type_.precision,
                "scale": type_.scale,
            }

    def visit_INTEGER(self, type_, **kw):
        return "INTEGER"

    def visit_SMALLINT(self, type_, **kw):
        return "SMALLINT"

    def visit_BIGINT(self, type_, **kw):
        return "BIGINT"

    def visit_TIMESTAMP(self, type_, **kw):
        return "TIMESTAMP"

    def visit_DATETIME(self, type_, **kw):
        return self.visit_TIMESTAMP(type_, **kw)

    def visit_DATE(self, type_, **kw):
        return "DATE"

    def visit_TIME(self, type_, **kw):
        raise exc.CompileError("Data type `{0}` is not supported".format(type_))

    def visit_CLOB(self, type_, **kw):
        return self.visit_BINARY(type_, **kw)

    def visit_NCLOB(self, type_, **kw):
        return self.visit_BINARY(type_, **kw)

    def visit_CHAR(self, type_, **kw):
        return self._render_string_type(type_, "CHAR")

    def visit_NCHAR(self, type_, **kw):
        return self._render_string_type(type_, "CHAR")

    def visit_VARCHAR(self, type_, **kw):
        return self._render_string_type(type_, "VARCHAR")

    def visit_NVARCHAR(self, type_, **kw):
        return self._render_string_type(type_, "VARCHAR")

    def visit_TEXT(self, type_, **kw):
        return "STRING"

    def visit_BLOB(self, type_, **kw):
        return self.visit_BINARY(type_, **kw)

    def visit_BINARY(self, type_, **kw):
        return "BINARY"

    def visit_VARBINARY(self, type_, **kw):
        return self.visit_BINARY(type_, **kw)

    def visit_BOOLEAN(self, type_, **kw):
        return "BOOLEAN"


class AthenaDDLCompiler(DDLCompiler):
    @property
    def preparer(self):
        return self._preparer

    @preparer.setter
    def preparer(self, value):
        pass

    def __init__(
        self,
        dialect,
        statement,
        schema_translate_map=None,
        compile_kwargs=util.immutabledict(),
    ):
        self._preparer = AthenaDDLIdentifierPreparer(dialect)
        super(AthenaDDLCompiler, self).__init__(
            dialect=dialect,
            statement=statement,
            schema_translate_map=schema_translate_map,
            compile_kwargs=compile_kwargs,
        )

    def visit_create_table(self, create):
        table = create.element
        preparer = self.preparer

        text = "\nCREATE EXTERNAL "
        text += "TABLE " + preparer.format_table(table) + " "
        text += "("

        separator = "\n"
        for create_column in create.columns:
            column = create_column.element
            try:
                processed = self.process(create_column)
                if processed is not None:
                    text += separator
                    separator = ", \n"
                    text += "\t" + processed
            except exc.CompileError as ce:
                util.raise_from_cause(
                    exc.CompileError(
                        util.u("(in table '{0}', column '{1}'): {2}").format(
                            table.description, column.name, ce.args[0]
                        )
                    )
                )

        const = self.create_table_constraints(
            table,
            _include_foreign_key_constraints=create.include_foreign_key_constraints,
        )
        if const:
            text += separator + "\t" + const

        text += "\n)\n%s\n\n" % self.post_create_table(table)
        return text

    def post_create_table(self, table):
        dialect_opts = table.dialect_options["awsathena"]
        raw_connection = (
            table.bind.raw_connection()
            if hasattr(table, "bind") and table.bind
            else None
        )
        # TODO Supports orc, avro, json, csv or tsv format
        text = "STORED AS PARQUET\n"

        if dialect_opts["location"]:
            location = dialect_opts["location"]
            location += "/" if location[-1] != "/" else ""
        elif raw_connection:
            base_location = (
                raw_connection._kwargs["s3_dir"]
                if "s3_dir" in raw_connection._kwargs
                else raw_connection.s3_staging_dir
            )
            schema = table.schema if table.schema else raw_connection.schema_name
            location = f"{base_location}{schema}/{table.name}/"
        else:
            location = None
        if not location:
            if raw_connection:
                raise exc.CompileError(
                    "`s3_dir` or `s3_staging_dir` parameter is required"
                    " in the connection string."
                )
            else:
                raise exc.CompileError(
                    "You need to specify the storage location for the table "
                    "using the `awsathena_location` dialect keyword argument"
                )
        text += f"LOCATION '{location}'\n"

        if dialect_opts["compression"]:
            compression = dialect_opts["compression"]
        elif raw_connection:
            raw_connection = table.bind.raw_connection()
            compression = raw_connection._kwargs.get("compression")
        else:
            compression = None
        if compression:
            text += "TBLPROPERTIES ('parquet.compress'='{0}')\n".format(
                compression.upper()
            )

        return text


_TYPE_MAPPINGS = {
    "boolean": BOOLEAN,
    "real": FLOAT,
    "float": FLOAT,
    "double": FLOAT,
    "tinyint": INTEGER,
    "smallint": INTEGER,
    "integer": INTEGER,
    "bigint": BIGINT,
    "decimal": DECIMAL,
    "char": STRINGTYPE,
    "varchar": STRINGTYPE,
    "array": STRINGTYPE,
    "row": STRINGTYPE,  # StructType
    "varbinary": BINARY,
    "map": STRINGTYPE,
    "date": DATE,
    "timestamp": TIMESTAMP,
}


class AthenaDialect(DefaultDialect):

    name = "awsathena"
    preparer = AthenaDMLIdentifierPreparer
    statement_compiler = AthenaStatementCompiler
    ddl_compiler = AthenaDDLCompiler
    type_compiler = AthenaTypeCompiler
    default_paramstyle = pyathena.paramstyle
    supports_alter = False
    supports_pk_autoincrement = False
    supports_default_values = False
    supports_empty_insert = False
    supports_multivalues_insert = True
    supports_native_decimal = True
    supports_native_boolean = True
    supports_unicode_statements = True
    supports_unicode_binds = True
    returns_unicode_strings = True
    description_encoding = None
    supports_native_boolean = True
    postfetch_lastrowid = False
    construct_arguments = [
        (
            schema.Table,
            {
                "location": None,
                "compression": None,
            },
        ),
    ]

    _pattern_data_catlog_exception = re.compile(
        r"(((Database|Namespace)\ (?P<schema>.+))|(Table\ (?P<table>.+)))\ not\ found\."
    )
    _pattern_column_type = re.compile(r"^([a-zA-Z]+)($|\(.+\)$)")

    @classmethod
    def dbapi(cls):
        return pyathena

    def _raw_connection(self, connection):
        if isinstance(connection, Engine):
            return connection.raw_connection()
        return connection.connection

    def create_connect_args(self, url):
        # Connection string format:
        #   awsathena+rest://
        #   {aws_access_key_id}:{aws_secret_access_key}@athena.{region_name}.amazonaws.com:443/
        #   {schema_name}?s3_staging_dir={s3_staging_dir}&...
        opts = {
            "aws_access_key_id": url.username if url.username else None,
            "aws_secret_access_key": url.password if url.password else None,
            "region_name": re.sub(
                r"^athena\.([a-z0-9-]+)\.amazonaws\.(com|com.cn)$", r"\1", url.host
            ),
            "schema_name": url.database if url.database else "default",
        }
        opts.update(url.query)
        if "verify" in opts:
            verify = opts["verify"]
            try:
                verify = bool(strtobool(verify))
            except ValueError:
                # Probably a file name of the CA cert bundle to use
                pass
            opts.update({"verify": verify})
        if "duration_seconds" in opts:
            opts.update({"duration_seconds": int(url.query["duration_seconds"])})
        if "poll_interval" in opts:
            opts.update({"poll_interval": float(url.query["poll_interval"])})
        if "kill_on_interrupt" in opts:
            opts.update(
                {"kill_on_interrupt": bool(strtobool(url.query["kill_on_interrupt"]))}
            )
        return [[], opts]

    @reflection.cache
    def get_schema_names(self, connection, **kw):
        query = """
                SELECT schema_name
                FROM information_schema.schemata
                WHERE schema_name NOT IN ('information_schema')
                """
        return [row.schema_name for row in connection.execute(query).fetchall()]

    @reflection.cache
    def _get_tables(self, connection, schema=None, **kw):
        raw_connection = self._raw_connection(connection)
        schema = schema if schema else raw_connection.schema_name
        tables = []
        next_token = None
        with raw_connection.connection.cursor() as cursor:
            while True:
                next_token, response = cursor._list_table_metadata(
                    schema_name=schema, next_token=next_token
                )
                tables.extend(response)
                if not next_token:
                    break
        return tables

    def get_table_names(self, connection, schema=None, **kw):
        tables = self._get_tables(connection, schema, **kw)
        # In Athena, only EXTERNAL_TABLE is supported.
        # https://docs.aws.amazon.com/athena/latest/APIReference/API_TableMetadata.html
        return [t.name for t in tables if t.table_type == "EXTERNAL_TABLE"]

    def get_view_names(self, connection, schema=None, **kw):
        tables = self._get_tables(connection, schema, **kw)
        return [t.name for t in tables if t.table_type == "VIRTUAL_VIEW"]

    def has_table(self, connection, table_name, schema=None, **kw):
        try:
            columns = self.get_columns(connection, table_name, schema)
            return True if columns else False
        except NoSuchTableError:
            return False

    @reflection.cache
    def get_columns(self, connection, table_name, schema=None, **kw):
        raw_connection = self._raw_connection(connection)
        schema = schema if schema else raw_connection.schema_name
        query = """
                SELECT
                  table_schema,
                  table_name,
                  column_name,
                  data_type,
                  is_nullable,
                  column_default,
                  ordinal_position,
                  comment
                FROM information_schema.columns
                WHERE table_schema = '{schema}'
                AND table_name = '{table}'
                """.format(
            schema=schema, table=table_name
        )
        retry_config = raw_connection.retry_config
        retry = tenacity.Retrying(
            retry=retry_if_exception(
                lambda exc: self._retry_if_data_catalog_exception(  # type: ignore
                    exc, schema, table_name
                )
            ),
            stop=stop_after_attempt(retry_config.attempt),
            wait=wait_exponential(
                multiplier=retry_config.multiplier,
                max=retry_config.max_delay,
                exp_base=retry_config.exponential_base,
            ),
            reraise=True,
        )
        try:
            return [
                {
                    "name": row.column_name,
                    "type": _TYPE_MAPPINGS.get(
                        self._get_column_type(row.data_type), NULLTYPE
                    ),
                    "nullable": True if row.is_nullable == "YES" else False,
                    "default": row.column_default
                    if not self._is_nan(row.column_default)
                    else None,
                    "ordinal_position": row.ordinal_position,
                    "comment": row.comment,
                }
                for row in retry(connection.execute, query).fetchall()
            ]
        except OperationalError as e:
            if not self._retry_if_data_catalog_exception(e, schema, table_name):
                raise NoSuchTableError(table_name) from e
            else:
                raise e

    def _retry_if_data_catalog_exception(self, exc, schema, table_name):
        if not isinstance(exc, OperationalError):
            return False

        match = self._pattern_data_catlog_exception.search(str(exc))
        if match and (
            match.group("schema") == schema or match.group("table") == table_name
        ):
            return False
        return True

    def _get_column_type(self, type_):
        return self._pattern_column_type.sub(r"\1", type_)

    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        # Athena has no support for foreign keys.
        return []  # pragma: no cover

    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        # Athena has no support for primary keys.
        return []  # pragma: no cover

    def get_indexes(self, connection, table_name, schema=None, **kw):
        # Athena has no support for indexes.
        return []  # pragma: no cover

    def do_rollback(self, dbapi_connection):
        # No transactions for Athena
        pass  # pragma: no cover

    def _check_unicode_returns(self, connection, additional_tests=None):
        # Requests gives back Unicode strings
        return True  # pragma: no cover

    def _check_unicode_description(self, connection):
        # Requests gives back Unicode strings
        return True  # pragma: no cover

    def _is_nan(self, column_default):
        return isinstance(column_default, numbers.Real) and math.isnan(column_default)


class AthenaRestDialect(AthenaDialect):
    driver = "rest"
