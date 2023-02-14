# -*- coding: utf-8 -*-
import re
from distutils.util import strtobool

import botocore
from sqlalchemy import exc, schema, types, util
from sqlalchemy.engine import Engine, reflection
from sqlalchemy.engine.default import DefaultDialect
from sqlalchemy.exc import NoSuchTableError
from sqlalchemy.sql.compiler import (
    DDLCompiler,
    GenericTypeCompiler,
    IdentifierPreparer,
    SQLCompiler,
)

import pyathena
from pyathena.model import AthenaFileFormat, AthenaRowFormatSerde

# https://docs.aws.amazon.com/athena/latest/ug/reserved-words.html#list-of-ddl-reserved-words
DDL_RESERVED_WORDS = [
    "ALL",
    "ALTER",
    "AND",
    "ARRAY",
    "AS",
    "AUTHORIZATION",
    "BETWEEN",
    "BIGINT",
    "BINARY",
    "BOOLEAN",
    "BOTH",
    "BY",
    "CASE",
    "CASHE",
    "CAST",
    "CHAR",
    "COLUMN",
    "COMMIT",
    "CONF",
    "CONSTRAINT",
    "CREATE",
    "CROSS",
    "CUBE",
    "CURRENT",
    "CURRENT_DATE",
    "CURRENT_TIMESTAMP",
    "CURSOR",
    "DATABASE",
    "DATE",
    "DAYOFWEEK",
    "DECIMAL",
    "DELETE",
    "DESCRIBE",
    "DISTINCT",
    "DOUBLE",
    "DROP",
    "ELSE",
    "END",
    "EXCHANGE",
    "EXISTS",
    "EXTENDED",
    "EXTERNAL",
    "EXTRACT",
    "FALSE",
    "FETCH",
    "FLOAT",
    "FLOOR",
    "FOLLOWING",
    "FOR",
    "FOREIGN",
    "FROM",
    "FULL",
    "FUNCTION",
    "GRANT",
    "GROUP",
    "GROUPING",
    "HAVING",
    "IF",
    "IMPORT",
    "IN",
    "INNER",
    "INSERT",
    "INT",
    "INTEGER",
    "INTERSECT",
    "INTERVAL",
    "INTO",
    "IS",
    "JOIN",
    "LATERAL",
    "LEFT",
    "LESS",
    "LIKE",
    "LOCAL",
    "MACRO",
    "MAP",
    "MORE",
    "NONE",
    "NOT",
    "NULL",
    "NUMERIC",
    "OF",
    "ON",
    "ONLY",
    "OR",
    "ORDER",
    "OUT",
    "OUTER",
    "OVER",
    "PARTIALSCAN",
    "PARTITION",
    "PERCENT",
    "PRECEDING",
    "PRECISION",
    "PRESERVE",
    "PRIMARY",
    "PROCEDURE",
    "RANGE",
    "READS",
    "REDUCE",
    "REFERENCES",
    "REGEXP",
    "REVOKE",
    "RIGHT",
    "RLIKE",
    "ROLLBACK",
    "ROLLUP",
    "ROW",
    "ROWS",
    "SELECT",
    "SET",
    "SMALLINT",
    "START",
    "TABLE",
    "TABLESAMPLE",
    "THEN",
    "TIME",
    "TIMESTAMP",
    "TO",
    "TRANSFORM",
    "TRIGGER",
    "TRUE",
    "TRUNCATE",
    "UNBOUNDED",
    "UNION",
    "UNIQUEJOIN",
    "UPDATE",
    "USER",
    "USING",
    "UTC_TIMESTAMP",
    "VALUES",
    "VARCHAR",
    "VIEWS",
    "WHEN",
    "WHERE",
    "WINDOW",
    "WITH",
]
# https://docs.aws.amazon.com/athena/latest/ug/reserved-words.html#list-of-reserved-words-sql-select
SELECT_STATEMENT_RESERVED_WORDS = [
    "ALL",
    "ALTER",
    "AND",
    "ARRAY",
    "AS",
    "AUTHORIZATION",
    "BETWEEN",
    "BIGINT",
    "BINARY",
    "BOOLEAN",
    "BOTH",
    "BY",
    "CASE",
    "CASHE",
    "CAST",
    "CHAR",
    "COLUMN",
    "COMMIT",
    "CONF",
    "CONSTRAINT",
    "CREATE",
    "CROSS",
    "CUBE",
    "CURRENT",
    "CURRENT_DATE",
    "CURRENT_TIMESTAMP",
    "CURSOR",
    "DATABASE",
    "DATE",
    "DAYOFWEEK",
    "DECIMAL",
    "DELETE",
    "DESCRIBE",
    "DISTINCT",
    "DOUBLE",
    "DROP",
    "ELSE",
    "END",
    "EXCHANGE",
    "EXISTS",
    "EXTENDED",
    "EXTERNAL",
    "EXTRACT",
    "FALSE",
    "FETCH",
    "FLOAT",
    "FLOOR",
    "FOLLOWING",
    "FOR",
    "FOREIGN",
    "FROM",
    "FULL",
    "FUNCTION",
    "GRANT",
    "GROUP",
    "GROUPING",
    "HAVING",
    "IF",
    "IMPORT",
    "IN",
    "INNER",
    "INSERT",
    "INT",
    "INTEGER",
    "INTERSECT",
    "INTERVAL",
    "INTO",
    "IS",
    "JOIN",
    "LATERAL",
    "LEFT",
    "LESS",
    "LIKE",
    "LOCAL",
    "MACRO",
    "MAP",
    "MORE",
    "NONE",
    "NOT",
    "NULL",
    "NUMERIC",
    "OF",
    "ON",
    "ONLY",
    "OR",
    "ORDER",
    "OUT",
    "OUTER",
    "OVER",
    "PARTIALSCAN",
    "PARTITION",
    "PERCENT",
    "PRECEDING",
    "PRECISION",
    "PRESERVE",
    "PRIMARY",
    "PROCEDURE",
    "RANGE",
    "READS",
    "REDUCE",
    "REFERENCES",
    "REGEXP",
    "REVOKE",
    "RIGHT",
    "RLIKE",
    "ROLLBACK",
    "ROLLUP",
    "ROW",
    "ROWS",
    "SELECT",
    "SET",
    "SMALLINT",
    "START",
    "TABLE",
    "TABLESAMPLE",
    "THEN",
    "TIME",
    "TIMESTAMP",
    "TO",
    "TRANSFORM",
    "TRIGGER",
    "TRUE",
    "TRUNCATE",
    "UNBOUNDED",
    "UNION",
    "UNIQUEJOIN",
    "UPDATE",
    "USER",
    "USING",
    "UTC_TIMESTAMP",
    "VALUES",
    "VARCHAR",
    "VIEWS",
    "WHEN",
    "WHERE",
    "WINDOW",
    "WITH",
]
RESERVED_WORDS = list(sorted(set(DDL_RESERVED_WORDS + SELECT_STATEMENT_RESERVED_WORDS)))


class AthenaDMLIdentifierPreparer(IdentifierPreparer):
    reserved_words = RESERVED_WORDS


class HashableDict(dict):  # type: ignore
    def __hash__(self):
        return hash(tuple(sorted(self.items())))


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
    def visit_char_length_func(self, fn, **kw):
        return f"length{self.function_argspec(fn, **kw)}"

    def visit_cast(self, cast, **kwargs):
        if isinstance(cast.type, types.VARCHAR) and cast.type.length is None:
            type_clause = "VARCHAR"
        elif isinstance(cast.type, types.CHAR) and cast.type.length is None:
            type_clause = "CHAR"
        elif isinstance(cast.type, (types.BINARY, types.VARBINARY)):
            type_clause = "VARBINARY"
        else:
            type_clause = cast.typeclause._compiler_dispatch(self, **kwargs)
        return f"CAST({cast.clause._compiler_dispatch(self, **kwargs)} AS {type_clause})"

    def limit_clause(self, select, **kw):
        text = []
        # https://docs.sqlalchemy.org/en/14/core/connections.html#example-rendering-limit-offset-with-post-compile-parameters
        if hasattr(select, "_simple_int_clause"):
            offset_clause = select._offset_clause
            if offset_clause is not None and select._simple_int_clause(offset_clause):
                text.append(f" OFFSET {self.process(offset_clause.render_literal_execute(), **kw)}")
            limit_clause = select._limit_clause
            if limit_clause is not None and select._simple_int_clause(limit_clause):
                text.append(f" LIMIT {self.process(limit_clause.render_literal_execute(), **kw)}")
        else:
            # SQLAlchemy < 1.4
            if select._offset_clause is not None:
                text.append(" OFFSET " + self.process(select._offset_clause, **kw))
            if select._limit_clause is not None:
                text.append(" LIMIT " + self.process(select._limit_clause, **kw))
        return "\n".join(text)


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
            return f"DECIMAL({type_.precision})"
        else:
            return f"DECIMAL({type_.precision}, {type_.scale})"

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
        raise exc.CompileError(f"Data type `{type_}` is not supported")

    def visit_CLOB(self, type_, **kw):
        return self.visit_BINARY(type_, **kw)

    def visit_NCLOB(self, type_, **kw):
        return self.visit_BINARY(type_, **kw)

    def visit_CHAR(self, type_, **kw):
        if type_.length:
            return self._render_string_type(type_, "CHAR")
        return "STRING"

    def visit_NCHAR(self, type_, **kw):
        return self.visit_CHAR(type_, **kw)

    def visit_VARCHAR(self, type_, **kw):
        if type_.length:
            return self._render_string_type(type_, "VARCHAR")
        return "STRING"

    def visit_NVARCHAR(self, type_, **kw):
        return self.visit_VARCHAR(type_, **kw)

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

    def _escape_comment(self, value):
        value = value.replace("\\", "\\\\").replace("'", r"\'")
        # DDL statements raise a KeyError if the placeholders aren't escaped
        if self.dialect.identifier_preparer._double_percents:
            value = value.replace("%", "%%")
        return f"'{value}'"

    def _get_comment_specification(self, comment):
        return f"COMMENT {self._escape_comment(comment)}"

    def _get_bucket_count(self, dialect_opts, connect_opts):
        if dialect_opts["bucket_count"]:
            bucket_count = dialect_opts["bucket_count"]
        elif connect_opts:
            bucket_count = connect_opts.get("bucket_count")
        else:
            bucket_count = None
        return bucket_count

    def _get_file_format(self, dialect_opts, connect_opts):
        if dialect_opts["file_format"]:
            file_format = dialect_opts["file_format"]
        elif connect_opts:
            file_format = connect_opts.get("file_format")
        else:
            file_format = None
        return file_format

    def _get_file_format_specification(self, dialect_opts, connect_opts):
        file_format = self._get_file_format(dialect_opts, connect_opts)
        text = []
        if file_format:
            text.append(f"STORED AS {file_format}")
        return "\n".join(text)

    def _get_row_format(self, dialect_opts, connect_opts):
        if dialect_opts["row_format"]:
            row_format = dialect_opts["row_format"]
        elif connect_opts:
            row_format = connect_opts.get("row_format")
        else:
            row_format = None
        return row_format

    def _get_row_format_specification(self, dialect_opts, connect_opts):
        row_format = self._get_row_format(dialect_opts, connect_opts)
        text = []
        if row_format:
            text.append(f"ROW FORMAT {row_format}")
        return "\n".join(text)

    def _get_serde_properties(self, dialect_opts, connect_opts):
        if dialect_opts["serdeproperties"]:
            serde_properties = dialect_opts["serdeproperties"]
        elif connect_opts:
            serde_properties = connect_opts.get("serdeproperties")
        else:
            serde_properties = None
        return serde_properties

    def _get_serde_properties_specification(self, dialect_opts, connect_opts):
        serde_properties = self._get_serde_properties(dialect_opts, connect_opts)
        text = []
        if serde_properties:
            text.append("WITH SERDEPROPERTIES (")
            if isinstance(serde_properties, dict):
                text.append(",\n".join([f"\t'{k}' = '{v}'" for k, v in serde_properties.items()]))
            else:
                text.append(serde_properties)
            text.append(")")
        return "\n".join(text)

    def _get_table_location(self, table, dialect_opts, connect_opts):
        if dialect_opts["location"]:
            location = dialect_opts["location"]
            location += "/" if not location.endswith("/") else ""
        elif connect_opts:
            base_location = (
                connect_opts["location"]
                if "location" in connect_opts
                else connect_opts.get("s3_staging_dir")
            )
            schema = table.schema if table.schema else connect_opts["schema_name"]
            location = f"{base_location}{schema}/{table.name}/"
        else:
            location = None
        return location

    def _get_table_location_specification(self, table, dialect_opts, connect_opts):
        location = self._get_table_location(table, dialect_opts, connect_opts)
        text = []
        if location:
            text.append(f"LOCATION '{location}'")
        else:
            if connect_opts:
                raise exc.CompileError(
                    "`location` or `s3_staging_dir` parameter is required "
                    "in the connection string"
                )
            else:
                raise exc.CompileError(
                    "The location of the table should be specified "
                    "by the dialect keyword argument `awsathena_location`"
                )
        return "\n".join(text)

    def _get_table_properties(self, dialect_opts, connect_opts):
        if dialect_opts["tblproperties"]:
            table_properties = dialect_opts["tblproperties"]
        elif connect_opts:
            table_properties = connect_opts.get("tblproperties")
        else:
            table_properties = None
        return table_properties

    def _get_compression(self, dialect_opts, connect_opts):
        if dialect_opts["compression"]:
            compression = dialect_opts["compression"]
        elif connect_opts:
            compression = connect_opts.get("compression")
        else:
            compression = None
        return compression

    def _get_table_properties_specification(self, dialect_opts, connect_opts):
        table_properties = self._get_table_properties(dialect_opts, connect_opts)
        if table_properties:
            if isinstance(table_properties, dict):
                table_properties = [
                    ",\n".join([f"\t'{k}' = '{v}'" for k, v in table_properties.items()])
                ]
            else:
                table_properties = [table_properties]
        else:
            table_properties = []

        compression = self._get_compression(dialect_opts, connect_opts)
        if compression:
            file_format = self._get_file_format(dialect_opts, connect_opts)
            row_format = self._get_row_format(dialect_opts, connect_opts)
            if file_format:
                if file_format == AthenaFileFormat.FILE_FORMAT_PARQUET:
                    table_properties.append(f"\t'parquet.compress' = '{compression}'")
                elif file_format == AthenaFileFormat.FILE_FORMAT_ORC:
                    table_properties.append(f"\t'orc.compress' = '{compression}'")
                else:
                    table_properties.append(f"\t'write.compress' = '{compression}'")
            elif row_format:
                if AthenaRowFormatSerde.is_parquet(row_format):
                    table_properties.append(f"\t'parquet.compress' = '{compression}'")
                elif AthenaRowFormatSerde.is_orc(row_format):
                    table_properties.append(f"\t'orc.compress' = '{compression}'")
                else:
                    table_properties.append(f"\t'write.compress' = '{compression}'")

        table_properties = ",\n".join(table_properties)
        text = []
        if table_properties:
            text.append("TBLPROPERTIES (")
            text.append(table_properties)
            text.append(")")
        return "\n".join(text)

    def get_column_specification(self, column, **kwargs):
        if isinstance(column.type, (types.Integer, types.INTEGER, types.INT)):
            # https://docs.aws.amazon.com/athena/latest/ug/create-table.html
            # In Data Definition Language (DDL) queries like CREATE TABLE,
            # use the int keyword to represent an integer
            type_ = "INT"
        else:
            type_ = self.dialect.type_compiler.process(column.type, type_expression=column)
        text = [f"{self.preparer.format_column(column)} {type_}"]
        if column.comment:
            text.append(f"{self._get_comment_specification(column.comment)}")
        return " ".join(text)

    def visit_check_constraint(self, constraint, **kw):
        return None

    def visit_column_check_constraint(self, constraint, **kw):
        return None

    def visit_foreign_key_constraint(self, constraint, **kw):
        return None

    def visit_primary_key_constraint(self, constraint, **kw):
        return None

    def visit_unique_constraint(self, constraint, **kw):
        return None

    def _get_connect_option_partitions(self, connect_opts):
        if connect_opts:
            partition = connect_opts.get("partition")
            partitions = partition.split(",") if partition else []
        else:
            partitions = []
        return partitions

    def _get_connect_option_buckets(self, connect_opts):
        if connect_opts:
            bucket = connect_opts.get("cluster")
            buckets = bucket.split(",") if bucket else []
        else:
            buckets = []
        return buckets

    def _prepared_columns(self, table, create_columns, connect_opts):
        columns, partitions, buckets = [], [], []
        conn_partitions = self._get_connect_option_partitions(connect_opts)
        conn_buckets = self._get_connect_option_buckets(connect_opts)
        for create_column in create_columns:
            column = create_column.element
            column_dialect_opts = column.dialect_options["awsathena"]
            try:
                processed = self.process(create_column)
                if processed is not None:
                    if (
                        column_dialect_opts["partition"]
                        or column.name in conn_partitions
                        or f"{table.name}.{column.name}" in conn_partitions
                    ):
                        partitions.append(f"\t{processed}")
                    else:
                        columns.append(f"\t{processed}")
                    if (
                        column_dialect_opts["cluster"]
                        or column.name in conn_buckets
                        or f"{table.name}.{column.name}" in conn_buckets
                    ):
                        buckets.append(f"\t{self.preparer.format_column(column)}")
            except exc.CompileError as ce:
                util.raise_(
                    exc.CompileError(
                        util.u(
                            f"(in table '{table.description}', column '{column.name}'): "
                            f"{ce.args[0]}"
                        )
                    ),
                    from_=ce,
                )
        return columns, partitions, buckets

    def visit_create_table(self, create, **kwargs):
        table = create.element
        table_dialect_opts = table.dialect_options["awsathena"]
        connect_opts = self.dialect._connect_options

        text = ["\nCREATE EXTERNAL TABLE"]
        if create.if_not_exists:
            text.append("IF NOT EXISTS")
        text.append(self.preparer.format_table(table))
        text.append("(")
        text = [" ".join(text)]

        columns, partitions, buckets = self._prepared_columns(table, create.columns, connect_opts)
        text.append(",\n".join(columns))
        text.append(")")

        if table.comment:
            text.append(self._get_comment_specification(table.comment))

        if partitions:
            text.append("PARTITIONED BY (")
            text.append(",\n".join(partitions))
            text.append(")")

        bucket_count = self._get_bucket_count(table_dialect_opts, connect_opts)
        if buckets and bucket_count:
            text.append("CLUSTERED BY (")
            text.append(",\n".join(buckets))
            text.append(f") INTO {bucket_count} BUCKETS")

        text.append(f"{self.post_create_table(table)}\n")
        return "\n".join(text)

    def post_create_table(self, table):
        dialect_opts = table.dialect_options["awsathena"]
        connect_opts = self.dialect._connect_options
        text = [
            self._get_row_format_specification(dialect_opts, connect_opts),
            self._get_serde_properties_specification(dialect_opts, connect_opts),
            self._get_file_format_specification(dialect_opts, connect_opts),
            self._get_table_location_specification(table, dialect_opts, connect_opts),
            self._get_table_properties_specification(dialect_opts, connect_opts),
        ]
        return "\n".join([t for t in text if t])


class AthenaDialect(DefaultDialect):
    name = "awsathena"
    preparer = AthenaDMLIdentifierPreparer
    statement_compiler = AthenaStatementCompiler
    ddl_compiler = AthenaDDLCompiler
    type_compiler = AthenaTypeCompiler
    default_paramstyle = pyathena.paramstyle
    cte_follows_insert = True
    supports_alter = False
    supports_pk_autoincrement = False
    supports_default_values = False
    supports_empty_insert = False
    supports_multivalues_insert = True
    supports_native_decimal = True
    supports_native_boolean = True
    supports_unicode_statements = True
    supports_unicode_binds = True
    supports_statement_cache = True
    returns_unicode_strings = True
    description_encoding = None
    postfetch_lastrowid = False
    construct_arguments = [
        (
            schema.Table,
            {
                "location": None,
                "compression": None,
                "row_format": None,
                "file_format": None,
                "serdeproperties": None,
                "tblproperties": None,
                "bucket_count": None,
            },
        ),
        (
            schema.Column,
            {
                "partition": False,
                "cluster": False,
            },
        ),
    ]

    _connect_options = dict()  # type: ignore
    _pattern_column_type = re.compile(r"^([a-zA-Z]+)(?:$|[\(|<](.+)[\)|>]$)")

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
        self._connect_options = self._create_connect_args(url)
        return [[], self._connect_options]

    def _create_connect_args(self, url):
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
            opts.update({"kill_on_interrupt": bool(strtobool(url.query["kill_on_interrupt"]))})
        return opts

    @reflection.cache
    def _get_schemas(self, connection, **kw):
        raw_connection = self._raw_connection(connection)
        catalog = raw_connection.catalog_name
        with raw_connection.connection.cursor() as cursor:
            try:
                return cursor.list_databases(catalog)
            except pyathena.error.OperationalError as exc:
                cause = exc.__cause__
                if (
                    isinstance(cause, botocore.exceptions.ClientError)
                    and cause.response["Error"]["Code"] == "InvalidRequestException"
                ):
                    return []
                raise

    @reflection.cache
    def _get_table(self, connection, table_name, schema=None, **kw):
        raw_connection = self._raw_connection(connection)
        schema = schema if schema else raw_connection.schema_name
        with raw_connection.connection.cursor() as cursor:
            try:
                return cursor.get_table_metadata(table_name, schema_name=schema, logging_=False)
            except pyathena.error.OperationalError as exc:
                cause = exc.__cause__
                if (
                    isinstance(cause, botocore.exceptions.ClientError)
                    and cause.response["Error"]["Code"] == "MetadataException"
                ):
                    raise NoSuchTableError(table_name) from exc
                raise

    @reflection.cache
    def _get_tables(self, connection, schema=None, **kw):
        raw_connection = self._raw_connection(connection)
        schema = schema if schema else raw_connection.schema_name
        with raw_connection.connection.cursor() as cursor:
            return cursor.list_table_metadata(schema_name=schema)

    def get_schema_names(self, connection, **kw):
        schemas = self._get_schemas(connection, **kw)
        return [s.name for s in schemas]

    def get_table_names(self, connection, schema=None, **kw):
        # Tables created by Athena are always classified as `EXTERNAL_TABLE`,
        # but Athena can also query tables classified as `MANAGED_TABLE`.
        # Managed Tables are created by default when creating tables via Spark when
        # Glue has been enabled as the Hive Metastore for Elastic Map Reduce (EMR) clusters.
        # With Athena Federation, tables in the database that are connected to Athena via lambda
        # function, is classified as `EXTERNAL` and fully queryable
        tables = self._get_tables(connection, schema, **kw)
        return [
            t.name
            for t in tables
            if t.table_type in ["EXTERNAL_TABLE", "MANAGED_TABLE", "EXTERNAL"]
        ]

    def get_view_names(self, connection, schema=None, **kw):
        tables = self._get_tables(connection, schema, **kw)
        return [t.name for t in tables if t.table_type == "VIRTUAL_VIEW"]

    def get_table_comment(self, connection, table_name, schema=None, **kw):
        metadata = self._get_table(connection, table_name, schema=schema, **kw)
        return {"text": metadata.comment}

    def get_table_options(self, connection, table_name, schema=None, **kw):
        metadata = self._get_table(connection, table_name, schema=schema, **kw)
        # TODO The metadata retrieved from the API does not seem to include bucketing information.
        return {
            "awsathena_location": metadata.location,
            "awsathena_compression": metadata.compression,
            "awsathena_row_format": metadata.row_format,
            "awsathena_file_format": metadata.file_format,
            "awsathena_serdeproperties": HashableDict(metadata.serde_properties),
            "awsathena_tblproperties": HashableDict(metadata.table_properties),
        }

    def has_table(self, connection, table_name, schema=None, **kw):
        try:
            columns = self.get_columns(connection, table_name, schema)
            return True if columns else False
        except NoSuchTableError:
            return False

    @reflection.cache
    def get_columns(self, connection, table_name, schema=None, **kw):
        metadata = self._get_table(connection, table_name, schema=schema, **kw)
        columns = [
            {
                "name": c.name,
                "type": self._get_column_type(c.type),
                "nullable": True,
                "default": None,
                "autoincrement": False,
                "comment": c.comment,
                "dialect_options": {"awsathena_partition": None},
            }
            for c in metadata.columns
        ]
        columns += [
            {
                "name": c.name,
                "type": self._get_column_type(c.type),
                "nullable": True,
                "default": None,
                "autoincrement": False,
                "comment": c.comment,
                "dialect_options": {"awsathena_partition": True},
            }
            for c in metadata.partition_keys
        ]
        return columns

    def _get_column_type(self, type_):
        match = self._pattern_column_type.match(type_)
        if match:
            name = match.group(1).lower()
            length = match.group(2)
        else:
            name = type_.lower()
            length = None

        args = []
        if name in ["boolean"]:
            col_type = types.BOOLEAN
        elif name in ["float", "double", "real"]:
            col_type = types.FLOAT
        elif name in ["tinyint", "smallint", "integer", "int"]:
            col_type = types.INTEGER
        elif name in ["bigint"]:
            col_type = types.BIGINT
        elif name in ["decimal"]:
            col_type = types.DECIMAL
            if length:
                precision, scale = length.split(",")
                args = [int(precision), int(scale)]
        elif name in ["char"]:
            col_type = types.CHAR
            if length:
                args = [int(length)]
        elif name in ["varchar"]:
            col_type = types.VARCHAR
            if length:
                args = [int(length)]
        elif name in ["string"]:
            col_type = types.String
        elif name in ["date"]:
            col_type = types.DATE
        elif name in ["timestamp"]:
            col_type = types.TIMESTAMP
        elif name in ["binary", "varbinary"]:
            col_type = types.BINARY
        elif name in ["array", "map", "struct", "row", "json"]:
            col_type = types.String
        else:
            util.warn(f"Did not recognize type '{type_}'")
            col_type = types.NullType
        return col_type(*args)

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


class AthenaRestDialect(AthenaDialect):
    driver = "rest"
    supports_statement_cache = True


class AthenaPandasDialect(AthenaDialect):
    driver = "pandas"
    supports_statement_cache = True

    def create_connect_args(self, url):
        from pyathena.pandas.cursor import PandasCursor

        opts = super()._create_connect_args(url)
        opts.update({"cursor_class": PandasCursor})
        cursor_kwargs = dict()
        if "unload" in opts:
            cursor_kwargs.update({"unload": bool(strtobool(opts.pop("unload")))})
        if "engine" in opts:
            cursor_kwargs.update({"engine": opts.pop("engine")})
        if "chunksize" in opts:
            cursor_kwargs.update({"chunksize": int(opts.pop("chunksize"))})  # type: ignore
        if cursor_kwargs:
            opts.update({"cursor_kwargs": cursor_kwargs})
        return [[], opts]


class AthenaArrowDialect(AthenaDialect):
    driver = "arrow"
    supports_statement_cache = True

    def create_connect_args(self, url):
        from pyathena.arrow.cursor import ArrowCursor

        opts = super()._create_connect_args(url)
        opts.update({"cursor_class": ArrowCursor})
        cursor_kwargs = dict()
        if "unload" in opts:
            cursor_kwargs.update({"unload": bool(strtobool(opts.pop("unload")))})
        if cursor_kwargs:
            opts.update({"cursor_kwargs": cursor_kwargs})
        return [[], opts]
