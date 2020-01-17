# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import math
import numbers
import re
from collections import namedtuple

import tenacity
from future.utils import raise_from
from sqlalchemy import exc, util, Column
from sqlalchemy.engine import reflection, Engine
from sqlalchemy.engine.default import DefaultDialect
from sqlalchemy.exc import NoSuchTableError, OperationalError
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.expression import FunctionElement
from sqlalchemy.types import (SchemaType, to_instance, TypeDecorator, TypeEngine, UserDefinedType)
from sqlalchemy.sql.compiler import (BIND_PARAMS, BIND_PARAMS_ESC,
                                     IdentifierPreparer, SQLCompiler, DDLCompiler)
from sqlalchemy.sql.sqltypes import (BIGINT, BINARY, BOOLEAN, DATE, DECIMAL, FLOAT,
                                     INTEGER, NULLTYPE, STRINGTYPE, TIMESTAMP)
from tenacity import retry_if_exception, stop_after_attempt, wait_exponential

import pyathena

iteritems = getattr(dict, 'iteritems', dict.items)  # py2-3 compatibility


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
            initial_quote='`',
            final_quote=None,
            escape_quote='`',
            quote_case_sensitive_collations=True,
            omit_schema=False
    ):
        super(AthenaDDLIdentifierPreparer, self).__init__(
            dialect=dialect,
            initial_quote=initial_quote,
            final_quote=final_quote,
            escape_quote=escape_quote,
            quote_case_sensitive_collations=quote_case_sensitive_collations,
            omit_schema=omit_schema
        )


class AthenaStatementCompiler(SQLCompiler):
    """PrestoCompiler

    https://github.com/dropbox/PyHive/blob/master/pyhive/sqlalchemy_presto.py"""

    def visit_char_length_func(self, fn, **kw):
        return 'length{0}'.format(self.function_argspec(fn, **kw))

    def visit_textclause(self, textclause, **kw):
        def do_bindparam(m):
            name = m.group(1)
            if name in textclause._bindparams:
                return self.process(textclause._bindparams[name], **kw)
            else:
                return self.bindparam_string(name, **kw)

        if not self.stack:
            self.isplaintext = True

        if len(textclause._bindparams) == 0:
            # Prevents double escaping of percent character
            return textclause.text
        else:
            # un-escape any \:params
            return BIND_PARAMS_ESC.sub(
                lambda m: m.group(1),
                BIND_PARAMS.sub(
                    do_bindparam,
                    self.post_process_text(textclause.text))
            )


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
            bind=None,
            schema_translate_map=None,
            compile_kwargs=util.immutabledict()):
        self._preparer = AthenaDDLIdentifierPreparer(dialect)
        super(AthenaDDLCompiler, self).__init__(
            dialect=dialect,
            statement=statement,
            bind=bind,
            schema_translate_map=schema_translate_map,
            compile_kwargs=compile_kwargs)

    def visit_create_table(self, create):
        table = create.element
        preparer = self.preparer

        text = '\nCREATE EXTERNAL '
        text += 'TABLE ' + preparer.format_table(table) + ' '
        text += '('

        separator = '\n'
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
                            table.description, column.name, ce.args[0])
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
        raw_connection = table.bind.raw_connection()
        # TODO Supports orc, avro, json, csv or tsv format
        text = 'STORED AS PARQUET\n'

        location = raw_connection._kwargs['s3_dir'] if 's3_dir' in raw_connection._kwargs \
            else raw_connection.s3_staging_dir
        if not location:
            raise exc.CompileError('`s3_dir` or `s3_staging_dir` parameter is required'
                                   ' in the connection string.')
        text += "LOCATION '{0}{1}/{2}/'\n".format(location, table.schema, table.name)

        compression = raw_connection._kwargs.get('compression')
        if compression:
            text += "TBLPROPERTIES ('parquet.compress'='{0}')\n".format(compression.upper())

        return text


class StructElement(FunctionElement):
    """
    Instances of this class wrap a struct type.
    """
    def __init__(self, base, field, type_):
        self.name = field
        self.type = to_instance(type_)

        super(StructElement, self).__init__(base)


class StructValue(dict):
    """
    Instances of this class wrap a struct children's values.
    """
    def __getattr__(self, item):
        if item in self:
            return self[item]
        return super(dict).__getattribute__(item)

    def __hash__(self):
        h = 0
        for key, value in iteritems(self):
            h ^= hash((key, value))
        return h


def _parse_struct(value, open_delimiter='{', close_delimiter='}'):
    parsed = {}
    value = value.strip()[1:-1]

    t = 'key'
    k = None
    v = ''
    prev_c = None
    sub_struct = False
    for c in value:
        if c == '=' and t == 'key':
            k = v.strip()
            t = 'value'
            v = ''
        elif not sub_struct and c == ',' and t == 'value':
            parsed[k] = v
            k = None
            t = 'key'
            v = ''
        elif sub_struct and c == close_delimiter:
            sub_struct = False
            parsed[k] = v + c
            t = 'key'
            v = ''
        elif prev_c == '=' and c == open_delimiter:
            sub_struct = True
            v += c
        else:
            v += c

        prev_c = c
    if k:
        parsed[k] = v

    return {
        k: v if v != 'null' else None
        for k, v in iteritems(parsed)
    }


@compiles(StructElement)
def _compile_struct_elem(expr, compiler, **kwargs):
    return '(%s).%s' % (compiler.process(expr.clauses, **kwargs), expr.name)


class StructType(UserDefinedType, SchemaType):
    """
    Represents a struct type.

    :param name:
        Name of the struct type.
    :param columns:
        List of columns that this struct consists of
    """
    python_type = tuple

    class comparator_factory(UserDefinedType.Comparator):
        def __getattr__(self, key):
            try:
                type_ = self.type.colmap[key].type
            except KeyError:
                raise KeyError(
                    "struct '%s' doesn't have an attribute: '%s'" % (
                        super(StructType, self.type).name, key
                    )
                )

            return StructElement(self.expr, key, type_)

    def __init__(self, name, columns):
        SchemaType.__init__(self)
        self.name = name
        self.columns = columns
        self.colmap = {}
        for column in columns:
            self.colmap[column.name] = column

        self.type_cls = namedtuple(
            self.name, [c.name for c in columns]
        )

    def get_col_spec(self):
        return self.name

    def bind_processor(self, dialect):
        raise NotImplemented()

    def result_processor(self, dialect, coltype):
        def process(value):
            if value is None:
                return None
            parsed_value = _parse_struct(value)
            data_dict = {}

            for column in self.columns:
                column_value = parsed_value.get(column.name)
                if isinstance(column.type, StructType):
                    if column_value:
                        column_value = process(column_value)
                    data_dict[column.name] = column_value
                elif isinstance(column.type, TypeDecorator):
                    data_dict[column.name] = column.type.process_result_value(column_value, dialect)
                elif isinstance(column.type, TypeEngine):
                    if column_value:
                        column_value = column.type.python_type(column_value)
                    data_dict[column.name] = column_value
                else:
                    data_dict[column.name] = column_value
            return StructValue(data_dict)
        return process

    def create(self, bind=None, checkfirst=None):
        raise NotImplemented()

    def drop(self, bind=None, checkfirst=True):
        raise NotImplemented()


_TYPE_MAPPINGS = {
    'boolean': BOOLEAN,
    'real': FLOAT,
    'float': FLOAT,
    'double': FLOAT,
    'tinyint': INTEGER,
    'smallint': INTEGER,
    'integer': INTEGER,
    'bigint': BIGINT,
    'decimal': DECIMAL,
    'char': STRINGTYPE,
    'varchar': STRINGTYPE,
    'array': STRINGTYPE,
    'row': StructType,
    'varbinary': BINARY,
    'map': STRINGTYPE,
    'date': DATE,
    'timestamp': TIMESTAMP,
}


class AthenaDialect(DefaultDialect):

    name = 'awsathena'
    driver = 'rest'
    preparer = AthenaDMLIdentifierPreparer
    statement_compiler = AthenaStatementCompiler
    ddl_compiler = AthenaDDLCompiler
    default_paramstyle = pyathena.paramstyle
    supports_alter = False
    supports_pk_autoincrement = False
    supports_default_values = False
    supports_empty_insert = False
    supports_multivalues_insert = True
    supports_unicode_statements = True
    supports_unicode_binds = True
    returns_unicode_strings = True
    description_encoding = None
    supports_native_boolean = True
    postfetch_lastrowid = False

    _pattern_data_catlog_exception = re.compile(
        r'(((Database|Namespace)\ (?P<schema>.+))|(Table\ (?P<table>.+)))\ not\ found\.')
    _pattern_column_type = re.compile(r'^([a-zA-Z]+)($|\(.+\)$)')

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
            'aws_access_key_id': url.username if url.username else None,
            'aws_secret_access_key': url.password if url.password else None,
            'region_name': re.sub(r'^athena\.([a-z0-9-]+)\.amazonaws\.com$', r'\1', url.host),
            'schema_name': url.database if url.database else 'default'
        }
        opts.update(url.query)
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
    def get_table_names(self, connection, schema=None, **kw):
        raw_connection = self._raw_connection(connection)
        schema = schema if schema else raw_connection.schema_name
        query = """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = '{schema}'
                """.format(schema=schema)
        return [row.table_name for row in connection.execute(query).fetchall()]

    def has_table(self, connection, table_name, schema=None):
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
                """.format(schema=schema, table=table_name)
        retry_config = raw_connection.retry_config
        retry = tenacity.Retrying(
            retry=retry_if_exception(
                lambda exc: self._retry_if_data_catalog_exception(exc, schema, table_name)),
            stop=stop_after_attempt(retry_config.attempt),
            wait=wait_exponential(multiplier=retry_config.multiplier,
                                  max=retry_config.max_delay,
                                  exp_base=retry_config.exponential_base),
            reraise=True)
        try:
            return [
                {
                    'name': row.column_name,
                    'type': self._get_column_type(row.column_name, row.data_type),
                    'nullable': True if row.is_nullable == 'YES' else False,
                    'default': row.column_default if not self._is_nan(row.column_default) else None,
                    'ordinal_position': row.ordinal_position,
                    'comment': row.comment,
                } for row in retry(connection.execute, query).fetchall()
            ]
        except OperationalError as e:
            if not self._retry_if_data_catalog_exception(e, schema, table_name):
                raise_from(NoSuchTableError(table_name), e)
            else:
                raise e

    def _retry_if_data_catalog_exception(self, exc, schema, table_name):
        if not isinstance(exc, OperationalError):
            return False

        match = self._pattern_data_catlog_exception.search(str(exc))
        if match and (match.group('schema') == schema or
                      match.group('table') == table_name):
            return False
        return True

    @staticmethod
    def _parse_struct_schema(schema):
        parsed = {}
        schema = schema.strip()[1:-1]

        t = 'name'
        k = None
        v = ''
        prev_c = None
        for c in schema:
            if c == ' ' and t == 'name':
                if prev_c == ',':
                    continue
                t = 'type'
                k = v
                v = ''
            elif c == ',' and t == 'type':
                t = 'name'
                parsed[k] = v
                k = None
                v = ''
            else:
                v += c

            prev_c = c
        if k:
            parsed[k] = v

        return parsed

    def _get_column_type(self, column_name, column_type):
        type_name = self._pattern_column_type.sub(r'\1', column_type)
        type_ = _TYPE_MAPPINGS.get(type_name, NULLTYPE)

        if type_name in ('row',):
            def struct_wrapper():
                return StructType(
                    column_name,
                    [
                        Column(name, self._get_column_type(name, type_))
                        for name, type_ in iteritems(self._parse_struct_schema(column_type[len(type_name):]))
                    ]
                )
            return struct_wrapper
        else:
            return type_

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
        return isinstance(column_default, numbers.Number) and math.isnan(column_default)
