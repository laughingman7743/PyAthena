# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import re

import tenacity
from future.utils import raise_from
from sqlalchemy.engine import reflection
from sqlalchemy.engine.default import DefaultDialect
from sqlalchemy.exc import NoSuchTableError, OperationalError
from sqlalchemy.sql.compiler import IdentifierPreparer, SQLCompiler
from sqlalchemy.sql.sqltypes import (BIGINT, BINARY, BOOLEAN, DATE, DECIMAL, FLOAT,
                                     INTEGER, NULLTYPE, STRINGTYPE, TIMESTAMP)
from tenacity import retry_if_exception, stop_after_attempt, wait_exponential

import pyathena


class UniversalSet(object):
    """UniversalSet

    https://github.com/dropbox/PyHive/blob/master/pyhive/common.py"""
    def __contains__(self, item):
        return True


class AthenaIdentifierPreparer(IdentifierPreparer):
    """PrestoIdentifierPreparer

    https://github.com/dropbox/PyHive/blob/master/pyhive/sqlalchemy_presto.py"""
    reserved_words = UniversalSet()


class AthenaCompiler(SQLCompiler):
    """PrestoCompiler

    https://github.com/dropbox/PyHive/blob/master/pyhive/sqlalchemy_presto.py"""
    def visit_char_length_func(self, fn, **kw):
        return 'length{0}'.format(self.function_argspec(fn, **kw))


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
    'row': STRINGTYPE,  # StructType
    'varbinary': BINARY,
    'map': STRINGTYPE,
    'date': DATE,
    'timestamp': TIMESTAMP,
}


class AthenaDialect(DefaultDialect):

    name = 'awsathena'
    driver = 'rest'
    preparer = AthenaIdentifierPreparer
    statement_compiler = AthenaCompiler
    default_paramstyle = pyathena.paramstyle
    supports_alter = False
    supports_pk_autoincrement = False
    supports_default_values = False
    supports_empty_insert = False
    supports_unicode_statements = True
    supports_unicode_binds = True
    returns_unicode_strings = True
    description_encoding = None
    supports_native_boolean = True

    _pattern_data_catlog_exception = re.compile(
        r'(((Database|Namespace)\ (?P<schema>.+))|(Table\ (?P<table>.+)))\ not\ found\.')
    _pattern_column_type = re.compile(r'^([a-zA-Z]+)($|\(.+\)$)')

    @classmethod
    def dbapi(cls):
        return pyathena

    def _get_default_schema_name(self, connection):
        return connection.connection.schema_name

    def create_connect_args(self, url):
        # Connection string format:
        #   awsathena+rest://
        #   {aws_access_key_id}:{aws_secret_access_key}@athena.{region_name}.amazonaws.com:443/
        #   {schema_name}?s3_staging_dir={s3_staging_dir}&...
        opts = {
            'aws_access_key_id': url.username,
            'aws_secret_access_key': url.password,
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
        schema = schema if schema else connection.connection.schema_name
        query = """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = '{schema}'
                """.format(schema=schema)
        return [row.table_name for row in connection.execute(query).fetchall()]

    def has_table(self, connection, table_name, schema=None):
        try:
            self.get_columns(connection, table_name, schema)
            return True
        except NoSuchTableError:
            return False

    @reflection.cache
    def get_columns(self, connection, table_name, schema=None, **kw):
        schema = schema if schema else connection.connection.schema_name
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
        retry = tenacity.Retrying(
            retry=retry_if_exception(
                lambda exc: self._retry_if_data_catalog_exception(exc, schema, table_name)),
            stop=stop_after_attempt(connection.connection.retry_attempt),
            wait=wait_exponential(multiplier=connection.connection.retry_multiplier,
                                  max=connection.connection.retry_max_delay,
                                  exp_base=connection.connection.retry_exponential_base),
            reraise=True)
        try:
            return [
                {
                    'name': row.column_name,
                    'type': _TYPE_MAPPINGS.get(self._get_column_type(row.data_type), NULLTYPE),
                    'nullable': True if row.is_nullable == 'YES' else False,
                    'default': row.column_default,
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

    def _get_column_type(self, type_):
        return self._pattern_column_type.sub(r'\1', type_)

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
