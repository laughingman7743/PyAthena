# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import functools
import logging
import threading
import re
import uuid

import tenacity
from past.builtins import xrange
from tenacity import (after_log, retry_if_exception,
                      stop_after_attempt, wait_exponential)

from pyathena import DataError, OperationalError
from pyathena.model import AthenaCompression

_logger = logging.getLogger(__name__)

PATTERN_OUTPUT_LOCATION = re.compile(r'^s3://(?P<bucket>[a-zA-Z0-9.\-_]+)/(?P<key>.+)$')


def parse_output_location(output_location):
    match = PATTERN_OUTPUT_LOCATION.search(output_location)
    if match:
        return match.group('bucket'), match.group('key')
    else:
        raise DataError('Unknown `output_location` format.')


def get_chunks(df, chunksize=None):
    rows = len(df)
    if rows == 0:
        return
    if chunksize is None:
        chunksize = rows
    elif chunksize <= 0:
        raise ValueError('Chunk size argument must be greater than zero')

    chunks = int(rows / chunksize) + 1
    for i in xrange(chunks):
        start_i = i * chunksize
        end_i = min((i + 1) * chunksize, rows)
        if start_i >= end_i:
            break
        yield df[start_i:end_i]


def reset_index(df, index_label=None):
    df.index.name = index_label if index_label else 'index'
    try:
        df.reset_index(inplace=True)
    except ValueError as e:
        raise ValueError('Duplicate name in index/columns: {0}'.format(e))


def as_pandas(cursor, coerce_float=False):
    from pandas import DataFrame
    names = [metadata[0] for metadata in cursor.description]
    return DataFrame.from_records(cursor.fetchall(), columns=names,
                                  coerce_float=coerce_float)


def to_sql_type_mappings(col):
    import pandas as pd
    col_type = pd._lib.infer_dtype(col, skipna=True)
    if col_type == 'datetime64' or col_type == 'datetime':
        return 'TIMESTAMP'
    elif col_type == 'timedelta':
        return 'INT'
    elif col_type == "timedelta64":
        return 'BIGINT'
    elif col_type == 'floating':
        if col.dtype == 'float32':
            return 'FLOAT'
        else:
            return 'DOUBLE'
    elif col_type == 'integer':
        if col.dtype == 'int32':
            return 'INT'
        else:
            return 'BIGINT'
    elif col_type == 'boolean':
        return 'BOOLEAN'
    elif col_type == "date":
        return 'DATE'
    elif col_type == 'bytes':
        return 'BINARY'
    elif col_type in ['complex', 'time']:
        raise ValueError('{0} datatype not supported'.format(col_type))
    return 'STRING'


def to_sql(df, name, conn, location, schema='default',
           index=False, index_label=None, chunksize=None,
           if_exists='fail', compression=None, flavor='spark',
           type_mappings=to_sql_type_mappings):
    # TODO Supports orc, avro, json, csv or tsv format
    # TODO Supports partitioning
    if if_exists not in ('fail', 'replace', 'append'):
        raise ValueError('`{0}` is not valid for if_exists'.format(if_exists))
    if compression is not None and not AthenaCompression.is_valid(compression):
        raise ValueError('`{0}` is not valid for compression'.format(compression))

    import pyarrow as pa
    import pyarrow.parquet as pq
    bucket_name, key_prefix = parse_output_location(location)
    bucket = conn.session.resource('s3', region_name=conn.region_name,
                                   **conn._client_kwargs).Bucket(bucket_name)
    cursor = conn.cursor()
    retry_config = conn.retry_config

    table = cursor.execute("""
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = '{schema}'
    AND table_name = '{table}'
    """.format(schema=schema, table=name)).fetchall()
    if if_exists == 'fail':
        if table:
            raise OperationalError('Table `{0}.{1}` already exists.'.format(schema, name))
    elif if_exists == 'replace':
        if table:
            cursor.execute("""
            DROP TABLE {schema}.{table}
            """.format(schema=schema, table=name))
            objects = bucket.objects.filter(Prefix=key_prefix)
            if list(objects.limit(1)):
                objects.delete()

    if index:
        reset_index(df, index_label)
    for chunk in get_chunks(df, chunksize):
        table = pa.Table.from_pandas(chunk)
        buf = pa.BufferOutputStream()
        pq.write_table(table, buf,
                       compression=compression,
                       flavor=flavor)
        retry_api_call(bucket.put_object,
                       config=retry_config,
                       Body=buf.getvalue().to_pybytes(),
                       Key=key_prefix + str(uuid.uuid4()))

    ddl = generate_ddl(df=df,
                       name=name,
                       location=location,
                       schema=schema,
                       compression=compression,
                       type_mappings=type_mappings)
    cursor.execute(ddl)


def get_column_names_and_types(df, type_mappings):
    return [
        (str(df.columns[i]), type_mappings(df.iloc[:, i]))
        for i in xrange(len(df.columns))
    ]


def generate_ddl(df, name, location, schema='default', compression=None,
                 type_mappings=to_sql_type_mappings):
    ddl = 'CREATE EXTERNAL TABLE IF NOT EXISTS `{0}`.`{1}` (\n'.format(schema, name)
    ddl += ',\n'.join([
        '`{0}` {1}'.format(c[0], c[1])
        for c in get_column_names_and_types(df, type_mappings)
    ])
    ddl += '\n)\n'
    ddl += 'STORED AS PARQUET\n'
    ddl += "LOCATION '{0}'\n".format(location)
    if compression:
        ddl += "TBLPROPERTIES ('parquet.compress'='{0}')\n".format(compression.upper())
    return ddl


def synchronized(wrapped):
    """The missing @synchronized decorator

    https://git.io/vydTA"""
    _lock = threading.RLock()

    @functools.wraps(wrapped)
    def _wrapper(*args, **kwargs):
        with _lock:
            return wrapped(*args, **kwargs)
    return _wrapper


class RetryConfig(object):

    def __init__(self, exceptions=('ThrottlingException', 'TooManyRequestsException'),
                 attempt=5, multiplier=1, max_delay=100, exponential_base=2):
        self.exceptions = exceptions
        self.attempt = attempt
        self.multiplier = multiplier
        self.max_delay = max_delay
        self.exponential_base = exponential_base


def retry_api_call(func, config, logger=None,
                   *args, **kwargs):
    retry = tenacity.Retrying(
        retry=retry_if_exception(
            lambda e: getattr(e, 'response', {}).get(
                'Error', {}).get('Code', None) in config.exceptions
            if e else False),
        stop=stop_after_attempt(config.attempt),
        wait=wait_exponential(multiplier=config.multiplier,
                              max=config.max_delay,
                              exp_base=config.exponential_base),
        after=after_log(logger, logger.level) if logger else None,
        reraise=True
    )
    return retry(func, *args, **kwargs)
