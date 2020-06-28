# -*- coding: utf-8 -*-
from __future__ import absolute_import, unicode_literals

import concurrent
import functools
import logging
import re
import threading
import uuid
from collections import OrderedDict
from concurrent.futures.thread import ThreadPoolExecutor
from copy import deepcopy

import tenacity
from boto3 import Session
from future.utils import iteritems
from past.builtins import xrange
from tenacity import after_log, retry_if_exception, stop_after_attempt, wait_exponential

from pyathena import DataError, OperationalError, cpu_count
from pyathena.model import AthenaCompression

_logger = logging.getLogger(__name__)

PATTERN_OUTPUT_LOCATION = re.compile(r"^s3://(?P<bucket>[a-zA-Z0-9.\-_]+)/(?P<key>.+)$")


def parse_output_location(output_location):
    match = PATTERN_OUTPUT_LOCATION.search(output_location)
    if match:
        return match.group("bucket"), match.group("key")
    else:
        raise DataError("Unknown `output_location` format.")


def get_chunks(df, chunksize=None):
    rows = len(df)
    if rows == 0:
        return
    if chunksize is None:
        chunksize = rows
    elif chunksize <= 0:
        raise ValueError("Chunk size argument must be greater than zero")

    chunks = int(rows / chunksize) + 1
    for i in xrange(chunks):
        start_i = i * chunksize
        end_i = min((i + 1) * chunksize, rows)
        if start_i >= end_i:
            break
        yield df[start_i:end_i]


def reset_index(df, index_label=None):
    df.index.name = index_label if index_label else "index"
    try:
        df.reset_index(inplace=True)
    except ValueError as e:
        raise ValueError("Duplicate name in index/columns: {0}".format(e))


def as_pandas(cursor, coerce_float=False):
    from pandas import DataFrame

    names = [metadata[0] for metadata in cursor.description]
    return DataFrame.from_records(
        cursor.fetchall(), columns=names, coerce_float=coerce_float
    )


def to_sql_type_mappings(col):
    import pandas as pd

    col_type = pd._lib.infer_dtype(col, skipna=True)
    if col_type == "datetime64" or col_type == "datetime":
        return "TIMESTAMP"
    elif col_type == "timedelta":
        return "INT"
    elif col_type == "timedelta64":
        return "BIGINT"
    elif col_type == "floating":
        if col.dtype == "float32":
            return "FLOAT"
        else:
            return "DOUBLE"
    elif col_type == "integer":
        if col.dtype == "int32":
            return "INT"
        else:
            return "BIGINT"
    elif col_type == "boolean":
        return "BOOLEAN"
    elif col_type == "date":
        return "DATE"
    elif col_type == "bytes":
        return "BINARY"
    elif col_type in ["complex", "time"]:
        raise ValueError("Data type `{0}` is not supported".format(col_type))
    return "STRING"


def to_parquet(
    df,
    bucket_name,
    prefix,
    retry_config,
    session_kwargs,
    client_kwargs,
    compression=None,
    flavor="spark",
):
    import pyarrow as pa
    import pyarrow.parquet as pq

    session = Session(**session_kwargs)
    client = session.resource("s3", **client_kwargs)
    bucket = client.Bucket(bucket_name)
    table = pa.Table.from_pandas(df)
    buf = pa.BufferOutputStream()
    pq.write_table(table, buf, compression=compression, flavor=flavor)
    response = retry_api_call(
        bucket.put_object,
        config=retry_config,
        Body=buf.getvalue().to_pybytes(),
        Key=prefix + str(uuid.uuid4()),
    )
    return "s3://{0}/{1}".format(response.bucket_name, response.key)


def to_sql(
    df,
    name,
    conn,
    location,
    schema="default",
    index=False,
    index_label=None,
    partitions=None,
    chunksize=None,
    if_exists="fail",
    compression=None,
    flavor="spark",
    type_mappings=to_sql_type_mappings,
    executor_class=ThreadPoolExecutor,
    max_workers=(cpu_count() or 1) * 5,
):
    # TODO Supports orc, avro, json, csv or tsv format
    if if_exists not in ("fail", "replace", "append"):
        raise ValueError("`{0}` is not valid for if_exists".format(if_exists))
    if compression is not None and not AthenaCompression.is_valid(compression):
        raise ValueError("`{0}` is not valid for compression".format(compression))
    if partitions is None:
        partitions = []

    bucket_name, key_prefix = parse_output_location(location)
    bucket = conn.session.resource(
        "s3", region_name=conn.region_name, **conn._client_kwargs
    ).Bucket(bucket_name)
    cursor = conn.cursor()

    table = cursor.execute(
        """
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = '{schema}'
    AND table_name = '{table}'
    """.format(
            schema=schema, table=name
        )
    ).fetchall()
    if if_exists == "fail":
        if table:
            raise OperationalError(
                "Table `{0}.{1}` already exists.".format(schema, name)
            )
    elif if_exists == "replace":
        if table:
            cursor.execute(
                """
            DROP TABLE {schema}.{table}
            """.format(
                    schema=schema, table=name
                )
            )
            objects = bucket.objects.filter(Prefix=key_prefix)
            if list(objects.limit(1)):
                objects.delete()

    if index:
        reset_index(df, index_label)
    with executor_class(max_workers=max_workers) as e:
        futures = []
        session_kwargs = deepcopy(conn._session_kwargs)
        session_kwargs.update({"profile_name": conn.profile_name})
        client_kwargs = deepcopy(conn._client_kwargs)
        client_kwargs.update({"region_name": conn.region_name})
        if partitions:
            for keys, group in df.groupby(by=partitions, observed=True):
                keys = keys if isinstance(keys, tuple) else (keys,)
                group = group.drop(partitions, axis=1)
                partition_prefix = "/".join(
                    ["{0}={1}".format(key, val) for key, val in zip(partitions, keys)]
                )
                for chunk in get_chunks(group, chunksize):
                    futures.append(
                        e.submit(
                            to_parquet,
                            chunk,
                            bucket_name,
                            "{0}{1}/".format(key_prefix, partition_prefix),
                            conn._retry_config,
                            session_kwargs,
                            client_kwargs,
                            compression,
                            flavor,
                        )
                    )
        else:
            for chunk in get_chunks(df, chunksize):
                futures.append(
                    e.submit(
                        to_parquet,
                        chunk,
                        bucket_name,
                        key_prefix,
                        conn._retry_config,
                        session_kwargs,
                        client_kwargs,
                        compression,
                        flavor,
                    )
                )
        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            _logger.info("to_parquet: {0}".format(result))

    ddl = generate_ddl(
        df=df,
        name=name,
        location=location,
        schema=schema,
        partitions=partitions,
        compression=compression,
        type_mappings=type_mappings,
    )
    _logger.info(ddl)
    cursor.execute(ddl)
    if partitions:
        repair = "MSCK REPAIR TABLE {0}.{1}".format(schema, name)
        _logger.info(repair)
        cursor.execute(repair)


def get_column_names_and_types(df, type_mappings):
    return OrderedDict(
        (
            (str(df.columns[i]), type_mappings(df.iloc[:, i]))
            for i in xrange(len(df.columns))
        )
    )


def generate_ddl(
    df,
    name,
    location,
    schema="default",
    partitions=None,
    compression=None,
    type_mappings=to_sql_type_mappings,
):
    if partitions is None:
        partitions = []
    column_names_and_types = get_column_names_and_types(df, type_mappings)
    ddl = "CREATE EXTERNAL TABLE IF NOT EXISTS `{0}`.`{1}` (\n".format(schema, name)
    ddl += ",\n".join(
        [
            "`{0}` {1}".format(col, type_)
            for col, type_ in iteritems(column_names_and_types)
            if col not in partitions
        ]
    )
    ddl += "\n)\n"
    if partitions:
        ddl += "PARTITIONED BY (\n"
        ddl += ",\n".join(
            ["`{0}` {1}".format(p, column_names_and_types[p]) for p in partitions]
        )
        ddl += "\n)\n"
    ddl += "STORED AS PARQUET\n"
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
    def __init__(
        self,
        exceptions=("ThrottlingException", "TooManyRequestsException"),
        attempt=5,
        multiplier=1,
        max_delay=100,
        exponential_base=2,
    ):
        self.exceptions = exceptions
        self.attempt = attempt
        self.multiplier = multiplier
        self.max_delay = max_delay
        self.exponential_base = exponential_base


def retry_api_call(func, config, logger=None, *args, **kwargs):
    retry = tenacity.Retrying(
        retry=retry_if_exception(
            lambda e: getattr(e, "response", {}).get("Error", {}).get("Code", None)
            in config.exceptions
            if e
            else False
        ),
        stop=stop_after_attempt(config.attempt),
        wait=wait_exponential(
            multiplier=config.multiplier,
            max=config.max_delay,
            exp_base=config.exponential_base,
        ),
        after=after_log(logger, logger.level) if logger else None,
        reraise=True,
    )
    return retry(func, *args, **kwargs)
