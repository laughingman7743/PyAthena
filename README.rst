.. image:: https://badge.fury.io/py/pyathena.svg
    :target: https://badge.fury.io/py/pyathena

.. image:: https://img.shields.io/pypi/pyversions/PyAthena.svg
    :target: https://pypi.org/project/PyAthena/

.. image:: https://github.com/laughingman7743/PyAthena/actions/workflows/test.yaml/badge.svg
    :target: https://github.com/laughingman7743/PyAthena/actions/workflows/test.yaml

.. image:: https://img.shields.io/pypi/l/PyAthena.svg
    :target: https://github.com/laughingman7743/PyAthena/blob/master/LICENSE

.. image:: https://pepy.tech/badge/pyathena/month
    :target: https://pepy.tech/project/pyathena

.. image:: https://img.shields.io/badge/code%20style-black-000000.svg
    :target: https://github.com/psf/black

.. image:: https://img.shields.io/badge/%20imports-isort-%231674b1?style=flat&labelColor=ef8336
    :target: https://pycqa.github.io/isort/

PyAthena
========

PyAthena is a Python `DB API 2.0 (PEP 249)`_ client for `Amazon Athena`_.

.. _`DB API 2.0 (PEP 249)`: https://www.python.org/dev/peps/pep-0249/
.. _`Amazon Athena`: https://docs.aws.amazon.com/athena/latest/APIReference/Welcome.html

.. contents:: Table of Contents:
   :local:
   :depth: 2

Requirements
------------

* Python

  - CPython 3.7 3.8 3.9 3.10, 3.11

Installation
------------

.. code:: bash

    $ pip install PyAthena

Extra packages:

+---------------+---------------------------------------+------------------+
| Package       | Install command                       | Version          |
+===============+=======================================+==================+
| SQLAlchemy    | ``pip install PyAthena[SQLAlchemy]``  | >=1.0.0, <2.0.0  |
+---------------+---------------------------------------+------------------+
| Pandas        | ``pip install PyAthena[Pandas]``      | >=1.3.0          |
+---------------+---------------------------------------+------------------+
| Arrow         | ``pip install PyAthena[Arrow]``       | >=7.0.0          |
+---------------+---------------------------------------+------------------+
| fastparquet   | ``pip install PyAthena[fastparquet]`` | >=0.4.0          |
+---------------+---------------------------------------+------------------+

Usage
-----

Basic usage
~~~~~~~~~~~

.. code:: python

    from pyathena import connect

    cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                     region_name="us-west-2").cursor()
    cursor.execute("SELECT * FROM one_row")
    print(cursor.description)
    print(cursor.fetchall())

Cursor iteration
~~~~~~~~~~~~~~~~

.. code:: python

    from pyathena import connect

    cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                     region_name="us-west-2").cursor()
    cursor.execute("SELECT * FROM many_rows LIMIT 10")
    for row in cursor:
        print(row)

Query with parameter
~~~~~~~~~~~~~~~~~~~~

Supported `DB API paramstyle`_ is only ``PyFormat``.
``PyFormat`` only supports `named placeholders`_ with old ``%`` operator style and parameters specify dictionary format.

.. code:: python

    from pyathena import connect

    cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                     region_name="us-west-2").cursor()
    cursor.execute("""
                   SELECT col_string FROM one_row_complex
                   WHERE col_string = %(param)s
                   """, {"param": "a string"})
    print(cursor.fetchall())

if ``%`` character is contained in your query, it must be escaped with ``%%`` like the following:

.. code:: sql

    SELECT col_string FROM one_row_complex
    WHERE col_string = %(param)s OR col_string LIKE 'a%%'

.. _`DB API paramstyle`: https://www.python.org/dev/peps/pep-0249/#paramstyle
.. _`named placeholders`: https://pyformat.info/#named_placeholders

SQLAlchemy
~~~~~~~~~~

Install SQLAlchemy with ``pip install "SQLAlchemy>=1.0.0, <2.0.0"`` or ``pip install PyAthena[SQLAlchemy]``.
Supported SQLAlchemy is 1.0.0 or higher and less than 2.0.0.

.. code:: python

    from urllib.parse import quote_plus
    from sqlalchemy.engine import create_engine
    from sqlalchemy.sql.expression import select
    from sqlalchemy.sql.functions import func
    from sqlalchemy.sql.schema import Table, MetaData

    conn_str = "awsathena+rest://{aws_access_key_id}:{aws_secret_access_key}@athena.{region_name}.amazonaws.com:443/"\
               "{schema_name}?s3_staging_dir={s3_staging_dir}"
    engine = create_engine(conn_str.format(
        aws_access_key_id=quote_plus("YOUR_ACCESS_KEY_ID"),
        aws_secret_access_key=quote_plus("YOUR_SECRET_ACCESS_KEY"),
        region_name="us-west-2",
        schema_name="default",
        s3_staging_dir=quote_plus("s3://YOUR_S3_BUCKET/path/to/")))
    with engine.connect() as connection:
        many_rows = Table("many_rows", MetaData(), autoload_with=connection)
        result = connection.execute(select([func.count("*")], from_obj=many_rows))
        print(result.scalar())

The connection string has the following format:

.. code:: text

    awsathena+rest://{aws_access_key_id}:{aws_secret_access_key}@athena.{region_name}.amazonaws.com:443/{schema_name}?s3_staging_dir={s3_staging_dir}&...

If you do not specify ``aws_access_key_id`` and ``aws_secret_access_key`` using instance profile or boto3 configuration file:

.. code:: text

    awsathena+rest://:@athena.{region_name}.amazonaws.com:443/{schema_name}?s3_staging_dir={s3_staging_dir}&...

NOTE: ``s3_staging_dir`` requires quote. If ``aws_access_key_id``, ``aws_secret_access_key`` and other parameter contain special characters, quote is also required.

Dialect & driver
^^^^^^^^^^^^^^^^

+-----------+--------+------------------+-----------------+
| Dialect   | Driver | Schema           | Cursor          |
+===========+========+==================+=================+
| awsathena |        | awsathena        | DefaultCursor   |
+-----------+--------+------------------+-----------------+
| awsathena | rest   | awsathena+rest   | DefaultCursor   |
+-----------+--------+------------------+-----------------+
| awsathena | pandas | awsathena+pandas | `PandasCursor`_ |
+-----------+--------+------------------+-----------------+
| awsathena | arrow  | awsathena+arrow  | `ArrowCursor`_  |
+-----------+--------+------------------+-----------------+
| awsathena | jdbc   | awsathena+jdbc   | `PyAthenaJDBC`_ |
+-----------+--------+------------------+-----------------+

.. _`PyAthenaJDBC`: https://github.com/laughingman7743/PyAthenaJDBC


Dialect options
^^^^^^^^^^^^^^^

Table options
#############

location
    Type:
        str
    Description:
        Specifies the location of the underlying data in the Amazon S3 from which the table is created.
    value:
        s3://bucket/path/to/
    Example:
        .. code:: python

            Table("some_table", metadata, ..., awsathena_location="s3://bucket/path/to/")
compression
    Type:
        str
    Description:
        Specifies the compression format.
    Value:
        * BZIP2
        * DEFLATE
        * GZIP
        * LZ4
        * LZO
        * SNAPPY
        * ZLIB
        * ZSTD
        * NONE|UNCOMPRESSED
    Example:
        .. code:: python

            Table("some_table", metadata, ..., awsathena_compression="SNAPPY")
row_format
    Type:
        str
    Description:
        Specifies the row format of the table and its underlying source data if applicable.
    Value:
        * [DELIMITED FIELDS TERMINATED BY char [ESCAPED BY char]]
        * [DELIMITED COLLECTION ITEMS TERMINATED BY char]
        * [MAP KEYS TERMINATED BY char]
        * [LINES TERMINATED BY char]
        * [NULL DEFINED AS char]
        * SERDE 'serde_name'
    Example:
        .. code:: python

            Table("some_table", metadata, ..., awsathena_row_format="SERDE 'org.openx.data.jsonserde.JsonSerDe'")
file_format
    Type:
        str
    Description:
        Specifies the file format for table data.
    Value:
        * SEQUENCEFILE
        * TEXTFILE
        * RCFILE
        * ORC
        * PARQUET
        * AVRO
        * ION
        * INPUTFORMAT input_format_classname OUTPUTFORMAT output_format_classname
    Example:
        .. code:: python

            Table("some_table", metadata, ..., awsathena_file_format="PARQUET")
            Table("some_table", metadata, ..., awsathena_file_format="INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'")
serdeproperties
    Type:
        dict[str, str]
    Description:
        Specifies one or more custom properties allowed in SerDe.
    Value:
        .. code:: python

            { "property_name": "property_value", "property_name": "property_value", ... }
    Example:
        .. code:: python

            Table("some_table", metadata, ..., awsathena_serdeproperties={
                "separatorChar": ",", "escapeChar": "\\\\"
            })
tblproperties
    Type:
        dict[str, str]
    Description:
        Specifies custom metadata key-value pairs for the table definition in addition to predefined table properties.
    Value:
        .. code:: python

            { "property_name": "property_value", "property_name": "property_value", ... }
    Example:
        .. code:: python

            Table("some_table", metadata, ..., awsathena_tblproperties={
                "projection.enabled": "true",
                "projection.dt.type": "date",
                "projection.dt.range": "NOW-1YEARS,NOW",
                "projection.dt.format": "yyyy-MM-dd",
            })
bucket_count
    Type:
        int
    Description:
        The number of buckets for bucketing your data.
    Value:
        Integer value greater than or equal to 0
    Example:
        .. code:: python

            Table("some_table", metadata, ..., awsathena_bucket_count=5)

All table options can also be configured with the connection string as follows:

.. code:: text

    awsathena+rest://:@athena.us-west-2.amazonaws.com:443/default?s3_staging_dir=s3%3A%2F%2Fbucket%2Fpath%2Fto%2F&location=s3%3A%2F%2Fbucket%2Fpath%2Fto%2F&file_format=parquet&compression=snappy&...

``serdeproperties`` and ``tblproperties`` must be converted to strings in the ``'key'='value','key'='value'`` format and url encoded.
If single quotes are included, escape them with a backslash.

For example, if you configure a projection setting ``'projection.enabled'='true','projection.dt.type'='date','projection.dt.range'='NOW-1YEARS,NOW','projection.dt.format'= 'yyyy-MM-dd'`` in tblproperties, it would look like this

.. code:: text

    awsathena+rest://:@athena.us-west-2.amazonaws.com:443/default?s3_staging_dir=s3%3A%2F%2Fbucket%2Fpath%2Fto%2F&tblproperties=%27projection.enabled%27%3D%27true%27%2C%27projection.dt.type%27%3D%27date%27%2C%27projection.dt.range%27%3D%27NOW-1YEARS%2CNOW%27%2C%27projection.dt.format%27%3D+%27yyyy-MM-dd%27

Column options
##############

partition
    Type:
        bool
    Description:
        Specifies a key for partitioning data.
    Value:
        True / False
    Example:
        .. code:: python

            Column("some_column", types.String, ..., awsathena_partition=True)
cluster
    Type:
        bool
    Description:
        Divides the data in the specified column into data subsets called buckets, with or without partitioning.
    Value:
        True / False
    Example:
        .. code:: python

            Column("some_column", types.String, ..., awsathena_cluster=True)

To configure column options from the connection string, specify the column name as a comma-separated string.

.. code:: text

    awsathena+rest://:@athena.us-west-2.amazonaws.com:443/default?partition=column1%2Ccolumn2&cluster=column1%2Ccolumn2&...

If you want to limit the column options to specific table names only, specify the table and column names connected by dots as a comma-separated string.

.. code:: text

    awsathena+rest://:@athena.us-west-2.amazonaws.com:443/default?partition=table1.column1%2Ctable1.column2&cluster=table2.column1%2Ctable2.column2&...

Pandas
~~~~~~

As DataFrame
^^^^^^^^^^^^

You can use the `pandas.read_sql_query`_ to handle the query results as a `pandas.DataFrame object`_.

.. code:: python

    from pyathena import connect
    import pandas as pd

    conn = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                   region_name="us-west-2")
    df = pd.read_sql_query("SELECT * FROM many_rows", conn)
    print(df.head())

NOTE: `Poor performance when using pandas.read_sql #222 <https://github.com/laughingman7743/PyAthena/issues/222>`_

The ``pyathena.pandas.util`` package also has helper methods.

.. code:: python

    from pyathena import connect
    from pyathena.pandas.util import as_pandas

    cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                     region_name="us-west-2").cursor()
    cursor.execute("SELECT * FROM many_rows")
    df = as_pandas(cursor)
    print(df.describe())

If you want to use the query results output to S3 directly, you can use `PandasCursor`_.
This cursor fetches query results faster than the default cursor. (See `benchmark results`_.)

.. _`pandas.read_sql_query`: https://pandas.pydata.org/docs/reference/api/pandas.read_sql_query.html
.. _`benchmark results`: benchmarks/

To SQL
^^^^^^

You can use `pandas.DataFrame.to_sql`_ to write records stored in DataFrame to Amazon Athena.
`pandas.DataFrame.to_sql`_ uses `SQLAlchemy`_, so you need to install it.

.. code:: python

    import pandas as pd
    from urllib.parse import quote_plus
    from sqlalchemy import create_engine

    conn_str = "awsathena+rest://:@athena.{region_name}.amazonaws.com:443/"\
               "{schema_name}?s3_staging_dir={s3_staging_dir}&location={location}&compression=snappy"
    engine = create_engine(conn_str.format(
        region_name="us-west-2",
        schema_name="YOUR_SCHEMA",
        s3_staging_dir=quote_plus("s3://YOUR_S3_BUCKET/path/to/"),
        location=quote_plus("s3://YOUR_S3_BUCKET/path/to/")))

    df = pd.DataFrame({"a": [1, 2, 3, 4, 5]})
    df.to_sql("YOUR_TABLE", engine, schema="YOUR_SCHEMA", index=False, if_exists="replace", method="multi")

The location of the Amazon S3 table is specified by the ``location`` parameter in the connection string.
If ``location`` is not specified, ``s3_staging_dir`` parameter will be used. The following rules apply.

.. code:: text

    s3://{location or s3_staging_dir}/{schema}/{table}/

The file format, row format, and compression settings are specified in the connection string, see `Table options`_.

The ``pyathena.pandas.util`` package also has helper methods.

.. code:: python

    import pandas as pd
    from pyathena import connect
    from pyathena.pandas.util import to_sql

    conn = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                   region_name="us-west-2")
    df = pd.DataFrame({"a": [1, 2, 3, 4, 5]})
    to_sql(df, "YOUR_TABLE", conn, "s3://YOUR_S3_BUCKET/path/to/",
           schema="YOUR_SCHEMA", index=False, if_exists="replace")

This helper method supports partitioning.

.. code:: python

    import pandas as pd
    from datetime import date
    from pyathena import connect
    from pyathena.pandas.util import to_sql

    conn = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                   region_name="us-west-2")
    df = pd.DataFrame({
        "a": [1, 2, 3, 4, 5],
        "dt": [
            date(2020, 1, 1), date(2020, 1, 1), date(2020, 1, 1),
            date(2020, 1, 2),
            date(2020, 1, 3)
        ],
    })
    to_sql(df, "YOUR_TABLE", conn, "s3://YOUR_S3_BUCKET/path/to/",
           schema="YOUR_SCHEMA", partitions=["dt"])

    cursor = conn.cursor()
    cursor.execute("SHOW PARTITIONS YOUR_TABLE")
    print(cursor.fetchall())

Conversion to Parquet and upload to S3 use `ThreadPoolExecutor`_ by default.
It is also possible to use `ProcessPoolExecutor`_.

.. code:: python

    import pandas as pd
    from concurrent.futures.process import ProcessPoolExecutor
    from pyathena import connect
    from pyathena.pandas.util import to_sql

    conn = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                   region_name="us-west-2")
    df = pd.DataFrame({"a": [1, 2, 3, 4, 5]})
    to_sql(df, "YOUR_TABLE", conn, "s3://YOUR_S3_BUCKET/path/to/",
           schema="YOUR_SCHEMA", index=False, if_exists="replace",
           chunksize=1, executor_class=ProcessPoolExecutor, max_workers=5)

.. _`pandas.DataFrame.to_sql`: https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_sql.html
.. _`ThreadPoolExecutor`: https://docs.python.org/3/library/concurrent.futures.html#threadpoolexecutor
.. _`ProcessPoolExecutor`: https://docs.python.org/3/library/concurrent.futures.html#processpoolexecutor

DictCursor
~~~~~~~~~~

DictCursor retrieve the query execution result as a dictionary type with column names and values.

You can use the DictCursor by specifying the ``cursor_class``
with the connect method or connection object.

.. code:: python

    from pyathena import connect
    from pyathena.cursor import DictCursor

    cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                     region_name="us-west-2",
                     cursor_class=DictCursor).cursor()

.. code:: python

    from pyathena.connection import Connection
    from pyathena.cursor import DictCursor

    cursor = Connection(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                        region_name="us-west-2",
                        cursor_class=DictCursor).cursor()

It can also be used by specifying the cursor class when calling the connection object's cursor method.

.. code:: python

    from pyathena import connect
    from pyathena.cursor import DictCursor

    cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                     region_name="us-west-2").cursor(DictCursor)

.. code:: python

    from pyathena.connection import Connection
    from pyathena.cursor import DictCursor

    cursor = Connection(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                        region_name="us-west-2").cursor(DictCursor)

The basic usage is the same as the Cursor.

.. code:: python

    from pyathena.connection import Connection
    from pyathena.cursor import DictCursor

    cursor = Connection(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                        region_name="us-west-2").cursor(DictCursor)
    cursor.execute("SELECT * FROM many_rows LIMIT 10")
    for row in cursor:
        print(row["a"])

If you want to change the dictionary type (e.g., use OrderedDict), you can specify like the following.

.. code:: python

    from collections import OrderedDict
    from pyathena import connect
    from pyathena.cursor import DictCursor

    cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                     region_name="us-west-2",
                     cursor_class=DictCursor).cursor(dict_type=OrderedDict)

.. code:: python

    from collections import OrderedDict
    from pyathena import connect
    from pyathena.cursor import DictCursor

    cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                     region_name="us-west-2").cursor(cursor=DictCursor, dict_type=OrderedDict)

AsyncCursor
~~~~~~~~~~~

AsyncCursor is a simple implementation using the concurrent.futures package.
This cursor does not follow the `DB API 2.0 (PEP 249)`_.

You can use the AsyncCursor by specifying the ``cursor_class``
with the connect method or connection object.

.. code:: python

    from pyathena import connect
    from pyathena.async_cursor import AsyncCursor

    cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                     region_name="us-west-2",
                     cursor_class=AsyncCursor).cursor()

.. code:: python

    from pyathena.connection import Connection
    from pyathena.async_cursor import AsyncCursor

    cursor = Connection(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                        region_name="us-west-2",
                        cursor_class=AsyncCursor).cursor()

It can also be used by specifying the cursor class when calling the connection object's cursor method.

.. code:: python

    from pyathena import connect
    from pyathena.async_cursor import AsyncCursor

    cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                     region_name="us-west-2").cursor(AsyncCursor)

.. code:: python

    from pyathena.connection import Connection
    from pyathena.async_cursor import AsyncCursor

    cursor = Connection(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                        region_name="us-west-2").cursor(AsyncCursor)

The default number of workers is 5 or cpu number * 5.
If you want to change the number of workers you can specify like the following.

.. code:: python

    from pyathena import connect
    from pyathena.async_cursor import AsyncCursor

    cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                     region_name="us-west-2",
                     cursor_class=AsyncCursor).cursor(max_workers=10)

The execute method of the AsyncCursor returns the tuple of the query ID and the `future object`_.

.. code:: python

    from pyathena import connect
    from pyathena.async_cursor import AsyncCursor

    cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                     region_name="us-west-2",
                     cursor_class=AsyncCursor).cursor()

    query_id, future = cursor.execute("SELECT * FROM many_rows")

The return value of the `future object`_ is an ``AthenaResultSet`` object.
This object has an interface that can fetch and iterate query results similar to synchronous cursors.
It also has information on the result of query execution.

.. code:: python

    from pyathena import connect
    from pyathena.async_cursor import AsyncCursor

    cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                     region_name="us-west-2",
                     cursor_class=AsyncCursor).cursor()
    query_id, future = cursor.execute("SELECT * FROM many_rows")
    result_set = future.result()
    print(result_set.state)
    print(result_set.state_change_reason)
    print(result_set.completion_date_time)
    print(result_set.submission_date_time)
    print(result_set.data_scanned_in_bytes)
    print(result_set.engine_execution_time_in_millis)
    print(result_set.query_queue_time_in_millis)
    print(result_set.total_execution_time_in_millis)
    print(result_set.query_planning_time_in_millis)
    print(result_set.service_processing_time_in_millis)
    print(result_set.output_location)
    print(result_set.description)
    for row in result_set:
        print(row)

.. code:: python

    from pyathena import connect
    from pyathena.async_cursor import AsyncCursor

    cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                     region_name="us-west-2",
                     cursor_class=AsyncCursor).cursor()
    query_id, future = cursor.execute("SELECT * FROM many_rows")
    result_set = future.result()
    print(result_set.fetchall())

A query ID is required to cancel a query with the AsyncCursor.

.. code:: python

    from pyathena import connect
    from pyathena.async_cursor import AsyncCursor

    cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                     region_name="us-west-2",
                     cursor_class=AsyncCursor).cursor()
    query_id, future = cursor.execute("SELECT * FROM many_rows")
    cursor.cancel(query_id)

NOTE: The cancel method of the `future object`_ does not cancel the query.

.. _`future object`: https://docs.python.org/3/library/concurrent.futures.html#future-objects

AsyncDictCursor
~~~~~~~~~~~~~~~

AsyncDIctCursor is an AsyncCursor that can retrieve the query execution result
as a dictionary type with column names and values.

You can use the DictCursor by specifying the ``cursor_class``
with the connect method or connection object.

.. code:: python

    from pyathena import connect
    from pyathena.async_cursor import AsyncDictCursor

    cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                     region_name="us-west-2",
                     cursor_class=AsyncDictCursor).cursor()

.. code:: python

    from pyathena.connection import Connection
    from pyathena.async_cursor import AsyncDictCursor

    cursor = Connection(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                        region_name="us-west-2",
                        cursor_class=AsyncDictCursor).cursor()

It can also be used by specifying the cursor class when calling the connection object's cursor method.

.. code:: python

    from pyathena import connect
    from pyathena.async_cursor import AsyncDictCursor

    cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                     region_name="us-west-2").cursor(AsyncDictCursor)

.. code:: python

    from pyathena.connection import Connection
    from pyathena.async_cursor import AsyncDictCursor

    cursor = Connection(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                        region_name="us-west-2").cursor(AsyncDictCursor)

The basic usage is the same as the AsyncCursor.

.. code:: python

    from pyathena.connection import Connection
    from pyathena.cursor import DictCursor

    cursor = Connection(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                        region_name="us-west-2").cursor(AsyncDictCursor)
    query_id, future = cursor.execute("SELECT * FROM many_rows LIMIT 10")
    result_set = future.result()
    for row in result_set:
        print(row["a"])

If you want to change the dictionary type (e.g., use OrderedDict), you can specify like the following.

.. code:: python

    from collections import OrderedDict
    from pyathena import connect
    from pyathena.async_cursor import AsyncDictCursor

    cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                     region_name="us-west-2",
                     cursor_class=AsyncDictCursor).cursor(dict_type=OrderedDict)

.. code:: python

    from collections import OrderedDict
    from pyathena import connect
    from pyathena.async_cursor import AsyncDictCursor

    cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                     region_name="us-west-2").cursor(cursor=AsyncDictCursor, dict_type=OrderedDict)

PandasCursor
~~~~~~~~~~~~

PandasCursor directly handles the CSV file of the query execution result output to S3.
This cursor is to download the CSV file after executing the query, and then loaded into `pandas.DataFrame object`_.
Performance is better than fetching data with Cursor.

You can use the PandasCursor by specifying the ``cursor_class``
with the connect method or connection object.

.. code:: python

    from pyathena import connect
    from pyathena.pandas.cursor import PandasCursor

    cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                     region_name="us-west-2",
                     cursor_class=PandasCursor).cursor()

.. code:: python

    from pyathena.connection import Connection
    from pyathena.pandas.cursor import PandasCursor

    cursor = Connection(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                        region_name="us-west-2",
                        cursor_class=PandasCursor).cursor()

It can also be used by specifying the cursor class when calling the connection object's cursor method.

.. code:: python

    from pyathena import connect
    from pyathena.pandas.cursor import PandasCursor

    cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                     region_name="us-west-2").cursor(PandasCursor)

.. code:: python

    from pyathena.connection import Connection
    from pyathena.pandas.cursor import PandasCursor

    cursor = Connection(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                        region_name="us-west-2").cursor(PandasCursor)

The as_pandas method returns a `pandas.DataFrame object`_.

.. code:: python

    from pyathena import connect
    from pyathena.pandas.cursor import PandasCursor

    cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                     region_name="us-west-2",
                     cursor_class=PandasCursor).cursor()

    df = cursor.execute("SELECT * FROM many_rows").as_pandas()
    print(df.describe())
    print(df.head())

Support fetch and iterate query results.

.. code:: python

    from pyathena import connect
    from pyathena.pandas.cursor import PandasCursor

    cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                     region_name="us-west-2",
                     cursor_class=PandasCursor).cursor()

    cursor.execute("SELECT * FROM many_rows")
    print(cursor.fetchone())
    print(cursor.fetchmany())
    print(cursor.fetchall())

.. code:: python

    from pyathena import connect
    from pyathena.pandas.cursor import PandasCursor

    cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                     region_name="us-west-2",
                     cursor_class=PandasCursor).cursor()

    cursor.execute("SELECT * FROM many_rows")
    for row in cursor:
        print(row)

The DATE and TIMESTAMP of Athena's data type are returned as `pandas.Timestamp`_ type.

.. code:: python

    from pyathena import connect
    from pyathena.pandas.cursor import PandasCursor

    cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                     region_name="us-west-2",
                     cursor_class=PandasCursor).cursor()

    cursor.execute("SELECT col_timestamp FROM one_row_complex")
    print(type(cursor.fetchone()[0]))  # <class 'pandas._libs.tslibs.timestamps.Timestamp'>

Execution information of the query can also be retrieved.

.. code:: python

    from pyathena import connect
    from pyathena.pandas.cursor import PandasCursor

    cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                     region_name="us-west-2",
                     cursor_class=PandasCursor).cursor()

    cursor.execute("SELECT * FROM many_rows")
    print(cursor.state)
    print(cursor.state_change_reason)
    print(cursor.completion_date_time)
    print(cursor.submission_date_time)
    print(cursor.data_scanned_in_bytes)
    print(cursor.engine_execution_time_in_millis)
    print(cursor.query_queue_time_in_millis)
    print(cursor.total_execution_time_in_millis)
    print(cursor.query_planning_time_in_millis)
    print(cursor.service_processing_time_in_millis)
    print(cursor.output_location)

If you want to customize the pandas.Dataframe object dtypes and converters, create a converter class like this:

.. code:: python

    from pyathena.converter import Converter

    class CustomPandasTypeConverter(Converter):

        def __init__(self):
            super(CustomPandasTypeConverter, self).__init__(
                mappings=None,
                types={
                    "boolean": object,
                    "tinyint": float,
                    "smallint": float,
                    "integer": float,
                    "bigint": float,
                    "float": float,
                    "real": float,
                    "double": float,
                    "decimal": float,
                    "char": str,
                    "varchar": str,
                    "array": str,
                    "map": str,
                    "row": str,
                    "varbinary": str,
                    "json": str,
                }
            )

        def convert(self, type_, value):
            # Not used in PandasCursor.
            pass

Specify the combination of converter functions in the mappings argument and the dtypes combination in the types argument.

Then you simply specify an instance of this class in the convertes argument when creating a connection or cursor.

.. code:: python

    from pyathena import connect
    from pyathena.pandas.cursor import PandasCursor

    cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                     region_name="us-west-2").cursor(PandasCursor, converter=CustomPandasTypeConverter())

.. code:: python

    from pyathena import connect
    from pyathena.pandas.cursor import PandasCursor

    cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                     region_name="us-west-2",
                     converter=CustomPandasTypeConverter()).cursor(PandasCursor)

If the unload option is enabled, the Parquet file itself has a schema, so the conversion is done to the dtypes according to that schema,
and the ``mappings`` and ``types`` settings of the Converter class are not used.

If you want to change the NaN behavior of pandas.Dataframe,
you can do so by using the ``keep_default_na``, ``na_values`` and ``quoting`` arguments of the cursor object's execute method.

.. code:: python

    from pyathena import connect
    from pyathena.pandas.cursor import PandasCursor

    cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                     region_name="us-west-2",
                     cursor_class=PandasCursor).cursor()
    df = cursor.execute("SELECT * FROM many_rows",
                        keep_default_na=False,
                        na_values=[""]).as_pandas()

NOTE: PandasCursor handles the CSV file on memory. Pay attention to the memory capacity.

.. _`pandas.DataFrame object`: https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html
.. _`pandas.Timestamp`: https://pandas.pydata.org/docs/reference/api/pandas.Timestamp.html

[PandasCursor] Chunksize options
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The Pandas cursor can read the CSV file for each specified number of rows by using the chunksize option.
This option should reduce memory usage.

The chunksize option can be enabled by specifying an integer value in the ``cursor_kwargs`` argument of the connect method or as an argument to the cursor method.

.. code:: python

    from pyathena import connect
    from pyathena.pandas.cursor import PandasCursor

    cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                     region_name="us-west-2",
                     cursor_class=PandasCursor,
                     cursor_kwargs={
                         "chunksize": 1_000_000
                     }).cursor()

.. code:: python

    from pyathena import connect
    from pyathena.pandas.cursor import PandasCursor

    cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                     region_name="us-west-2",
                     cursor_class=PandasCursor).cursor(chunksize=1_000_000)

It can also be specified in the execution method when executing the query.

.. code:: python

    from pyathena import connect
    from pyathena.pandas.cursor import PandasCursor

    cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                     region_name="us-west-2",
                     cursor_class=PandasCursor).cursor()
    cursor.execute("SELECT * FROM many_rows", chunksize=1_000_000)

SQLAlchemy allows this option to be specified in the connection string.

.. code:: text

    awsathena+pandas://:@athena.{region_name}.amazonaws.com:443/{schema_name}?s3_staging_dir={s3_staging_dir}&chunksize=1000000...

When this option is used, the object returned by the as_pandas method is a ``DataFrameIterator`` object.
This object has exactly the same interface as the ``TextFileReader`` object and can be handled in the same way.

.. code:: python

    from pyathena import connect
    from pyathena.pandas.cursor import PandasCursor

    cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                     region_name="us-west-2",
                     cursor_class=PandasCursor).cursor()
    df_iter = cursor.execute("SELECT * FROM many_rows", chunksize=1_000_000).as_pandas()
    for df in df_iter:
        print(df.describe())
        print(df.head())

You can also concatenate them into a single `pandas.DataFrame object`_ using `pandas.concat`_.

.. code:: python

    import pandas
    from pyathena import connect
    from pyathena.pandas.cursor import PandasCursor

    cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                     region_name="us-west-2",
                     cursor_class=PandasCursor).cursor()
    df_iter = cursor.execute("SELECT * FROM many_rows", chunksize=1_000_000).as_pandas()
    df = pandas.concat((df for df in df_iter), ignore_index=True)

You can use the ``get_chunk`` method to retrieve a `pandas.DataFrame object`_ for each specified number of rows.
When all rows have been read, calling the ``get_chunk`` method will raise ``StopIteration``.

.. code:: python

    from pyathena import connect
    from pyathena.pandas.cursor import PandasCursor

    cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                     region_name="us-west-2",
                     cursor_class=PandasCursor).cursor()
    df_iter = cursor.execute("SELECT * FROM many_rows LIMIT 15", chunksize=1_000_000).as_pandas()
    df_iter.get_chunk(10)
    df_iter.get_chunk(10)
    df_iter.get_chunk(10)  # raise StopIteration

.. _`pandas.concat`: https://pandas.pydata.org/docs/reference/api/pandas.concat.html

[PandasCursor] Unload options
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

PandasCursor also supports the unload option, as does `ArrowCursor`_.

See `[ArrowCursor] Unload options`_ for more information.

The unload option can be enabled by specifying it in the ``cursor_kwargs`` argument of the connect method or as an argument to the cursor method.

.. code:: python

    from pyathena import connect
    from pyathena.pandas.cursor import PandasCursor

    cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                     region_name="us-west-2",
                     cursor_class=PandasCursor,
                     cursor_kwargs={
                         "unload": True
                     }).cursor()

.. code:: python

    from pyathena import connect
    from pyathena.pandas.cursor import PandasCursor

    cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                     region_name="us-west-2",
                     cursor_class=PandasCursor).cursor(unload=True)

SQLAlchemy allows this option to be specified in the connection string.

.. code:: text

    awsathena+pandas://:@athena.{region_name}.amazonaws.com:443/{schema_name}?s3_staging_dir={s3_staging_dir}&unload=true...

AsyncPandasCursor
~~~~~~~~~~~~~~~~~

AsyncPandasCursor is an AsyncCursor that can handle `pandas.DataFrame object`_.
This cursor directly handles the CSV of query results output to S3 in the same way as PandasCursor.

You can use the AsyncPandasCursor by specifying the ``cursor_class``
with the connect method or connection object.

.. code:: python

    from pyathena import connect
    from pyathena.pandas.async_cursor import AsyncPandasCursor

    cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                     region_name="us-west-2",
                     cursor_class=AsyncPandasCursor).cursor()

.. code:: python

    from pyathena.connection import Connection
    from pyathena.pandas.async_cursor import AsyncPandasCursor

    cursor = Connection(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                        region_name="us-west-2",
                        cursor_class=AsyncPandasCursor).cursor()

It can also be used by specifying the cursor class when calling the connection object's cursor method.

.. code:: python

    from pyathena import connect
    from pyathena.pandas.async_cursor import AsyncPandasCursor

    cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                     region_name="us-west-2").cursor(AsyncPandasCursor)

.. code:: python

    from pyathena.connection import Connection
    from pyathena.pandas.async_cursor import AsyncPandasCursor

    cursor = Connection(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                        region_name="us-west-2").cursor(AsyncPandasCursor)

The default number of workers is 5 or cpu number * 5.
If you want to change the number of workers you can specify like the following.

.. code:: python

    from pyathena import connect
    from pyathena.pandas.async_cursor import AsyncPandasCursor

    cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                     region_name="us-west-2",
                     cursor_class=AsyncPandasCursor).cursor(max_workers=10)

The execute method of the AsyncPandasCursor returns the tuple of the query ID and the `future object`_.

.. code:: python

    from pyathena import connect
    from pyathena.pandas.async_cursor import AsyncPandasCursor

    cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                     region_name="us-west-2",
                     cursor_class=AsyncPandasCursor).cursor()

    query_id, future = cursor.execute("SELECT * FROM many_rows")

The return value of the `future object`_ is an ``AthenaPandasResultSet`` object.
This object has an interface similar to ``AthenaResultSetObject``.

.. code:: python

    from pyathena import connect
    from pyathena.pandas.async_cursor import AsyncPandasCursor

    cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                     region_name="us-west-2",
                     cursor_class=AsyncPandasCursor).cursor()

    query_id, future = cursor.execute("SELECT * FROM many_rows")
    result_set = future.result()
    print(result_set.state)
    print(result_set.state_change_reason)
    print(result_set.completion_date_time)
    print(result_set.submission_date_time)
    print(result_set.data_scanned_in_bytes)
    print(result_set.engine_execution_time_in_millis)
    print(result_set.query_queue_time_in_millis)
    print(result_set.total_execution_time_in_millis)
    print(result_set.query_planning_time_in_millis)
    print(result_set.service_processing_time_in_millis)
    print(result_set.output_location)
    print(result_set.description)
    for row in result_set:
        print(row)

.. code:: python

    from pyathena import connect
    from pyathena.pandas.async_cursor import AsyncPandasCursor

    cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                     region_name="us-west-2",
                     cursor_class=AsyncPandasCursor).cursor()

    query_id, future = cursor.execute("SELECT * FROM many_rows")
    result_set = future.result()
    print(result_set.fetchall())

This object also has an as_pandas method that returns a `pandas.DataFrame object`_ similar to the PandasCursor.

.. code:: python

    from pyathena import connect
    from pyathena.pandas.async_cursor import AsyncPandasCursor

    cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                     region_name="us-west-2",
                     cursor_class=AsyncPandasCursor).cursor()

    query_id, future = cursor.execute("SELECT * FROM many_rows")
    result_set = future.result()
    df = result_set.as_pandas()
    print(df.describe())
    print(df.head())

The DATE and TIMESTAMP of Athena's data type are returned as `pandas.Timestamp`_ type.

.. code:: python

    from pyathena import connect
    from pyathena.pandas.async_cursor import AsyncPandasCursor

    cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                     region_name="us-west-2",
                     cursor_class=AsyncPandasCursor).cursor()

    query_id, future = cursor.execute("SELECT col_timestamp FROM one_row_complex")
    result_set = future.result()
    print(type(result_set.fetchone()[0]))  # <class 'pandas._libs.tslibs.timestamps.Timestamp'>

As with AsyncCursor, you need a query ID to cancel a query.

.. code:: python

    from pyathena import connect
    from pyathena.pandas.async_cursor import AsyncPandasCursor

    cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                     region_name="us-west-2",
                     cursor_class=AsyncPandasCursor).cursor()

    query_id, future = cursor.execute("SELECT * FROM many_rows")
    cursor.cancel(query_id)

As with PandasCursor, the unload option is also available.

.. code:: python

    from pyathena import connect
    from pyathena.pandas.cursor import AsyncPandasCursor

    cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                     region_name="us-west-2",
                     cursor_class=AsyncPandasCursor,
                     cursor_kwargs={
                         "unload": True
                     }).cursor()

.. code:: python

    from pyathena import connect
    from pyathena.pandas.cursor import AsyncPandasCursor

    cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                     region_name="us-west-2",
                     cursor_class=AsyncPandasCursor).cursor(unload=True)

ArrowCursor
~~~~~~~~~~~

ArrowCursor directly handles the CSV file of the query execution result output to S3.
This cursor is to download the CSV file after executing the query, and then loaded into `pyarrow.Table object`_.
Performance is better than fetching data with Cursor.

You can use the ArrowCursor by specifying the ``cursor_class``
with the connect method or connection object.

.. code:: python

    from pyathena import connect
    from pyathena.arrow.cursor import ArrowCursor

    cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                     region_name="us-west-2",
                     cursor_class=ArrowCursor).cursor()

.. code:: python

    from pyathena.connection import Connection
    from pyathena.arrow.cursor import ArrowCursor

    cursor = Connection(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                        region_name="us-west-2",
                        cursor_class=ArrowCursor).cursor()

It can also be used by specifying the cursor class when calling the connection object's cursor method.

.. code:: python

    from pyathena import connect
    from pyathena.arrow.cursor import ArrowCursor

    cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                     region_name="us-west-2").cursor(ArrowCursor)

.. code:: python

    from pyathena.connection import Connection
    from pyathena.arrow.cursor import ArrowCursor

    cursor = Connection(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                        region_name="us-west-2").cursor(ArrowCursor)

The as_arrow method returns a `pyarrow.Table object`_.

.. code:: python

    from pyathena import connect
    from pyathena.arrow.cursor import ArrowCursor

    cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                     region_name="us-west-2",
                     cursor_class=ArrowCursor).cursor()

    table = cursor.execute("SELECT * FROM many_rows").as_arrow()
    print(table)
    print(table.column_names)
    print(table.columns)
    print(table.nbytes)
    print(table.num_columns)
    print(table.num_rows)
    print(table.schema)
    print(table.shape)

Support fetch and iterate query results.

.. code:: python

    from pyathena import connect
    from pyathena.arrow.cursor import ArrowCursor

    cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                     region_name="us-west-2",
                     cursor_class=ArrowCursor).cursor()

    cursor.execute("SELECT * FROM many_rows")
    print(cursor.fetchone())
    print(cursor.fetchmany())
    print(cursor.fetchall())

.. code:: python

    from pyathena import connect
    from pyathena.arrow.cursor import ArrowCursor

    cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                     region_name="us-west-2",
                     cursor_class=ArrowCursor).cursor()

    cursor.execute("SELECT * FROM many_rows")
    for row in cursor:
        print(row)

Execution information of the query can also be retrieved.

.. code:: python

    from pyathena import connect
    from pyathena.arrow.cursor import ArrowCursor

    cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                     region_name="us-west-2",
                     cursor_class=ArrowCursor).cursor()

    cursor.execute("SELECT * FROM many_rows")
    print(cursor.state)
    print(cursor.state_change_reason)
    print(cursor.completion_date_time)
    print(cursor.submission_date_time)
    print(cursor.data_scanned_in_bytes)
    print(cursor.engine_execution_time_in_millis)
    print(cursor.query_queue_time_in_millis)
    print(cursor.total_execution_time_in_millis)
    print(cursor.query_planning_time_in_millis)
    print(cursor.service_processing_time_in_millis)
    print(cursor.output_location)

If you want to customize the `pyarrow.Table object`_ types, create a converter class like this:

.. code:: python

    import pyarrow as pa
    from pyathena.arrow.converter import _to_date
    from pyathena.converter import Converter

    class CustomArrowTypeConverter(Converter):
        def __init__(self) -> None:
            super(CustomArrowTypeConverter, self).__init__(
                mappings={
                    "date": _to_date,
                },
                types={
                    "boolean": pa.bool_(),
                    "tinyint": pa.int8(),
                    "smallint": pa.int16(),
                    "integer": pa.int32(),
                    "bigint": pa.int64(),
                    "float": pa.float32(),
                    "real": pa.float64(),
                    "double": pa.float64(),
                    "char": pa.string(),
                    "varchar": pa.string(),
                    "string": pa.string(),
                    "timestamp": pa.timestamp("ms"),
                    "date": pa.timestamp("ms"),
                    "time": pa.string(),
                    "varbinary": pa.string(),
                    "array": pa.string(),
                    "map": pa.string(),
                    "row": pa.string(),
                    "decimal": pa.string(),
                    "json": pa.string(),
                },
            )

    def convert(self, type_, value):
        converter = self.get(type_)
        return converter(value)

``types`` is used to explicitly specify the Arrow type when reading CSV files.
``mappings`` is used as a conversion method when fetching data from a cursor object.

Then you simply specify an instance of this class in the convertes argument when creating a connection or cursor.

.. code:: python

    from pyathena import connect
    from pyathena.arrow.cursor import ArrowCursor

    cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                     region_name="us-west-2").cursor(ArrowCursor, converter=CustomArrowTypeConverter())

.. code:: python

    from pyathena import connect
    from pyathena.arrow.cursor import ArrowCursor

    cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                     region_name="us-west-2",
                     converter=CustomArrowTypeConverter()).cursor(ArrowCursor)

If the unload option is enabled, the Parquet file itself has a schema, so the conversion is done to the Arrow type according to that schema,
and the ``types`` setting of the Converter class is not used.

[ArrowCursor] Unload options
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

ArrowCursor supports the unload option. When this option is enabled,
queries with SELECT statements are automatically converted to unload statements and executed to Athena,
and the results are output in Parquet format (Snappy compressed) to ``s3_staging_dir``.
The cursor reads the output Parquet file directly.

The output of query results with the unload statement is faster than normal query execution.
In addition, the output Parquet file is split and can be read faster than a CSV file.
We recommend trying this option if you are concerned about the time it takes to execute the query and retrieve the results.

However, unload has some limitations. Please refer to the `official unload documentation`_ for more information on limitations.
As per the limitations of the official documentation, the results of unload will be written to multiple files in parallel,
and the contents of each file will be in sort order, but the relative order of the files to each other will not be sorted.
Note that specifying ORDER BY with this option enabled does not guarantee the sort order of the data.

The unload option can be enabled by specifying it in the ``cursor_kwargs`` argument of the connect method or as an argument to the cursor method.

.. code:: python

    from pyathena import connect
    from pyathena.arrow.cursor import ArrowCursor

    cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                     region_name="us-west-2",
                     cursor_class=ArrowCursor,
                     cursor_kwargs={
                         "unload": True
                     }).cursor()

.. code:: python

    from pyathena import connect
    from pyathena.arrow.cursor import ArrowCursor

    cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                     region_name="us-west-2",
                     cursor_class=ArrowCursor).cursor(unload=True)

SQLAlchemy allows this option to be specified in the connection string.

.. code:: text

    awsathena+arrow://:@athena.{region_name}.amazonaws.com:443/{schema_name}?s3_staging_dir={s3_staging_dir}&unload=true...

If a ``NOT_SUPPORTED`` occurs, a type not supported by unload is included in the result of the SELECT.
Try converting to another type, such as ``SELECT CAST(1 AS VARCHAR) AS name``.

.. code:: text

    pyathena.error.OperationalError: NOT_SUPPORTED: Unsupported Hive type: time

In most cases of ``SYNTAX_ERROR``, you forgot to alias the column in the SELECT result.
Try adding an alias to the SELECTed column, such as ``SELECT 1 AS name``.

.. code:: text

    pyathena.error.OperationalError: SYNTAX_ERROR: line 1:1: Column name not specified at position 1

.. _`pyarrow.Table object`: https://arrow.apache.org/docs/python/generated/pyarrow.Table.html
.. _`official unload documentation`: https://docs.aws.amazon.com/athena/latest/ug/unload.html

AsyncArrowCursor
~~~~~~~~~~~~~~~~

AsyncArrowCursor is an AsyncCursor that can handle `pyarrow.Table object`_.
This cursor directly handles the CSV of query results output to S3 in the same way as ArrowCursor.

You can use the AsyncArrowCursor by specifying the ``cursor_class``
with the connect method or connection object.

.. code:: python

    from pyathena import connect
    from pyathena.arrow.async_cursor import AsyncArrowCursor

    cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                     region_name="us-west-2",
                     cursor_class=AsyncArrowCursor).cursor()

.. code:: python

    from pyathena.connection import Connection
    from pyathena.arrow.async_cursor import AsyncArrowCursor

    cursor = Connection(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                        region_name="us-west-2",
                        cursor_class=AsyncArrowCursor).cursor()

It can also be used by specifying the cursor class when calling the connection object's cursor method.

.. code:: python

    from pyathena import connect
    from pyathena.arrow.async_cursor import AsyncArrowCursor

    cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                     region_name="us-west-2").cursor(AsyncArrowCursor)

.. code:: python

    from pyathena.connection import Connection
    from pyathena.arrow.async_cursor import AsyncArrowCursor

    cursor = Connection(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                        region_name="us-west-2").cursor(AsyncArrowCursor)

The default number of workers is 5 or cpu number * 5.
If you want to change the number of workers you can specify like the following.

.. code:: python

    from pyathena import connect
    from pyathena.arrow.async_cursor import AsyncArrowCursor

    cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                     region_name="us-west-2",
                     cursor_class=AsyncArrowCursor).cursor(max_workers=10)

The execute method of the AsyncArrowCursor returns the tuple of the query ID and the `future object`_.

.. code:: python

    from pyathena import connect
    from pyathena.arrow.async_cursor import AsyncArrowCursor

    cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                     region_name="us-west-2",
                     cursor_class=AsyncArrowCursor).cursor()

    query_id, future = cursor.execute("SELECT * FROM many_rows")

The return value of the `future object`_ is an ``AthenaArrowResultSet`` object.
This object has an interface similar to ``AthenaResultSetObject``.

.. code:: python

    from pyathena import connect
    from pyathena.arrow.async_cursor import AsyncArrowCursor

    cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                     region_name="us-west-2",
                     cursor_class=AsyncArrowCursor).cursor()

    query_id, future = cursor.execute("SELECT * FROM many_rows")
    result_set = future.result()
    print(result_set.state)
    print(result_set.state_change_reason)
    print(result_set.completion_date_time)
    print(result_set.submission_date_time)
    print(result_set.data_scanned_in_bytes)
    print(result_set.engine_execution_time_in_millis)
    print(result_set.query_queue_time_in_millis)
    print(result_set.total_execution_time_in_millis)
    print(result_set.query_planning_time_in_millis)
    print(result_set.service_processing_time_in_millis)
    print(result_set.output_location)
    print(result_set.description)
    for row in result_set:
        print(row)

.. code:: python

    from pyathena import connect
    from pyathena.arrow.async_cursor import AsyncArrowCursor

    cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                     region_name="us-west-2",
                     cursor_class=AsyncArrowCursor).cursor()

    query_id, future = cursor.execute("SELECT * FROM many_rows")
    result_set = future.result()
    print(result_set.fetchall())

This object also has an as_arrow method that returns a `pyarrow.Table object`_ similar to the ArrowCursor.

.. code:: python

    from pyathena import connect
    from pyathena.arrow.async_cursor import AsyncArrowCursor

    cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                     region_name="us-west-2",
                     cursor_class=AsyncArrowCursor).cursor()

    query_id, future = cursor.execute("SELECT * FROM many_rows")
    result_set = future.result()
    table = result_set.as_arrow()
    print(table)
    print(table.column_names)
    print(table.columns)
    print(table.nbytes)
    print(table.num_columns)
    print(table.num_rows)
    print(table.schema)
    print(table.shape)

As with AsyncCursor, you need a query ID to cancel a query.

.. code:: python

    from pyathena import connect
    from pyathena.arrow.async_cursor import AsyncArrowCursor

    cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                     region_name="us-west-2",
                     cursor_class=AsyncArrowCursor).cursor()

    query_id, future = cursor.execute("SELECT * FROM many_rows")
    cursor.cancel(query_id)

As with ArrowCursor, the UNLOAD option is also available.

.. code:: python

    from pyathena import connect
    from pyathena.arrow.cursor import AsyncArrowCursor

    cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                     region_name="us-west-2",
                     cursor_class=AsyncArrowCursor,
                     cursor_kwargs={
                         "unload": True
                     }).cursor()

.. code:: python

    from pyathena import connect
    from pyathena.arrow.cursor import AsyncArrowCursor

    cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                     region_name="us-west-2",
                     cursor_class=AsyncArrowCursor).cursor(unload=True)

Quickly re-run queries
~~~~~~~~~~~~~~~~~~~~~~

Result reuse configuration
^^^^^^^^^^^^^^^^^^^^^^^^^^

Athena engine version 3 allows you to `reuse the results of previous queries`_.

It is available by specifying the arguments ``result_reuse_enable`` and ``result_reuse_minutes`` in the connection object.

.. code:: python

    from pyathena import connect

    conn = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                   region_name="us-west-2",
                   work_group="YOUR_WORK_GROUP",
                   result_reuse_enable=True,
                   result_reuse_minutes=60)

You can also specify ``result_reuse_enable`` and ``result_reuse_minutes`` when executing a query.

.. code:: python

    from pyathena import connect

    cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                     region_name="us-west-2").cursor()
    cursor.execute("SELECT * FROM one_row",
                   work_group="YOUR_WORK_GROUP",
                   result_reuse_enable=True,
                   result_reuse_minutes=60)

If the following error occurs, please use a workgroup configured with Athena engine version 3.

.. code:: text

    pyathena.error.DatabaseError: An error occurred (InvalidRequestException) when calling the StartQueryExecution operation: This functionality is not enabled in the selected engine version. Please check the engine version settings or contact AWS support for further assistance.

If for some reason you cannot use the reuse feature of Athena engine version 3, please use the `Cache configuration`_ implemented by PyAthena.

.. _`reuse the results of previous queries`: https://docs.aws.amazon.com/athena/latest/ug/reusing-query-results.html

Cache configuration
^^^^^^^^^^^^^^^^^^^

**Please use the Result reuse configuration.**

You can attempt to re-use the results from a previously executed query to help save time and money in the cases where your underlying data isn't changing.
Set the ``cache_size`` or ``cache_expiration_time`` parameter of ``cursor.execute()`` to a number larger than 0 to enable caching.

.. code:: python

    from pyathena import connect

    cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                     region_name="us-west-2").cursor()
    cursor.execute("SELECT * FROM one_row")  # run once
    print(cursor.query_id)
    cursor.execute("SELECT * FROM one_row", cache_size=10)  # re-use earlier results
    print(cursor.query_id)  # You should expect to see the same Query ID

The unit of ``expiration_time`` is seconds. To use the results of queries executed up to one hour ago, specify like the following.

.. code:: python

    from pyathena import connect

    cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                     region_name="us-west-2").cursor()
    cursor.execute("SELECT * FROM one_row", cache_expiration_time=3600)  # Use queries executed within 1 hour as cache.

If ``cache_size`` is not specified, the value of ``sys.maxsize`` will be automatically set and all query results executed up to one hour ago will be checked.
Therefore, it is recommended to specify ``cache_expiration_time`` together with ``cache_size`` like the following.

.. code:: python

    from pyathena import connect

    cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                     region_name="us-west-2").cursor()
    cursor.execute("SELECT * FROM one_row", cache_size=100, cache_expiration_time=3600)  # Use the last 100 queries within 1 hour as cache.

Results will only be re-used if the query strings match *exactly*,
and the query was a DML statement (the assumption being that you always want to re-run queries like ``CREATE TABLE`` and ``DROP TABLE``).

The S3 staging directory is not checked, so it's possible that the location of the results is not in your provided ``s3_staging_dir``.

Credentials
-----------

Support `Boto3 credentials`_.

.. _`Boto3 credentials`: http://boto3.readthedocs.io/en/latest/guide/configuration.html

Additional environment variable:

.. code:: bash

    $ export AWS_ATHENA_S3_STAGING_DIR=s3://YOUR_S3_BUCKET/path/to/
    $ export AWS_ATHENA_WORK_GROUP=YOUR_WORK_GROUP

Examples
~~~~~~~~

Passing credentials as parameters
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code:: python

    from pyathena import connect

    cursor = connect(aws_access_key_id="YOUR_ACCESS_KEY_ID",
                     aws_secret_access_key="YOUR_SECRET_ACCESS_KEY",
                     s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                     region_name="us-west-2").cursor()

.. code:: python

    from pyathena import connect

    cursor = connect(aws_access_key_id="YOUR_ACCESS_KEY_ID",
                     aws_secret_access_key="YOUR_SECRET_ACCESS_KEY",
                     aws_session_token="YOUR_SESSION_TOKEN",
                     s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                     region_name="us-west-2").cursor()

Multi-factor authentication
^^^^^^^^^^^^^^^^^^^^^^^^^^^

You will be prompted to enter the MFA code.
The program execution will be blocked until the MFA code is entered.

.. code:: python

    from pyathena import connect

    cursor = connect(duration_seconds=3600,
                     serial_number="arn:aws:iam::ACCOUNT_NUMBER_WITHOUT_HYPHENS:mfa/MFA_DEVICE_ID",
                     s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                     region_name="us-west-2").cursor()

Shared credentials file
^^^^^^^^^^^^^^^^^^^^^^^

The shared credentials file has a default location of ~/.aws/credentials.

If you use the default profile, there is no need to specify credential information.

.. code:: python

    from pyathena import connect

    cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                     region_name="us-west-2").cursor()

You can also specify a profile other than the default.

.. code:: python

    from pyathena import connect

    cursor = connect(profile_name="YOUR_PROFILE_NAME",
                     s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                     region_name="us-west-2").cursor()

Assume role provider
^^^^^^^^^^^^^^^^^^^^

.. code:: python

    from pyathena import connect

    cursor = connect(role_arn="YOUR_ASSUME_ROLE_ARN",
                     role_session_name="PyAthena-session",
                     duration_seconds=3600,
                     s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                     region_name="us-west-2").cursor()

Assume role provider with MFA
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

You will be prompted to enter the MFA code.
The program execution will be blocked until the MFA code is entered.

.. code:: python

    from pyathena import connect

    cursor = connect(role_arn="YOUR_ASSUME_ROLE_ARN",
                     role_session_name="PyAthena-session",
                     duration_seconds=3600,
                     serial_number="arn:aws:iam::ACCOUNT_NUMBER_WITHOUT_HYPHENS:mfa/MFA_DEVICE_ID",
                     s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                     region_name="us-west-2").cursor()

Instance profiles
^^^^^^^^^^^^^^^^^

No need to specify credential information.

.. code:: python

    from pyathena import connect

    cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                     region_name="us-west-2").cursor()

Testing
-------

Depends on the following environment variables:

.. code:: bash

    $ export AWS_DEFAULT_REGION=us-west-2
    $ export AWS_ATHENA_S3_STAGING_DIR=s3://YOUR_S3_BUCKET/path/to/
    $ export AWS_ATHENA_WORKGROUP=pyathena-test

In addition, you need to create a workgroup with the `Query result location` set to the name specified in the `AWS_ATHENA_WORKGROUP` environment variable.
If primary is not available as the default workgroup, specify an alternative workgroup name for the default in the environment variable `AWS_ATHENA_DEFAULT_WORKGROUP`.

.. code:: bash

    $ export AWS_ATHENA_DEFAULT_WORKGROUP=DEFAULT_WORKGROUP

Run test
~~~~~~~~

.. code:: bash

    $ pip install poetry
    $ poetry install -v
    $ poetry run pytest

Run test multiple Python versions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code:: bash

    $ pip install poetry
    $ poetry install -v
    $ pyenv local 3.11.1 3.10.1 3.9.1 3.8.2 3.7.2
    $ poetry run tox

GitHub Actions
~~~~~~~~~~~~~~

GitHub Actions uses OpenID Connect (OIDC) to access AWS resources. You will need to refer to the `GitHub Actions documentation`_ to configure it.

.. _`GitHub Actions documentation`: https://docs.github.com/actions/deployment/security-hardening-your-deployments/configuring-openid-connect-in-amazon-web-services

The CloudFormation templates for creating GitHub OIDC Provider and IAM Role can be found in the `aws-actions/configure-aws-credentials repository`_.

.. _`aws-actions/configure-aws-credentials repository`: https://github.com/aws-actions/configure-aws-credentials#sample-iam-role-cloudformation-template

Under `scripts/cloudformation`_ you will also find a CloudFormation template with additional permissions and workgroup settings needed for testing.

.. _`scripts/cloudformation`: scripts/cloudformation/

The example of the CloudFormation execution command is the following:

.. code:: bash

    $ aws --region us-west-2 \
        cloudformation create-stack \
        --stack-name github-actions-oidc-pyathena \
        --capabilities CAPABILITY_NAMED_IAM \
        --template-body file://./scripts/cloudformation/github_actions_oidc.yaml \
        --parameters ParameterKey=GitHubOrg,ParameterValue=laughingman7743 \
          ParameterKey=RepositoryName,ParameterValue=PyAthena \
          ParameterKey=BucketName,ParameterValue=laughingman7743-athena \
          ParameterKey=RoleName,ParameterValue=github-actions-oidc-pyathena-test \
          ParameterKey=WorkGroupName,ParameterValue=pyathena-test

Code formatting
---------------

The code formatting uses `black`_ and `isort`_.

Appy format
~~~~~~~~~~~

.. code:: bash

    $ make fmt

Check format
~~~~~~~~~~~~

.. code:: bash

    $ make chk

.. _`black`: https://github.com/psf/black
.. _`isort`: https://github.com/timothycrosley/isort

License
-------

`MIT license`_

Many of the implementations in this library are based on `PyHive`_, thanks for `PyHive`_.

.. _`MIT license`: LICENSE
.. _`PyHive`: https://github.com/dropbox/PyHive
