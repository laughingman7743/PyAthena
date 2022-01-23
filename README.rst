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

PyAthena
========

PyAthena is a Python `DB API 2.0 (PEP 249)`_ client for `Amazon Athena`_.

.. _`DB API 2.0 (PEP 249)`: https://www.python.org/dev/peps/pep-0249/
.. _`Amazon Athena`: https://docs.aws.amazon.com/athena/latest/APIReference/Welcome.html

Requirements
------------

* Python

  - CPython 3.7 3.8 3.9 3.10

Installation
------------

.. code:: bash

    $ pip install PyAthena

Extra packages:

+---------------+--------------------------------------+------------------+
| Package       | Install command                      | Version          |
+===============+======================================+==================+
| Pandas        | ``pip install PyAthena[Pandas]``     | >=1.0.0          |
+---------------+--------------------------------------+------------------+
| SQLAlchemy    | ``pip install PyAthena[SQLAlchemy]`` | >=1.0.0, <2.0.0  |
+---------------+--------------------------------------+------------------+

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
| awsathena | jdbc   | awsathena+jdbc   | `PyAthenaJDBC`_ |
+-----------+--------+------------------+-----------------+

.. _`PyAthenaJDBC`: https://github.com/laughingman7743/PyAthenaJDBC

Pandas
~~~~~~

As DataFrame
^^^^^^^^^^^^

You can use the `pandas.read_sql_query`_ to handle the query results as a `DataFrame object`_.

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

.. _`pandas.read_sql_query`: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_sql_query.html
.. _`benchmark results`: benchmarks/README.rst

To SQL
^^^^^^

You can use `pandas.DataFrame.to_sql`_ to write records stored in DataFrame to Amazon Athena.
`pandas.DataFrame.to_sql`_ uses `SQLAlchemy`_, so you need to install it.

.. code:: python

    import pandas as pd
    from urllib.parse import quote_plus
    from sqlalchemy import create_engine

    conn_str = "awsathena+rest://:@athena.{region_name}.amazonaws.com:443/"\
               "{schema_name}?s3_staging_dir={s3_staging_dir}&s3_dir={s3_dir}&compression=snappy"
    engine = create_engine(conn_str.format(
        region_name="us-west-2",
        schema_name="YOUR_SCHEMA",
        s3_staging_dir=quote_plus("s3://YOUR_S3_BUCKET/path/to/"),
        s3_dir=quote_plus("s3://YOUR_S3_BUCKET/path/to/")))

    df = pd.DataFrame({"a": [1, 2, 3, 4, 5]})
    df.to_sql("YOUR_TABLE", engine, schema="YOUR_SCHEMA", index=False, if_exists="replace", method="multi")

The location of the Amazon S3 table is specified by the ``s3_dir`` parameter in the connection string.
If ``s3_dir`` is not specified, ``s3_staging_dir`` parameter will be used. The following rules apply.

.. code:: text

    s3://{s3_dir or s3_staging_dir}/{schema}/{table}/

The data format only supports Parquet. The compression format is specified by the ``compression`` parameter in the connection string.

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

.. _`pandas.DataFrame.to_sql`: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.to_sql.html
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

AsynchronousCursor
~~~~~~~~~~~~~~~~~~

AsynchronousCursor is a simple implementation using the concurrent.futures package.
This cursor does not follow the `DB API 2.0 (PEP 249)`_.

You can use the AsynchronousCursor by specifying the ``cursor_class``
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

The execute method of the AsynchronousCursor returns the tuple of the query ID and the `future object`_.

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

A query ID is required to cancel a query with the AsynchronousCursor.

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

AsynchronousDictCursor
~~~~~~~~~~~~~~~~~~~~~~

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
This cursor is to download the CSV file after executing the query, and then loaded into `DataFrame object`_.
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

The as_pandas method returns a `DataFrame object`_.

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

If you want to customize the Dataframe object dtypes and converters, create a converter class like this:

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

If you want to change the NaN behavior of Pandas Dataframe,
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

.. _`DataFrame object`: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html
.. _`pandas.Timestamp`: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Timestamp.html

AsyncPandasCursor
~~~~~~~~~~~~~~~~~

AsyncPandasCursor is an AsyncCursor that can handle Pandas DataFrame.
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

The execute method of the AsynchronousPandasCursor returns the tuple of the query ID and the `future object`_.

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

This object also has an as_pandas method that returns a `DataFrame object`_ similar to the PandasCursor.

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

As with AsynchronousCursor, you need a query ID to cancel a query.

.. code:: python

    from pyathena import connect
    from pyathena.pandas.async_cursor import AsyncPandasCursor

    cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                     region_name="us-west-2",
                     cursor_class=AsyncPandasCursor).cursor()

    query_id, future = cursor.execute("SELECT * FROM many_rows")
    cursor.cancel(query_id)

Quickly re-run queries
~~~~~~~~~~~~~~~~~~~~~~

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

    $ export AWS_ACCESS_KEY_ID=YOUR_ACCESS_KEY_ID
    $ export AWS_SECRET_ACCESS_KEY=YOUR_SECRET_ACCESS_KEY
    $ export AWS_DEFAULT_REGION=us-west-2
    $ export AWS_ATHENA_S3_STAGING_DIR=s3://YOUR_S3_BUCKET/path/to/

And you need to create a workgroup named ``test-pyathena`` with the ``Query result location`` configuration.

Run test
~~~~~~~~

.. code:: bash

    $ pip install poetry
    $ poetry install -v
    $ poetry run scripts/test_data/upload_test_data.sh
    $ poetry run pytest
    $ poetry run scripts/test_data/delete_test_data.sh

Run test multiple Python versions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code:: bash

    $ pip install poetry
    $ poetry install -v
    $ poetry run scripts/test_data/upload_test_data.sh
    $ pyenv local 3.10.1 3.9.1 3.8.2 3.7.2
    $ poetry run tox
    $ poetry run scripts/test_data/delete_test_data.sh

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
