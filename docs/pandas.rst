.. _pandas:

Pandas
======

.. _as-dataframe:

As DataFrame
------------

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

.. _to-sql:

To SQL
------

You can use `pandas.DataFrame.to_sql`_ to write records stored in DataFrame to Amazon Athena.
`pandas.DataFrame.to_sql`_ uses :ref:`sqlalchemy`, so you need to install it.

.. code:: python

    import pandas as pd
    from sqlalchemy import create_engine

    conn_str = "awsathena+rest://:@athena.{region_name}.amazonaws.com:443/"\
               "{schema_name}?s3_staging_dir={s3_staging_dir}&location={location}&compression=snappy"
    engine = create_engine(conn_str.format(
        region_name="us-west-2",
        schema_name="YOUR_SCHEMA",
        s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
        location="s3://YOUR_S3_BUCKET/path/to/"))

    df = pd.DataFrame({"a": [1, 2, 3, 4, 5]})
    df.to_sql("YOUR_TABLE", engine, schema="YOUR_SCHEMA", index=False, if_exists="replace", method="multi")

The location of the Amazon S3 table is specified by the ``location`` parameter in the connection string.
If ``location`` is not specified, ``s3_staging_dir`` parameter will be used. The following rules apply.

.. code:: text

    s3://{location or s3_staging_dir}/{schema}/{table}/

The file format, row format, and compression settings are specified in the connection string.

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

.. _pandas-cursor:

PandasCursor
------------

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
            super().__init__(
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

Chunksize options
~~~~~~~~~~~~~~~~~

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

Unload options
~~~~~~~~~~~~~~

PandasCursor also supports the unload option, as does :ref:`arrow-cursor`.

See `Unload options`_ for more information.

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

.. _async-pandas-cursor:

AsyncPandasCursor
-----------------

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

As with AsyncPandasCursor, you need a query ID to cancel a query.

.. code:: python

    from pyathena import connect
    from pyathena.pandas.async_cursor import AsyncPandasCursor

    cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                     region_name="us-west-2",
                     cursor_class=AsyncPandasCursor).cursor()

    query_id, future = cursor.execute("SELECT * FROM many_rows")
    cursor.cancel(query_id)

As with AsyncPandasCursor, the unload option is also available.

.. code:: python

    from pyathena import connect
    from pyathena.pandas.async_cursor import AsyncPandasCursor

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

.. _`benchmark results`: https://github.com/laughingman7743/PyAthena/tree/master/benchmarks
.. _`ThreadPoolExecutor`: https://docs.python.org/3/library/concurrent.futures.html#threadpoolexecutor
.. _`ProcessPoolExecutor`: https://docs.python.org/3/library/concurrent.futures.html#processpoolexecutor
.. _`future object`: https://docs.python.org/3/library/concurrent.futures.html#future-objects
.. _`pandas.read_sql_query`: https://pandas.pydata.org/docs/reference/api/pandas.read_sql_query.html
.. _`pandas.DataFrame object`: https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html
.. _`pandas.DataFrame.to_sql`: https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_sql.html
.. _`pandas.Timestamp`: https://pandas.pydata.org/docs/reference/api/pandas.Timestamp.html
.. _`pandas.concat`: https://pandas.pydata.org/docs/reference/api/pandas.concat.html
