Cursor
======

.. _default_cursor:

DefaultCursor
-------------

See :ref:`usage`.

.. _dict-cursor:

DictCursor
----------

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

.. _async-cursor:

AsyncCursor
-----------

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

.. _async-dict-cursor:

AsyncDictCursor
---------------

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
------------

See :ref:`pandas-cursor`.

AsyncPandasCursor
-----------------

See :ref:`async-pandas-cursor`.

ArrowCursor
-----------

See :ref:`arrow-cursor`.

AsyncArrowCursor
----------------

See :ref:`async-arrow-cursor`.

SparkCursor
-----------

See :ref:`spark-cursor`.

AsyncSparkCursor
----------------

See :ref:`async-spark-cursor`.

API Reference
-------------

For detailed API documentation of all cursor classes and their methods, 
see the :ref:`api` section.

.. _`DB API 2.0 (PEP 249)`: https://www.python.org/dev/peps/pep-0249/
.. _`future object`: https://docs.python.org/3/library/concurrent.futures.html#future-objects
