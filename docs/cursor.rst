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

.. _query-execution-callback:

Query Execution Callback
-------------------------

PyAthena provides a callback mechanism that allows you to get immediate access to the query ID 
as soon as the ``start_query_execution`` API call is made, before waiting for query completion.
This is useful for monitoring, logging, or cancelling long-running queries from another thread.

The ``on_start_query_execution`` callback can be configured at both the connection level and 
the execute level. When both are set, both callbacks will be invoked.

Connection-level callback
~~~~~~~~~~~~~~~~~~~~~~~~~

You can set a default callback for all queries executed through a connection:

.. code:: python

    from pyathena import connect

    def query_callback(query_id):
        print(f"Query started with ID: {query_id}")
        # You can use query_id for monitoring or cancellation

    cursor = connect(
        s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
        region_name="us-west-2",
        on_start_query_execution=query_callback
    ).cursor()
    
    cursor.execute("SELECT * FROM many_rows")  # Callback will be invoked

Execute-level callback
~~~~~~~~~~~~~~~~~~~~~~

You can also specify a callback for individual query executions:

.. code:: python

    from pyathena import connect

    def specific_callback(query_id):
        print(f"Specific query started: {query_id}")

    cursor = connect(
        s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
        region_name="us-west-2"
    ).cursor()
    
    cursor.execute(
        "SELECT * FROM many_rows", 
        on_start_query_execution=specific_callback
    )

Query cancellation example
~~~~~~~~~~~~~~~~~~~~~~~~~~

A common use case is to enable query cancellation from another thread:

.. code:: python

    import threading
    import time
    from pyathena import connect

    # Global variable to store query ID
    current_query_id = None

    def store_query_id(query_id):
        global current_query_id
        current_query_id = query_id
        print(f"Query started: {query_id}")

    def cancel_if_needed():
        time.sleep(10)  # Wait 10 seconds
        if current_query_id:
            cursor.cancel()  # Cancel the query
            print(f"Cancelled query: {current_query_id}")

    cursor = connect(
        s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
        region_name="us-west-2",
        on_start_query_execution=store_query_id
    ).cursor()

    # Start cancellation thread
    cancel_thread = threading.Thread(target=cancel_if_needed)
    cancel_thread.start()

    try:
        cursor.execute("SELECT * FROM very_large_table")
        print("Query completed successfully")
    except Exception as e:
        print(f"Query failed or was cancelled: {e}")

Multiple callbacks
~~~~~~~~~~~~~~~~~~~

When both connection-level and execute-level callbacks are specified, 
both callbacks will be invoked:

.. code:: python

    from pyathena import connect

    def connection_callback(query_id):
        print(f"Connection callback: {query_id}")
        # Log to monitoring system

    def execute_callback(query_id):
        print(f"Execute callback: {query_id}")
        # Store for cancellation if needed

    cursor = connect(
        s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
        region_name="us-west-2",
        on_start_query_execution=connection_callback
    ).cursor()
    
    # This will invoke both connection_callback and execute_callback
    cursor.execute(
        "SELECT 1", 
        on_start_query_execution=execute_callback
    )

Supported cursor types
~~~~~~~~~~~~~~~~~~~~~~

The ``on_start_query_execution`` callback is supported by the following cursor types:

* ``Cursor`` (default cursor)
* ``DictCursor`` 
* ``ArrowCursor``
* ``PandasCursor``

Note: ``AsyncCursor`` and its variants do not support this callback as they already 
return the query ID immediately through their different execution model.

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

.. _`DB API 2.0 (PEP 249)`: https://www.python.org/dev/peps/pep-0249/
.. _`future object`: https://docs.python.org/3/library/concurrent.futures.html#future-objects
