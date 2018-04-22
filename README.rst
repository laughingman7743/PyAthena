.. image:: https://img.shields.io/pypi/pyversions/PyAthena.svg
    :target: https://pypi.python.org/pypi/PyAthena/

.. image:: https://travis-ci.org/laughingman7743/PyAthena.svg?branch=master
    :target: https://travis-ci.org/laughingman7743/PyAthena

.. image:: https://codecov.io/gh/laughingman7743/PyAthena/branch/master/graph/badge.svg
    :target: https://codecov.io/gh/laughingman7743/PyAthena

.. image:: https://img.shields.io/pypi/l/PyAthena.svg
    :target: https://github.com/laughingman7743/PyAthena/blob/master/LICENSE


PyAthena
========

PyAthena is a Python `DB API 2.0 (PEP 249)`_ compliant client for `Amazon Athena`_.

.. _`DB API 2.0 (PEP 249)`: https://www.python.org/dev/peps/pep-0249/
.. _`Amazon Athena`: http://docs.aws.amazon.com/athena/latest/APIReference/Welcome.html

Requirements
------------

* Python

  - CPython 2,7, 3,4, 3.5, 3.6

Installation
------------

.. code:: bash

    $ pip install PyAthena

Extra packages:

+---------------+--------------------------------------+----------+
| Package       | Install command                      | Version  |
+===============+======================================+==========+
| Pandas        | ``pip install PyAthena[Pandas]``     | >=0.19.0 |
+---------------+--------------------------------------+----------+
| SQLAlchemy    | ``pip install PyAthena[SQLAlchemy]`` | >=1.0.0  |
+---------------+--------------------------------------+----------+

Usage
-----

Basic usage
~~~~~~~~~~~

.. code:: python

    from pyathena import connect

    cursor = connect(aws_access_key_id='YOUR_ACCESS_KEY_ID',
                     aws_secret_access_key='YOUR_SECRET_ACCESS_KEY',
                     s3_staging_dir='s3://YOUR_S3_BUCKET/path/to/',
                     region_name='us-west-2').cursor()
    cursor.execute("SELECT * FROM one_row")
    print(cursor.description)
    print(cursor.fetchall())

Cursor iteration
~~~~~~~~~~~~~~~~

.. code:: python

    from pyathena import connect

    cursor = connect(aws_access_key_id='YOUR_ACCESS_KEY_ID',
                     aws_secret_access_key='YOUR_SECRET_ACCESS_KEY',
                     s3_staging_dir='s3://YOUR_S3_BUCKET/path/to/',
                     region_name='us-west-2').cursor()
    cursor.execute("SELECT * FROM many_rows LIMIT 10")
    for row in cursor:
        print(row)

Query with parameter
~~~~~~~~~~~~~~~~~~~~

Supported `DB API paramstyle`_ is only ``PyFormat``.
``PyFormat`` only supports `named placeholders`_ with old ``%`` operator style and parameters specify dictionary format.

.. code:: python

    from pyathena import connect

    cursor = connect(aws_access_key_id='YOUR_ACCESS_KEY_ID',
                     aws_secret_access_key='YOUR_SECRET_ACCESS_KEY',
                     s3_staging_dir='s3://YOUR_S3_BUCKET/path/to/',
                     region_name='us-west-2').cursor()
    cursor.execute("""
                   SELECT col_string FROM one_row_complex
                   WHERE col_string = %(param)s
                   """, {'param': 'a string'})
    print(cursor.fetchall())

if ``%`` character is contained in your query, it must be escaped with ``%%`` like the following:

.. code:: sql

    SELECT col_string FROM one_row_complex
    WHERE col_string = %(param)s OR col_string LIKE 'a%%'

.. _`DB API paramstyle`: https://www.python.org/dev/peps/pep-0249/#paramstyle
.. _`named placeholders`: https://pyformat.info/#named_placeholders

SQLAlchemy
~~~~~~~~~~

Install SQLAlchemy with ``pip install SQLAlchemy>=1.0.0`` or ``pip install PyAthena[SQLAlchemy]``.
Supported SQLAlchemy is 1.0.0 or higher.

.. code:: python

    from urllib.parse import quote_plus  # PY2: from urllib import quote_plus
    from sqlalchemy.engine import create_engine
    from sqlalchemy.sql.expression import select
    from sqlalchemy.sql.functions import func
    from sqlalchemy.sql.schema import Table, MetaData

    conn_str = 'awsathena+rest://{aws_access_key_id}:{aws_secret_access_key}@athena.{region_name}.amazonaws.com:443/'\
               '{schema_name}?s3_staging_dir={s3_staging_dir}'
    engine = create_engine(conn_str.format(
        aws_access_key_id=quote_plus('YOUR_ACCESS_KEY_ID'),
        aws_secret_access_key=quote_plus('YOUR_SECRET_ACCESS_KEY'),
        region_name='us-west-2',
        schema_name='default',
        s3_staging_dir=quote_plus('s3://YOUR_S3_BUCKET/path/to/')))
    many_rows = Table('many_rows', MetaData(bind=engine), autoload=True)
    print(select([func.count('*')], from_obj=many_rows).scalar())

The connection string has the following format:

.. code:: python

    awsathena+rest://{aws_access_key_id}:{aws_secret_access_key}@athena.{region_name}.amazonaws.com:443/{schema_name}?s3_staging_dir={s3_staging_dir}&...

NOTE: ``s3_staging_dir`` requires quote. If ``aws_access_key_id``, ``aws_secret_access_key`` and other parameter contain special characters, quote is also required.

Pandas
~~~~~~

Minimal example for Pandas DataFrame:

.. code:: python

    from pyathena import connect
    import pandas as pd

    conn = connect(aws_access_key_id='YOUR_ACCESS_KEY_ID',
                   aws_secret_access_key='YOUR_SECRET_ACCESS_KEY',
                   s3_staging_dir='s3://YOUR_S3_BUCKET/path/to/',
                   region_name='us-west-2')
    df = pd.read_sql("SELECT * FROM many_rows", conn)
    print(df.head())

As Pandas DataFrame:

.. code:: python

    from pyathena import connect
    from pyathena.util import as_pandas

    cursor = connect(aws_access_key_id='YOUR_ACCESS_KEY_ID',
                     aws_secret_access_key='YOUR_SECRET_ACCESS_KEY',
                     s3_staging_dir='s3://YOUR_S3_BUCKET/path/to/',
                     region_name='us-west-2').cursor()
    cursor.execute("SELECT * FROM many_rows")
    df = as_pandas(cursor)
    print(df.describe())

Asynchronous Cursor
~~~~~~~~~~~~~~~~~~~

Asynchronous cursor is a simple implementation using the concurrent.futures package.
Python 2.7 uses `backport of the concurrent.futures`_ package.
This cursor is not `DB API 2.0 (PEP 249)`_ compliant.

You can use the asynchronous cursor by specifying the ``cursor_class``
with the connect method or connection object.

.. code:: python

    from pyathena import connect
    from pyathena.async_cursor import AsyncCursor

    cursor = connect(s3_staging_dir='s3://YOUR_S3_BUCKET/path/to/',
                     region_name='us-west-2',
                     cursor_class=AsyncCursor).cursor()

.. code:: python

    from pyathena.connection import Connection
    from pyathena.async_cursor import AsyncCursor

    cursor = Connection(s3_staging_dir='s3://YOUR_S3_BUCKET/path/to/',
                        region_name='us-west-2',
                        cursor_class=AsyncCursor).cursor()

It can also be used by specifying the cursor class when calling the connection object's cursor method.

.. code:: python

    from pyathena import connect
    from pyathena.async_cursor import AsyncCursor

    cursor = connect(s3_staging_dir='s3://YOUR_S3_BUCKET/path/to/',
                     region_name='us-west-2').cursor(AsyncCursor)

.. code:: python

    from pyathena.connection import Connection
    from pyathena.async_cursor import AsyncCursor

    cursor = Connection(s3_staging_dir='s3://YOUR_S3_BUCKET/path/to/',
                        region_name='us-west-2').cursor(AsyncCursor)

The default number of workers is 5 or cpu number * 5.
If you want to change the number of workers you can specify like the following.

.. code:: python

    from pyathena import connect
    from pyathena.async_cursor import AsyncCursor

    cursor = connect(s3_staging_dir='s3://YOUR_S3_BUCKET/path/to/',
                     region_name='us-west-2',
                     cursor_class=AsyncCursor).cursor(max_workers=10)

The execute method of the asynchronous cursor returns the tuple of the query ID and the `future object`_.

.. code:: python

    from pyathena import connect
    from pyathena.async_cursor import AsyncCursor

    cursor = connect(s3_staging_dir='s3://YOUR_S3_BUCKET/path/to/',
                     region_name='us-west-2',
                     cursor_class=AsyncCursor).cursor()

    query_id, future = cursor.execute("SELECT * FROM many_rows")

The return value of the `future object`_ is an ``AthenaResultSet`` object.
This object has an interface that can fetch and iterate query results similar to synchronous cursors.
It also has information on the result of query execution.

.. code:: python

    from pyathena import connect
    from pyathena.async_cursor import AsyncCursor

    cursor = connect(s3_staging_dir='s3://YOUR_S3_BUCKET/path/to/',
                     region_name='us-west-2',
                     cursor_class=AsyncCursor).cursor()

    query_id, future = cursor.execute("SELECT * FROM many_rows")
    result_set = future.result()
    print(result_set.state)
    print(result_set.state_change_reason)
    print(result_set.completion_date_time)
    print(result_set.submission_date_time)
    print(result_set.data_scanned_in_bytes)
    print(result_set.execution_time_in_millis)
    print(result_set.output_location)
    print(result_set.description)
    for row in result_set:
        print(row)

.. code:: python

    from pyathena import connect
    from pyathena.async_cursor import AsyncCursor

    cursor = connect(s3_staging_dir='s3://YOUR_S3_BUCKET/path/to/',
                     region_name='us-west-2',
                     cursor_class=AsyncCursor).cursor()

    query_id, future = cursor.execute("SELECT * FROM many_rows")
    result_set = future.result()
    print(result_set.fetchall())

A query ID is required to cancel a query with the asynchronous cursor.

.. code:: python

    from pyathena import connect
    from pyathena.async_cursor import AsyncCursor

    cursor = connect(s3_staging_dir='s3://YOUR_S3_BUCKET/path/to/',
                     region_name='us-west-2',
                     cursor_class=AsyncCursor).cursor()

    query_id, future = cursor.execute("SELECT * FROM many_rows")
    cursor.cancel(query_id)

NOTE: The cancel method of the `future object`_ does not cancel the query.

.. _`backport of the concurrent.futures`: https://pypi.python.org/pypi/futures
.. _`future object`: https://docs.python.org/3/library/concurrent.futures.html#future-objects

Credentials
-----------

Support `Boto3 credentials`_.

.. _`Boto3 credentials`: http://boto3.readthedocs.io/en/latest/guide/configuration.html

Additional environment variable:

.. code:: bash

    $ export AWS_ATHENA_S3_STAGING_DIR=s3://YOUR_S3_BUCKET/path/to/

Testing
-------

Depends on the following environment variables:

.. code:: bash

    $ export AWS_ACCESS_KEY_ID=YOUR_ACCESS_KEY_ID
    $ export AWS_SECRET_ACCESS_KEY=YOUR_SECRET_ACCESS_KEY
    $ export AWS_DEFAULT_REGION=us-west-2
    $ export AWS_ATHENA_S3_STAGING_DIR=s3://YOUR_S3_BUCKET/path/to/

Run test
~~~~~~~~

.. code:: bash

    $ pip install pipenv
    $ pipenv install --dev
    $ pipenv run scripts/test_data/upload_test_data.sh
    $ pipenv run pytest
    $ pipenv run scripts/test_data/delete_test_data.sh

Run test multiple Python versions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code:: bash

    $ pip install pipenv
    $ pipenv install --dev
    $ pipenv run scripts/test_data/upload_test_data.sh
    $ pyenv local 3.6.5 3.5.5 3.4.8 2.7.14
    $ pipenv run tox
    $ pipenv run scripts/test_data/delete_test_data.sh
