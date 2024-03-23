.. _usage:

Usage
=====

Basic usage
-----------

.. code:: python

    from pyathena import connect

    cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                     region_name="us-west-2").cursor()
    cursor.execute("SELECT * FROM one_row")
    print(cursor.description)
    print(cursor.fetchall())

Cursor iteration
----------------

.. code:: python

    from pyathena import connect

    cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                     region_name="us-west-2").cursor()
    cursor.execute("SELECT * FROM many_rows LIMIT 10")
    for row in cursor:
        print(row)

Query with parameters
---------------------

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

Quickly re-run queries
----------------------

Result reuse configuration
~~~~~~~~~~~~~~~~~~~~~~~~~~

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

Cache configuration
~~~~~~~~~~~~~~~~~~~

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

Environment variables
---------------------

Support `Boto3 environment variables`_.

Additional environment variables
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

AWS_ATHENA_S3_STAGING_DIR
    The S3 location where Athena automatically stores the query results and metadata information. Required if you have not set up workgroups. Not required if a workgroup has been set up.

AWS_ATHENA_WORK_GROUP
    The setting of the workgroup to execute the query.

Credentials
-----------

Support `Boto3 credentials`_.

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

.. _`DB API paramstyle`: https://www.python.org/dev/peps/pep-0249/#paramstyle
.. _`named placeholders`: https://pyformat.info/#named_placeholders
.. _`reuse the results of previous queries`: https://docs.aws.amazon.com/athena/latest/ug/reusing-query-results.html
.. _`Boto3 environment variables`: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html#using-environment-variables
.. _`Boto3 credentials`: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html
