.. _api_s3fs:

S3FS Integration
================

This section covers lightweight S3FS-based cursors and data converters that use Python's built-in ``csv`` module.

S3FS Cursors
------------

.. autoclass:: pyathena.s3fs.cursor.S3FSCursor
   :members:
   :inherited-members:

.. autoclass:: pyathena.s3fs.async_cursor.AsyncS3FSCursor
   :members:
   :inherited-members:

S3FS Data Converters
--------------------

.. autoclass:: pyathena.s3fs.converter.DefaultS3FSTypeConverter
   :members:

S3FS Result Set
---------------

.. autoclass:: pyathena.s3fs.result_set.AthenaS3FSResultSet
   :members:
   :inherited-members:
