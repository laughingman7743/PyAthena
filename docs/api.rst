.. _api:

API Reference
=============

This section provides detailed API documentation for all PyAthena classes and functions.

Core Components
---------------

Connection and Cursors
~~~~~~~~~~~~~~~~~~~~~~

.. automodule:: pyathena
   :members: connect

.. autoclass:: pyathena.connection.Connection
   :members:
   :inherited-members:

.. autoclass:: pyathena.cursor.Cursor
   :members:
   :inherited-members:

.. autoclass:: pyathena.cursor.DictCursor
   :members:
   :inherited-members:

Asynchronous Operations
~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: pyathena.async_cursor.AsyncCursor
   :members:
   :inherited-members:

.. autoclass:: pyathena.async_cursor.AsyncDictCursor
   :members:
   :inherited-members:

Pandas Integration
------------------

.. autoclass:: pyathena.pandas.cursor.PandasCursor
   :members:
   :inherited-members:

.. autoclass:: pyathena.pandas.async_cursor.AsyncPandasCursor
   :members:
   :inherited-members:

Apache Arrow Integration
------------------------

.. autoclass:: pyathena.arrow.cursor.ArrowCursor
   :members:
   :inherited-members:

.. autoclass:: pyathena.arrow.async_cursor.AsyncArrowCursor
   :members:
   :inherited-members:

Spark Integration
-----------------

.. autoclass:: pyathena.spark.cursor.SparkCursor
   :members:
   :inherited-members:

.. autoclass:: pyathena.spark.async_cursor.AsyncSparkCursor
   :members:
   :inherited-members:

Data Conversion
---------------

Type Converters
~~~~~~~~~~~~~~~

.. autoclass:: pyathena.converter.Converter
   :members:

.. autoclass:: pyathena.converter.DefaultTypeConverter
   :members:

.. autoclass:: pyathena.pandas.converter.DefaultPandasTypeConverter
   :members:

.. autoclass:: pyathena.arrow.converter.DefaultArrowTypeConverter
   :members:

Parameter Formatting
~~~~~~~~~~~~~~~~~~~~

.. autoclass:: pyathena.formatter.Formatter
   :members:

.. autoclass:: pyathena.formatter.DefaultParameterFormatter
   :members:

Utilities and Configuration
---------------------------

Retry Configuration
~~~~~~~~~~~~~~~~~~~

.. autoclass:: pyathena.util.RetryConfig
   :members:

.. autofunction:: pyathena.util.retry_api_call

.. autofunction:: pyathena.util.parse_output_location

File System Integration
~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: pyathena.filesystem.s3.S3FileSystem
   :members:

.. autoclass:: pyathena.filesystem.s3_object.S3Object
   :members:

Data Models
-----------

Query Execution
~~~~~~~~~~~~~~~

.. autoclass:: pyathena.model.AthenaQueryExecution
   :members:

.. autoclass:: pyathena.model.AthenaCalculationExecution
   :members:

File Formats and Compression
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: pyathena.model.AthenaFileFormat
   :members:

.. autoclass:: pyathena.model.AthenaCompression
   :members:

Exception Handling
------------------

.. automodule:: pyathena.error
   :members:
   :show-inheritance: