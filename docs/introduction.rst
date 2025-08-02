.. _introduction:

Introduction
============

.. _requirements:

Requirements
------------

* Python

  - CPython 3.9 3.10, 3.11 3.12 3.13

.. _installation:

Installation
------------

.. code:: bash

    $ pip install PyAthena

Extra packages:

+---------------+---------------------------------------+------------------+
| Package       | Install command                       | Version          |
+===============+=======================================+==================+
| SQLAlchemy    | ``pip install PyAthena[SQLAlchemy]``  | >=1.0.0          |
+---------------+---------------------------------------+------------------+
| Pandas        | ``pip install PyAthena[Pandas]``      | >=1.3.0          |
+---------------+---------------------------------------+------------------+
| Arrow         | ``pip install PyAthena[Arrow]``       | >=7.0.0          |
+---------------+---------------------------------------+------------------+
| fastparquet   | ``pip install PyAthena[fastparquet]`` | >=0.4.0          |
+---------------+---------------------------------------+------------------+

.. _features:

Features
--------

PyAthena provides comprehensive support for Amazon Athena's data types and features:

**Core Features:**
  - **DB API 2.0 Compliance**: Full PEP 249 compatibility for database operations
  - **SQLAlchemy Integration**: Native dialect support with table reflection and ORM capabilities
  - **Multiple Cursor Types**: Standard, Pandas, Arrow, and Spark cursor implementations
  - **Async Support**: Asynchronous query execution for non-blocking operations

**Data Type Support:**
  - **STRUCT/ROW Types**: :ref:`Complete support <sqlalchemy>` for complex nested data structures
  - **ARRAY Types**: Native handling of array data with automatic Python list conversion
  - **MAP Types**: :ref:`Complete support <sqlalchemy>` for key-value dictionary-like data structures
  - **JSON Integration**: Seamless JSON data parsing and conversion
  - **Performance Optimized**: Smart format detection for efficient data processing

**Additional Features:**
  - **Connection Management**: Efficient connection pooling and configuration
  - **Result Caching**: Athena query result reuse capabilities
  - **Error Handling**: Comprehensive exception handling and recovery
  - **S3 Integration**: Direct S3 data access and staging support

.. _license:

License
-------

`MIT license`_

Many of the implementations in this library are based on `PyHive`_, thanks for `PyHive`_.

.. _`MIT license`: https://github.com/laughingman7743/PyAthena/blob/master/LICENSE
.. _`PyHive`: https://github.com/dropbox/PyHive
