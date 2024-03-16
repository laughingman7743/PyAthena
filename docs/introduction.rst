.. _introduction:

Introduction
============

.. _requirements:

Requirements
------------

* Python

  - CPython 3.8 3.9 3.10, 3.11 3.12

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

.. _license:

License
-------

`MIT license`_

Many of the implementations in this library are based on `PyHive`_, thanks for `PyHive`_.

.. _`MIT license`: https://github.com/laughingman7743/PyAthena/blob/master/LICENSE
.. _`PyHive`: https://github.com/dropbox/PyHive
