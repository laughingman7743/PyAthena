.. image:: https://badge.fury.io/py/pyathena.svg
    :target: https://badge.fury.io/py/pyathena

.. image:: https://img.shields.io/pypi/pyversions/PyAthena.svg
    :target: https://pypi.org/project/PyAthena/

.. image:: https://github.com/laughingman7743/PyAthena/actions/workflows/test.yaml/badge.svg
    :target: https://github.com/laughingman7743/PyAthena/actions/workflows/test.yaml

.. image:: https://img.shields.io/pypi/l/PyAthena.svg
    :target: https://github.com/laughingman7743/PyAthena/blob/master/LICENSE

.. image:: https://static.pepy.tech/badge/pyathena/month
    :target: https://pepy.tech/project/pyathena

.. image:: https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json
    :target: https://github.com/astral-sh/ruff
    :alt: Ruff

.. image:: https://www.mypy-lang.org/static/mypy_badge.svg
    :target: https://mypy-lang.org/
    :alt: mypy

PyAthena
========

PyAthena is a Python `DB API 2.0 (PEP 249)`_ client for `Amazon Athena`_.

.. _`DB API 2.0 (PEP 249)`: https://www.python.org/dev/peps/pep-0249/
.. _`Amazon Athena`: https://docs.aws.amazon.com/athena/latest/APIReference/Welcome.html

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

.. _usage:

Usage
-----

.. code:: python

    from pyathena import connect

    cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                     region_name="us-west-2").cursor()
    cursor.execute("SELECT * FROM one_row")
    print(cursor.description)
    print(cursor.fetchall())

.. _license:

License
-------

`MIT license`_

Many of the implementations in this library are based on `PyHive`_, thanks for `PyHive`_.

.. _`MIT license`: LICENSE
.. _`PyHive`: https://github.com/dropbox/PyHive

Links
-----

- Documentation: https://laughingman7743.github.io/PyAthena/
- PyPI Releases: https://pypi.org/project/PyAthena/
- Source Code: https://github.com/laughingman7743/PyAthena/
- Issue Tracker: https://github.com/laughingman7743/PyAthena/issues
