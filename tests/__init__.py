# -*- coding: utf-8 -*-
from future.utils import PY26
from sqlalchemy.dialects import registry

if PY26:
    import unittest2 as unittest
else:
    import unittest  # noqa: F401


registry.register("awsathena.rest", "pyathena.sqlalchemy_athena", "AthenaDialect")
