# -*- coding: utf-8 -*-
from sqlalchemy.dialects import registry

registry.register("awsathena.rest", "pyathena.sqlalchemy_athena", "AthenaDialect")
