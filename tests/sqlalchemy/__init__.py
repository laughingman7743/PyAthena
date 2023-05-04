# -*- coding: utf-8 -*-
from sqlalchemy.dialects import registry

registry.register("awsathena", "pyathena.sqlalchemy.base", "AthenaDialect")
registry.register("awsathena.rest", "pyathena.sqlalchemy.rest", "AthenaRestDialect")
registry.register("awsathena.pandas", "pyathena.sqlalchemy.pandas", "AthenaPandasDialect")
registry.register("awsathena.arrow", "pyathena.sqlalchemy.arrow", "AthenaArrowDialect")
