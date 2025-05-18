# -*- coding: utf-8 -*-
from typing import TYPE_CHECKING

from pyathena.sqlalchemy.base import AthenaDialect

if TYPE_CHECKING:
    from types import ModuleType


class AthenaRestDialect(AthenaDialect):
    driver = "rest"
    supports_statement_cache = True

    @classmethod
    def import_dbapi(cls) -> "ModuleType":
        return super().import_dbapi()
