# -*- coding: utf-8 -*-
from typing import TYPE_CHECKING

from pyathena.sqlalchemy.base import AthenaDialect
from pyathena.util import strtobool

if TYPE_CHECKING:
    from types import ModuleType


class AthenaPandasDialect(AthenaDialect):
    driver = "pandas"
    supports_statement_cache = True

    def create_connect_args(self, url):
        from pyathena.pandas.cursor import PandasCursor

        opts = super()._create_connect_args(url)
        opts.update({"cursor_class": PandasCursor})
        cursor_kwargs = {}
        if "unload" in opts:
            cursor_kwargs.update({"unload": bool(strtobool(opts.pop("unload")))})
        if "engine" in opts:
            cursor_kwargs.update({"engine": opts.pop("engine")})
        if "chunksize" in opts:
            cursor_kwargs.update({"chunksize": int(opts.pop("chunksize"))})  # type: ignore
        if cursor_kwargs:
            opts.update({"cursor_kwargs": cursor_kwargs})
        return [[], opts]

    @classmethod
    def import_dbapi(cls) -> "ModuleType":
        return super().import_dbapi()
