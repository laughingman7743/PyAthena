# -*- coding: utf-8 -*-
import logging
from copy import deepcopy
from typing import Any, Callable, Dict, Optional, Type

from pyathena.converter import (
    Converter,
    _to_binary,
    _to_datetime,
    _to_decimal,
    _to_default,
    _to_json,
)

_logger = logging.getLogger(__name__)  # type: ignore


_DEFAULT_ARROW_CONVERTERS: Dict[str, Callable[[Optional[str]], Optional[Any]]] = {
    # TODO pyarrow.lib.ArrowInvalid:
    # In CSV column #1: CSV conversion error to timestamp[ms]:
    # invalid value '2022-01-01 11:22:33.123'
    "timestamp": _to_datetime,
    "decimal": _to_decimal,
    "varbinary": _to_binary,
    "json": _to_json,
}


class DefaultArrowTypeConverter(Converter):
    def __init__(self) -> None:
        super(DefaultArrowTypeConverter, self).__init__(
            mappings=deepcopy(_DEFAULT_ARROW_CONVERTERS),
            default=_to_default,
            types=self._dtypes,
        )

    @property
    def _dtypes(self) -> Dict[str, Type[Any]]:
        if not hasattr(self, "__dtypes"):
            import pyarrow as pa

            self.__dtypes = {
                "boolean": pa.bool_(),
                "tinyint": pa.int64(),
                "smallint": pa.int64(),
                "integer": pa.int64(),
                "bigint": pa.int64(),
                "float": pa.float64(),
                "real": pa.float64(),
                "double": pa.float64(),
                "char": pa.string(),
                "varchar": pa.string(),
                "string": pa.string(),
                "timestamp": pa.string(),
                # "timestamp": pa.timestamp("ms"),
                "date": pa.string(),
                "time": pa.string(),
                "varbinary": pa.string(),
                "array": pa.string(),
                "map": pa.string(),
                "row": pa.string(),
                "decimal": pa.string(),
                "json": pa.string(),
            }
        return self.__dtypes

    def convert(self, type_: str, value: Optional[str]) -> Optional[Any]:
        converter = self.get(type_)
        return converter(value)
