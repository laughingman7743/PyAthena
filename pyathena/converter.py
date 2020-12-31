# -*- coding: utf-8 -*-
import binascii
import json
import logging
from abc import ABCMeta, abstractmethod
from copy import deepcopy
from datetime import date, datetime, time
from decimal import Decimal
from distutils.util import strtobool
from typing import Any, Callable, Dict, Optional, Type

_logger = logging.getLogger(__name__)  # type: ignore


class Converter(object, metaclass=ABCMeta):
    def __init__(
        self,
        mappings: Dict[str, Callable[[Optional[str]], Optional[Any]]],
        default: Callable[[Optional[str]], Optional[Any]] = None,
        types: Dict[str, Type[Any]] = None,
    ) -> None:
        if mappings:
            self._mappings = mappings
        else:
            self._mappings = dict()
        self._default = default
        if types:
            self._types = types
        else:
            self._types = dict()

    @property
    def mappings(self) -> Dict[str, Callable[[Optional[str]], Optional[Any]]]:
        return self._mappings

    @property
    def types(self) -> Dict[str, Type[Any]]:
        return self._types

    def get(self, type_: str) -> Optional[Callable[[Optional[str]], Optional[Any]]]:
        return self.mappings.get(type_, self._default)

    def set(
        self, type_: str, converter: Callable[[Optional[str]], Optional[Any]]
    ) -> None:
        self.mappings[type_] = converter

    def remove(self, type_: str) -> None:
        self.mappings.pop(type_, None)

    def update(
        self, mappings: Dict[str, Callable[[Optional[str]], Optional[Any]]]
    ) -> None:
        self.mappings.update(mappings)

    @abstractmethod
    def convert(self, type_: str, value: Optional[str]) -> Optional[Any]:
        raise NotImplementedError  # pragma: no cover


def _to_date(varchar_value: Optional[str]) -> Optional[date]:
    if varchar_value is None:
        return None
    return datetime.strptime(varchar_value, "%Y-%m-%d").date()


def _to_datetime(varchar_value: Optional[str]) -> Optional[datetime]:
    if varchar_value is None:
        return None
    return datetime.strptime(varchar_value, "%Y-%m-%d %H:%M:%S.%f")


def _to_time(varchar_value: Optional[str]) -> Optional[time]:
    if varchar_value is None:
        return None
    return datetime.strptime(varchar_value, "%H:%M:%S.%f").time()


def _to_float(varchar_value: Optional[str]) -> Optional[float]:
    if varchar_value is None:
        return None
    return float(varchar_value)


def _to_int(varchar_value: Optional[str]) -> Optional[int]:
    if varchar_value is None:
        return None
    return int(varchar_value)


def _to_decimal(varchar_value: Optional[str]) -> Optional[Decimal]:
    if varchar_value is None or varchar_value == "":
        return None
    return Decimal(varchar_value)


def _to_boolean(varchar_value: Optional[str]) -> Optional[bool]:
    if varchar_value is None or varchar_value == "":
        return None
    return bool(strtobool(varchar_value))


def _to_binary(varchar_value: Optional[str]) -> Optional[bytes]:
    if varchar_value is None:
        return None
    return binascii.a2b_hex("".join(varchar_value.split(" ")))


def _to_json(varchar_value: Optional[str]) -> Optional[Any]:
    if varchar_value is None:
        return None
    return json.loads(varchar_value)


def _to_default(varchar_value: Optional[str]) -> Optional[str]:
    return varchar_value


_DEFAULT_CONVERTERS: Dict[str, Callable[[Optional[str]], Optional[Any]]] = {
    "boolean": _to_boolean,
    "tinyint": _to_int,
    "smallint": _to_int,
    "integer": _to_int,
    "bigint": _to_int,
    "float": _to_float,
    "real": _to_float,
    "double": _to_float,
    "char": _to_default,
    "varchar": _to_default,
    "string": _to_default,
    "timestamp": _to_datetime,
    "date": _to_date,
    "time": _to_time,
    "varbinary": _to_binary,
    "array": _to_default,
    "map": _to_default,
    "row": _to_default,
    "decimal": _to_decimal,
    "json": _to_json,
}
_DEFAULT_PANDAS_CONVERTERS: Dict[str, Callable[[Optional[str]], Optional[Any]]] = {
    "boolean": _to_boolean,
    "decimal": _to_decimal,
    "varbinary": _to_binary,
    "json": _to_json,
}


class DefaultTypeConverter(Converter):
    def __init__(self) -> None:
        super(DefaultTypeConverter, self).__init__(
            mappings=deepcopy(_DEFAULT_CONVERTERS), default=_to_default
        )

    def convert(self, type_: str, value: Optional[str]) -> Optional[Any]:
        converter = self.get(type_)
        if converter:
            return converter(value)
        return value


class DefaultPandasTypeConverter(Converter):
    def __init__(self) -> None:
        super(DefaultPandasTypeConverter, self).__init__(
            mappings=deepcopy(_DEFAULT_PANDAS_CONVERTERS),
            default=_to_default,
            types=self._dtypes,
        )

    @property
    def _dtypes(self) -> Dict[str, Type[Any]]:
        if not hasattr(self, "__dtypes"):
            import pandas as pd

            self.__dtypes = {
                "tinyint": pd.Int64Dtype(),
                "smallint": pd.Int64Dtype(),
                "integer": pd.Int64Dtype(),
                "bigint": pd.Int64Dtype(),
                "float": float,
                "real": float,
                "double": float,
                "char": str,
                "varchar": str,
                "string": str,
                "array": str,
                "map": str,
                "row": str,
            }
        return self.__dtypes

    def convert(self, type_: str, value: Optional[str]) -> Optional[Any]:
        pass
