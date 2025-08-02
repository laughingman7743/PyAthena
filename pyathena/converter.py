# -*- coding: utf-8 -*-
from __future__ import annotations

import binascii
import json
import logging
from abc import ABCMeta, abstractmethod
from copy import deepcopy
from datetime import date, datetime, time
from decimal import Decimal
from typing import Any, Callable, Dict, Optional, Type

from dateutil.tz import gettz

from pyathena.util import strtobool

_logger = logging.getLogger(__name__)  # type: ignore


def _to_date(varchar_value: Optional[str]) -> Optional[date]:
    if varchar_value is None:
        return None
    return datetime.strptime(varchar_value, "%Y-%m-%d").date()


def _to_datetime(varchar_value: Optional[str]) -> Optional[datetime]:
    if varchar_value is None:
        return None
    return datetime.strptime(varchar_value, "%Y-%m-%d %H:%M:%S.%f")


def _to_datetime_with_tz(varchar_value: Optional[str]) -> Optional[datetime]:
    if varchar_value is None:
        return None
    datetime_, _, tz = varchar_value.rpartition(" ")
    return datetime.strptime(datetime_, "%Y-%m-%d %H:%M:%S.%f").replace(tzinfo=gettz(tz))


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
    if not varchar_value:
        return None
    return Decimal(varchar_value)


def _to_boolean(varchar_value: Optional[str]) -> Optional[bool]:
    if not varchar_value:
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


def _to_struct(varchar_value: Optional[str]) -> Optional[Dict[str, Any]]:
    """Convert struct data to Python dictionary.

    Supports two formats:
    1. JSON format: '{"key": "value", "num": 123}' (recommended)
    2. Athena native format: '{key=value, num=123}' (basic cases only)

    For complex structs, use CAST(struct_column AS JSON) in your SQL query.

    Args:
        varchar_value: String representation of struct data

    Returns:
        Dictionary representation of struct, or None if parsing fails
    """
    if varchar_value is None:
        return None

    # Quick check: if it doesn't look like a struct, return None
    if not (varchar_value.startswith("{") and varchar_value.endswith("}")):
        return None

    # Optimize: Check if it looks like JSON vs Athena native format
    # JSON objects typically have quoted keys: {"key": value}
    # Athena native format has unquoted keys: {key=value}
    inner_preview = varchar_value[1:10] if len(varchar_value) > 10 else varchar_value[1:-1]

    if '"' in inner_preview or varchar_value.startswith('{"'):
        # Likely JSON format - try JSON parsing
        try:
            result = json.loads(varchar_value)
            return result if isinstance(result, dict) else None
        except json.JSONDecodeError:
            # If JSON parsing fails, fall back to native format parsing
            pass

    inner = varchar_value[1:-1].strip()
    if not inner:
        return {}

    try:
        if "=" in inner:
            # Named struct: {a=1, b=2}
            return _parse_named_struct(inner)
        # Unnamed struct: {Alice, 25}
        return _parse_unnamed_struct(inner)
    except Exception:
        return None


def _parse_named_struct(inner: str) -> Optional[Dict[str, Any]]:
    """Parse named struct format: a=1, b=2.

    Args:
        inner: Interior content of struct without braces.

    Returns:
        Dictionary with parsed key-value pairs, or None if no valid pairs found.
    """
    result = {}

    # Simple split by comma for basic cases
    pairs = [pair.strip() for pair in inner.split(",")]

    for pair in pairs:
        if "=" not in pair:
            continue

        key, value = pair.split("=", 1)
        key = key.strip()
        value = value.strip()

        # Skip pairs with special characters (safety check)
        if any(char in key for char in '{}="') or any(char in value for char in '{}="'):
            continue

        # Convert value to appropriate type
        result[key] = _convert_value(value)

    return result if result else None


def _parse_unnamed_struct(inner: str) -> Dict[str, Any]:
    """Parse unnamed struct format: Alice, 25.

    Args:
        inner: Interior content of struct without braces.

    Returns:
        Dictionary with indexed keys mapping to parsed values.
    """
    values = [v.strip() for v in inner.split(",")]
    return {str(i): _convert_value(value) for i, value in enumerate(values)}


def _convert_value(value: str) -> Any:
    """Convert string value to appropriate Python type.

    Args:
        value: String value to convert.

    Returns:
        Converted value as int, float, bool, None, or string.
    """
    if value.lower() == "null":
        return None
    if value.lower() == "true":
        return True
    if value.lower() == "false":
        return False
    if value.isdigit() or value.startswith("-") and value[1:].isdigit():
        return int(value)
    if "." in value and value.replace(".", "", 1).replace("-", "", 1).isdigit():
        return float(value)
    return value


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
    "timestamp with time zone": _to_datetime_with_tz,
    "date": _to_date,
    "time": _to_time,
    "varbinary": _to_binary,
    "array": _to_default,
    "map": _to_default,
    "row": _to_struct,
    "decimal": _to_decimal,
    "json": _to_json,
}


class Converter(metaclass=ABCMeta):
    def __init__(
        self,
        mappings: Dict[str, Callable[[Optional[str]], Optional[Any]]],
        default: Callable[[Optional[str]], Optional[Any]] = _to_default,
        types: Optional[Dict[str, Type[Any]]] = None,
    ) -> None:
        if mappings:
            self._mappings = mappings
        else:
            self._mappings = {}
        self._default = default
        if types:
            self._types = types
        else:
            self._types = {}

    @property
    def mappings(self) -> Dict[str, Callable[[Optional[str]], Optional[Any]]]:
        return self._mappings

    @property
    def types(self) -> Dict[str, Type[Any]]:
        return self._types

    def get(self, type_: str) -> Callable[[Optional[str]], Optional[Any]]:
        return self.mappings.get(type_, self._default)

    def set(self, type_: str, converter: Callable[[Optional[str]], Optional[Any]]) -> None:
        self.mappings[type_] = converter

    def remove(self, type_: str) -> None:
        self.mappings.pop(type_, None)

    def update(self, mappings: Dict[str, Callable[[Optional[str]], Optional[Any]]]) -> None:
        self.mappings.update(mappings)

    @abstractmethod
    def convert(self, type_: str, value: Optional[str]) -> Optional[Any]:
        raise NotImplementedError  # pragma: no cover


class DefaultTypeConverter(Converter):
    def __init__(self) -> None:
        super().__init__(mappings=deepcopy(_DEFAULT_CONVERTERS), default=_to_default)

    def convert(self, type_: str, value: Optional[str]) -> Optional[Any]:
        converter = self.get(type_)
        return converter(value)
