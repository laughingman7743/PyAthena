# -*- coding: utf-8 -*-
from __future__ import absolute_import, unicode_literals

import binascii
import json
import logging
from abc import ABCMeta, abstractmethod
from copy import deepcopy
from datetime import datetime
from decimal import Decimal
from distutils.util import strtobool

from future.utils import with_metaclass

_logger = logging.getLogger(__name__)


def _to_date(varchar_value):
    if varchar_value is None:
        return None
    return datetime.strptime(varchar_value, "%Y-%m-%d").date()


def _to_datetime(varchar_value):
    if varchar_value is None:
        return None
    return datetime.strptime(varchar_value, "%Y-%m-%d %H:%M:%S.%f")


def _to_time(varchar_value):
    if varchar_value is None:
        return None
    return datetime.strptime(varchar_value, "%H:%M:%S.%f").time()


def _to_float(varchar_value):
    if varchar_value is None:
        return None
    return float(varchar_value)


def _to_int(varchar_value):
    if varchar_value is None:
        return None
    return int(varchar_value)


def _to_decimal(varchar_value):
    if varchar_value is None:
        return None
    return Decimal(varchar_value)


def _to_boolean(varchar_value):
    if varchar_value is None or varchar_value == "":
        return None
    return bool(strtobool(varchar_value))


def _to_binary(varchar_value):
    if varchar_value is None:
        return None
    return binascii.a2b_hex("".join(varchar_value.split(" ")))


def _to_json(varchar_value):
    if varchar_value is None:
        return None
    return json.loads(varchar_value)


def _to_default(varchar_value):
    if varchar_value is None:
        return None
    else:
        return varchar_value


_DEFAULT_CONVERTERS = {
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
_DEFAULT_PANDAS_CONVERTERS = {
    "boolean": _to_boolean,
    "decimal": _to_decimal,
    "varbinary": _to_binary,
    "json": _to_json,
}


class Converter(with_metaclass(ABCMeta, object)):
    def __init__(self, mappings, default=None, types=None):
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
    def mappings(self):
        return self._mappings

    @property
    def types(self):
        return self._types

    def get(self, type_):
        return self.mappings.get(type_, self._default)

    def set(self, type_, converter):
        self.mappings[type_] = converter

    def remove(self, type_):
        self.mappings.pop(type_, None)

    def update(self, mappings):
        self.mappings.update(mappings)

    @abstractmethod
    def convert(self, type_, value):
        raise NotImplementedError  # pragma: no cover


class DefaultTypeConverter(Converter):
    def __init__(self):
        super(DefaultTypeConverter, self).__init__(
            mappings=deepcopy(_DEFAULT_CONVERTERS), default=_to_default
        )

    def convert(self, type_, value):
        converter = self.get(type_)
        return converter(value)


class DefaultPandasTypeConverter(Converter):
    def __init__(self):
        super(DefaultPandasTypeConverter, self).__init__(
            mappings=deepcopy(_DEFAULT_PANDAS_CONVERTERS),
            default=_to_default,
            types=self._dtypes,
        )

    @property
    def _dtypes(self):
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

    def convert(self, type_, value):
        pass
