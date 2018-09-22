# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import binascii
import json
import logging
from datetime import datetime
from decimal import Decimal

_logger = logging.getLogger(__name__)


def _to_date(varchar_value):
    if varchar_value is None:
        return None
    return datetime.strptime(varchar_value, '%Y-%m-%d').date()


def _to_datetime(varchar_value):
    if varchar_value is None:
        return None
    return datetime.strptime(varchar_value, '%Y-%m-%d %H:%M:%S.%f')


def _to_time(varchar_value):
    if varchar_value is None:
        return None
    return datetime.strptime(varchar_value, '%H:%M:%S.%f').time()


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
    if varchar_value is None:
        return None
    elif varchar_value.lower() == 'true':
        return True
    elif varchar_value.lower() == 'false':
        return False
    else:
        return None


def _to_binary(varchar_value):
    if varchar_value is None:
        return None
    return binascii.a2b_hex(''.join(varchar_value.split(' ')))


def _to_json(varchar_value):
    if varchar_value is None:
        return None
    return json.loads(varchar_value)


def _to_default(varchar_value):
    if varchar_value is None:
        return None
    else:
        return varchar_value


class TypeConverter(object):

    def __init__(self):
        self._mappings = _DEFAULT_CONVERTERS

    def convert(self, type_, varchar_value):
        converter = self._mappings.get(type_, _to_default)
        return converter(varchar_value)

    def register_converter(self, type_, converter):
        self._mappings[type_] = converter


_DEFAULT_CONVERTERS = {
    'boolean': _to_boolean,
    'tinyint': _to_int,
    'smallint': _to_int,
    'integer': _to_int,
    'bigint': _to_int,
    'float': _to_float,
    'real': _to_float,
    'double': _to_float,
    'char': _to_default,
    'varchar': _to_default,
    'timestamp': _to_datetime,
    'date': _to_date,
    'time': _to_time,
    'varbinary': _to_binary,
    'array': _to_default,
    'map': _to_default,
    'row': _to_default,
    'decimal': _to_decimal,
    'json': _to_json,
}

PANDAS_DTYPES = {
    'boolean': bool,
    'tinyint': int,
    'smallint': int,
    'integer': int,
    'bigint': int,
    'float': float,
    'real': float,
    'double': float,
    'char': str,
    'varchar': str,
    'array': str,
    'map': str,
    'row': str,
}

PANDAS_CONVERTERS = {
    'decimal': Decimal,
    'varbinary': lambda b: binascii.a2b_hex(''.join(b.split(' '))),
    'json': json.loads,
}

PANDAS_PARSE_DATES = [
    'date',
    'time',
    'time with time zone',
    'timestamp',
    'timestamp with time zone',
]
