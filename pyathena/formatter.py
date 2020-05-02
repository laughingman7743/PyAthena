# -*- coding: utf-8 -*-
from __future__ import absolute_import, unicode_literals

import logging
from abc import ABCMeta, abstractmethod
from copy import deepcopy
from datetime import date, datetime
from decimal import Decimal

from future.utils import iteritems, with_metaclass
from past.types import long, unicode

from pyathena.error import ProgrammingError

_logger = logging.getLogger(__name__)


def _escape_presto(val):
    """ParamEscaper

    https://github.com/dropbox/PyHive/blob/master/pyhive/common.py"""
    return "'{0}'".format(val.replace("'", "''"))


def _escape_hive(val):
    """HiveParamEscaper

     https://github.com/dropbox/PyHive/blob/master/pyhive/hive.py"""
    return "'{0}'".format(
        val.replace("\\", "\\\\")
        .replace("'", "\\'")
        .replace("\r", "\\r")
        .replace("\n", "\\n")
        .replace("\t", "\\t")
    )


def _format_none(formatter, escaper, val):
    return "null"


def _format_default(formatter, escaper, val):
    return val


def _format_date(formatter, escaper, val):
    return "DATE '{0}'".format(val.strftime("%Y-%m-%d"))


def _format_datetime(formatter, escaper, val):
    return "TIMESTAMP '{0}'".format(val.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3])


def _format_bool(formatter, escaper, val):
    return str(val)


def _format_str(formatter, escaper, val):
    return escaper(val)


def _format_seq(formatter, escaper, val):
    results = []
    for v in val:
        func = formatter.get(v)
        formatted = func(formatter, escaper, v)
        if not isinstance(formatted, (str, unicode,)):
            # force string format
            if isinstance(formatted, (float, Decimal,)):
                formatted = "{0:f}".format(formatted)
            else:
                formatted = "{0}".format(formatted)
        results.append(formatted)
    return "({0})".format(", ".join(results))


def _format_decimal(formatter, escaper, val):
    return "DECIMAL {0}".format(escaper("{0:f}".format(val)))


_DEFAULT_FORMATTERS = {
    type(None): _format_none,
    date: _format_date,
    datetime: _format_datetime,
    int: _format_default,
    float: _format_default,
    long: _format_default,
    Decimal: _format_decimal,
    bool: _format_bool,
    str: _format_str,
    unicode: _format_str,
    list: _format_seq,
    set: _format_seq,
    tuple: _format_seq,
}


class Formatter(with_metaclass(ABCMeta, object)):
    def __init__(self, mappings, default=None):
        self._mappings = mappings
        self._default = default

    @property
    def mappings(self):
        return self._mappings

    def get(self, type_):
        return self.mappings.get(type(type_), self._default)

    def set(self, type_, formatter):
        self.mappings[type_] = formatter

    def remove(self, type_):
        self.mappings.pop(type_, None)

    def update(self, mappings):
        self.mappings.update(mappings)

    @abstractmethod
    def format(self, operation, parameters=None):
        raise NotImplementedError  # pragma: no cover


class DefaultParameterFormatter(Formatter):
    def __init__(self):
        super(DefaultParameterFormatter, self).__init__(
            mappings=deepcopy(_DEFAULT_FORMATTERS), default=None
        )

    def format(self, operation, parameters=None):
        if not operation or not operation.strip():
            raise ProgrammingError("Query is none or empty.")
        operation = operation.strip()

        if operation.upper().startswith("SELECT") or operation.upper().startswith(
            "WITH"
        ):
            escaper = _escape_presto
        else:
            escaper = _escape_hive

        kwargs = dict()
        if parameters:
            if isinstance(parameters, dict):
                for k, v in iteritems(parameters):
                    func = self.get(v)
                    if not func:
                        raise TypeError("{0} is not defined formatter.".format(type(v)))
                    kwargs.update({k: func(self, escaper, v)})
            else:
                raise ProgrammingError(
                    "Unsupported parameter "
                    + "(Support for dict only): {0}".format(parameters)
                )

        return (operation % kwargs).strip() if kwargs else operation.strip()
