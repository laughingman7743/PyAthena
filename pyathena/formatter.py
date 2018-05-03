# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import logging
from datetime import date, datetime
from decimal import Decimal

from future.utils import iteritems
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
    return "'{0}'".format(val
                          .replace('\\', '\\\\')
                          .replace("'", "\\'")
                          .replace('\r', '\\r')
                          .replace('\n', '\\n')
                          .replace('\t', '\\t'))


def _format_none(formatter, escaper, val):
    return 'null'


def _format_default(formatter, escaper, val):
    return val


def _format_date(formatter, escaper, val):
    if escaper is _escape_presto:
        return "date'{0}'".format(val.strftime("%Y-%m-%d"))
    else:
        return "'{0}'".format(val.strftime("%Y-%m-%d"))


def _format_datetime(formatter, escaper, val):
    if escaper is _escape_presto:
        return "timestamp'{0}'".format(val.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3])
    else:
        return "'{0}'".format(val.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3])


def _format_bool(formatter, escaper, val):
    return str(val)


def _format_str(formatter, escaper, val):
    return escaper(val)


def _format_seq(formatter, escaper, val):
    results = []
    for v in val:
        func = formatter.get_formatter(v)
        formatted = func(formatter, escaper, v)
        if not isinstance(formatted, (str, unicode, )):
            # force string format
            if isinstance(formatted, (float, Decimal, )):
                formatted = '{0:f}'.format(formatted)
            else:
                formatted = '{0}'.format(formatted)
        results.append(formatted)
    return '({0})'.format(','.join(results))


class ParameterFormatter(object):

    def __init__(self):
        self.mappings = _DEFAULT_FORMATTERS

    def get_formatter(self, val):
        func = self.mappings.get(type(val), None)
        if not func:
            raise TypeError('{0} is not defined formatter.'.format(type(val)))
        return func

    def format(self, operation, parameters=None):
        if not operation or not operation.strip():
            raise ProgrammingError('Query is none or empty.')
        operation = operation.strip()

        if operation.upper().startswith('SELECT') or operation.upper().startswith('WITH'):
            escaper = _escape_presto
        else:
            escaper = _escape_hive

        kwargs = dict()
        if parameters:
            if isinstance(parameters, dict):
                for k, v in iteritems(parameters):
                    func = self.get_formatter(v)
                    kwargs.update({k: func(self, escaper, v)})
            else:
                raise ProgrammingError('Unsupported parameter ' +
                                       '(Support for dict only): {0}'.format(parameters))

        return (operation % kwargs).strip() if kwargs else operation.strip()

    def register_formatter(self, type_, formatter):
        self.mappings[type_] = formatter


_DEFAULT_FORMATTERS = {
    type(None): _format_none,
    date: _format_date,
    datetime: _format_datetime,
    int: _format_default,
    float: _format_default,
    long: _format_default,
    Decimal: _format_default,
    bool: _format_bool,
    str: _format_str,
    unicode: _format_str,
    list: _format_seq,
    set: _format_seq,
    tuple: _format_seq,
}
