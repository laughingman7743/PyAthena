# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import datetime

from pyathena.error import *  # noqa

__version__ = '1.4.0'

# Globals https://www.python.org/dev/peps/pep-0249/#globals
apilevel = '2.0'
threadsafety = 3
paramstyle = 'pyformat'


class DBAPITypeObject:
    """Type Objects and Constructors

    https://www.python.org/dev/peps/pep-0249/#type-objects-and-constructors
    """
    def __init__(self, *values):
        self.values = values

    def __cmp__(self, other):
        if other in self.values:
            return 0
        if other < self.values:
            return 1
        else:
            return -1

    def __eq__(self, other):
        return other in self.values


# https://docs.aws.amazon.com/athena/latest/ug/data-types.html
STRING = DBAPITypeObject('char', 'varchar', 'map', 'array', 'row')
BINARY = DBAPITypeObject('varbinary')
BOOLEAN = DBAPITypeObject('boolean')
NUMBER = DBAPITypeObject('tinyint', 'smallint', 'bigint', 'integer',
                         'real', 'double', 'float', 'decimal')
DATE = DBAPITypeObject('date')
TIME = DBAPITypeObject('time', 'time with time zone')
DATETIME = DBAPITypeObject('timestamp', 'timestamp with time zone')
JSON = DBAPITypeObject('json')

Date = datetime.date
Time = datetime.time
Timestamp = datetime.datetime


def connect(*args, **kwargs):
    from pyathena.connection import Connection
    return Connection(*args, **kwargs)
