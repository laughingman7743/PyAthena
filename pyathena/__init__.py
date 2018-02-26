# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals
import datetime

from pyathena.error import *  # noqa


__version__ = '1.2.2'


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


STRING = DBAPITypeObject('CHAR', 'NCHAR',
                         'VARCHAR', 'NVARCHAR',
                         'LONGVARCHAR', 'LONGNVARCHAR')
BINARY = DBAPITypeObject('BINARY', 'VARBINARY', 'LONGVARBINARY')
NUMBER = DBAPITypeObject('BOOLEAN', 'TINYINT', 'SMALLINT', 'BIGINT', 'INTEGER',
                         'REAL', 'DOUBLE', 'FLOAT', 'DECIMAL', 'NUMERIC')
DATETIME = DBAPITypeObject('TIMESTAMP')
ROWID = DBAPITypeObject('')


Date = datetime.date
Time = datetime.time
Timestamp = datetime.datetime


def connect(*args, **kwargs):
    from pyathena.connection import Connection
    return Connection(*args, **kwargs)
