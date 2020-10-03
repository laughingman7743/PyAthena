# -*- coding: utf-8 -*-
import datetime
from typing import Type

from pyathena.error import *  # noqa

__version__: str = "1.11.2"

# Globals https://www.python.org/dev/peps/pep-0249/#globals
apilevel: str = "2.0"
threadsafety: int = 3
paramstyle: str = "pyformat"


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
STRING: DBAPITypeObject = DBAPITypeObject("char", "varchar", "map", "array", "row")
BINARY: DBAPITypeObject = DBAPITypeObject("varbinary")
BOOLEAN: DBAPITypeObject = DBAPITypeObject("boolean")
NUMBER: DBAPITypeObject = DBAPITypeObject(
    "tinyint", "smallint", "bigint", "integer", "real", "double", "float", "decimal"
)
DATE: DBAPITypeObject = DBAPITypeObject("date")
TIME: DBAPITypeObject = DBAPITypeObject("time", "time with time zone")
DATETIME: DBAPITypeObject = DBAPITypeObject("timestamp", "timestamp with time zone")
JSON: DBAPITypeObject = DBAPITypeObject("json")

Date: Type[datetime.date] = datetime.date
Time: Type[datetime.time] = datetime.time
Timestamp: Type[datetime.datetime] = datetime.datetime


def connect(*args, **kwargs):
    from pyathena.connection import Connection

    return Connection(*args, **kwargs)
