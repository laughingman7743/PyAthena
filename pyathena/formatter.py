# -*- coding: utf-8 -*-
import logging
from abc import ABCMeta, abstractmethod
from copy import deepcopy
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Callable, Dict, Optional, Type, TypeVar

from pyathena.error import ProgrammingError

_logger = logging.getLogger(__name__)  # type: ignore
_T = TypeVar("_T", bound="Formatter")


class Formatter(object, metaclass=ABCMeta):
    def __init__(
        self,
        mappings: Dict[Type[Any], Callable[[_T, Callable[[str], str], Any], Any]],
        default: Callable[[_T, Callable[[str], str], Any], Any] = None,
    ) -> None:
        self._mappings = mappings
        self._default = default

    @property
    def mappings(
        self,
    ) -> Dict[Type[Any], Callable[[_T, Callable[[str], str], Any], Any]]:
        return self._mappings

    def get(self, type_) -> Optional[Callable[[_T, Callable[[str], str], Any], Any]]:
        return self.mappings.get(type(type_), self._default)

    def set(
        self,
        type_: Type[Any],
        formatter: Callable[[_T, Callable[[str], str], Any], Any],
    ) -> None:
        self.mappings[type_] = formatter

    def remove(self, type_: Type[Any]) -> None:
        self.mappings.pop(type_, None)

    def update(
        self, mappings: Dict[Type[Any], Callable[[_T, Callable[[str], str], Any], Any]]
    ) -> None:
        self.mappings.update(mappings)

    @abstractmethod
    def format(
        self, operation: str, parameters: Optional[Dict[str, Any]] = None
    ) -> str:
        raise NotImplementedError  # pragma: no cover


def _escape_presto(val: str) -> str:
    """ParamEscaper

    https://github.com/dropbox/PyHive/blob/master/pyhive/common.py"""
    return "'{0}'".format(val.replace("'", "''"))


def _escape_hive(val: str) -> str:
    """HiveParamEscaper

    https://github.com/dropbox/PyHive/blob/master/pyhive/hive.py"""
    return "'{0}'".format(
        val.replace("\\", "\\\\")
        .replace("'", "\\'")
        .replace("\r", "\\r")
        .replace("\n", "\\n")
        .replace("\t", "\\t")
    )


def _format_none(formatter: Formatter, escaper: Callable[[str], str], val: Any) -> Any:
    return "null"


def _format_default(
    formatter: Formatter, escaper: Callable[[str], str], val: Any
) -> Any:
    return val


def _format_date(formatter: Formatter, escaper: Callable[[str], str], val: Any) -> Any:
    return "DATE '{0}'".format(val.strftime("%Y-%m-%d"))


def _format_datetime(
    formatter: Formatter, escaper: Callable[[str], str], val: Any
) -> Any:
    return "TIMESTAMP '{0}'".format(val.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3])


def _format_bool(formatter: Formatter, escaper: Callable[[str], str], val: Any) -> Any:
    return str(val)


def _format_str(formatter: Formatter, escaper: Callable[[str], str], val: Any) -> Any:
    return escaper(val)


def _format_seq(formatter: Formatter, escaper: Callable[[str], str], val: Any) -> Any:
    results = []
    for v in val:
        func = formatter.get(v)
        if not func:
            raise TypeError("{0} is not defined formatter.".format(type(v)))
        formatted = func(formatter, escaper, v)
        if not isinstance(
            formatted,
            (str,),
        ):
            # force string format
            if isinstance(
                formatted,
                (
                    float,
                    Decimal,
                ),
            ):
                formatted = "{0:f}".format(formatted)
            else:
                formatted = "{0}".format(formatted)
        results.append(formatted)
    return "({0})".format(", ".join(results))


def _format_decimal(
    formatter: Formatter, escaper: Callable[[str], str], val: Any
) -> Any:
    return "DECIMAL {0}".format(escaper("{0:f}".format(val)))


_DEFAULT_FORMATTERS: Dict[
    Type[Any], Callable[[Formatter, Callable[[str], str], Any], Any]
] = {
    type(None): _format_none,
    date: _format_date,
    datetime: _format_datetime,
    int: _format_default,
    float: _format_default,
    Decimal: _format_decimal,
    bool: _format_bool,
    str: _format_str,
    list: _format_seq,
    set: _format_seq,
    tuple: _format_seq,
}


class DefaultParameterFormatter(Formatter):
    def __init__(self) -> None:
        super(DefaultParameterFormatter, self).__init__(
            mappings=deepcopy(_DEFAULT_FORMATTERS), default=None
        )

    def format(
        self, operation: str, parameters: Optional[Dict[str, Any]] = None
    ) -> str:
        if not operation or not operation.strip():
            raise ProgrammingError("Query is none or empty.")
        operation = operation.strip()

        if operation.upper().startswith("SELECT") or operation.upper().startswith(
            "WITH"
        ):
            escaper = _escape_presto
        else:
            escaper = _escape_hive

        kwargs: Optional[Dict[str, Any]] = None
        if parameters is not None:
            kwargs = dict()
            if isinstance(parameters, dict):
                for k, v in parameters.items():
                    func = self.get(v)
                    if not func:
                        raise TypeError("{0} is not defined formatter.".format(type(v)))
                    kwargs.update({k: func(self, escaper, v)})
            else:
                raise ProgrammingError(
                    "Unsupported parameter "
                    + "(Support for dict only): {0}".format(parameters)
                )

        return (operation % kwargs).strip() if kwargs is not None else operation.strip()
