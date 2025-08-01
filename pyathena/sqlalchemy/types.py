# -*- coding: utf-8 -*-
from __future__ import annotations

from datetime import date, datetime
from typing import TYPE_CHECKING, Any, Dict, Optional, Tuple, Union

from sqlalchemy.sql import sqltypes
from sqlalchemy.sql.type_api import TypeEngine

if TYPE_CHECKING:
    from sqlalchemy import Dialect
    from sqlalchemy.sql.type_api import _LiteralProcessorType


class AthenaTimestamp(TypeEngine[datetime]):
    render_literal_cast = True
    render_bind_cast = True

    @staticmethod
    def process(value: Optional[Union[datetime, Any]]) -> str:
        if isinstance(value, datetime):
            return f"""TIMESTAMP '{value.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]}'"""
        return f"TIMESTAMP '{str(value)}'"

    def literal_processor(self, dialect: "Dialect") -> Optional["_LiteralProcessorType[datetime]"]:
        return self.process


class AthenaDate(TypeEngine[date]):
    render_literal_cast = True
    render_bind_cast = True

    @staticmethod
    def process(value: Union[date, datetime, Any]) -> str:
        if isinstance(value, (date, datetime)):
            f"DATE '{value:%Y-%m-%d}'"
        return f"DATE '{str(value)}'"

    def literal_processor(self, dialect: "Dialect") -> Optional["_LiteralProcessorType[date]"]:
        return self.process


class Tinyint(sqltypes.Integer):
    __visit_name__ = "tinyint"


class TINYINT(Tinyint):
    __visit_name__ = "TINYINT"


class AthenaStruct(TypeEngine[Dict[str, Any]]):
    __visit_name__ = "struct"

    def __init__(self, *fields: Union[str, Tuple[str, Any]]) -> None:
        self.fields: Dict[str, TypeEngine[Any]] = {}

        for field in fields:
            if isinstance(field, str):
                self.fields[field] = sqltypes.String()
            elif isinstance(field, tuple) and len(field) == 2:
                field_name, field_type = field
                if isinstance(field_type, TypeEngine):
                    self.fields[field_name] = field_type
                else:
                    # Assume it's a SQLAlchemy type class and instantiate it
                    self.fields[field_name] = field_type()
            else:
                raise ValueError(f"Invalid field specification: {field}")

    def __getitem__(self, key: str) -> TypeEngine[Any]:
        return self.fields[key]

    @property
    def python_type(self) -> type:
        return dict


class STRUCT(AthenaStruct):
    __visit_name__ = "STRUCT"
