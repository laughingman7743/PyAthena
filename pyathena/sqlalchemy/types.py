# -*- coding: utf-8 -*-
from __future__ import annotations

from datetime import date, datetime
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple, Union

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


class AthenaMap(TypeEngine[Dict[str, Any]]):
    __visit_name__ = "map"

    def __init__(self, key_type: Any = None, value_type: Any = None) -> None:
        if key_type is None:
            self.key_type: TypeEngine[Any] = sqltypes.String()
        elif isinstance(key_type, TypeEngine):
            self.key_type = key_type
        else:
            # Assume it's a SQLAlchemy type class and instantiate it
            self.key_type = key_type()

        if value_type is None:
            self.value_type: TypeEngine[Any] = sqltypes.String()
        elif isinstance(value_type, TypeEngine):
            self.value_type = value_type
        else:
            # Assume it's a SQLAlchemy type class and instantiate it
            self.value_type = value_type()

    @property
    def python_type(self) -> type:
        return dict


class MAP(AthenaMap):
    __visit_name__ = "MAP"


class AthenaArray(TypeEngine[List[Any]]):
    __visit_name__ = "array"

    def __init__(self, item_type: Any = None) -> None:
        if item_type is None:
            self.item_type: TypeEngine[Any] = sqltypes.String()
        elif isinstance(item_type, TypeEngine):
            self.item_type = item_type
        else:
            # Assume it's a SQLAlchemy type class and instantiate it
            self.item_type = item_type()

    @property
    def python_type(self) -> type:
        return list


class ARRAY(AthenaArray):
    __visit_name__ = "ARRAY"
