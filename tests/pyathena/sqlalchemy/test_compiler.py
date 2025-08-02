# -*- coding: utf-8 -*-

from unittest.mock import Mock

from sqlalchemy import Integer, String

from pyathena.sqlalchemy.compiler import AthenaTypeCompiler
from pyathena.sqlalchemy.types import MAP, STRUCT, AthenaMap, AthenaStruct


class TestAthenaTypeCompiler:
    def test_visit_struct_empty(self):
        dialect = Mock()
        compiler = AthenaTypeCompiler(dialect)
        struct_type = AthenaStruct()
        result = compiler.visit_struct(struct_type)
        assert result == "ROW()"

    def test_visit_struct_with_fields(self):
        dialect = Mock()
        compiler = AthenaTypeCompiler(dialect)
        struct_type = AthenaStruct(("name", String), ("age", Integer))
        result = compiler.visit_struct(struct_type)
        # The exact order might vary, so we check that both fields are present
        assert "ROW(" in result
        assert "name STRING" in result or "name VARCHAR" in result
        assert "age INTEGER" in result
        assert result.endswith(")")

    def test_visit_struct_uppercase(self):
        dialect = Mock()
        compiler = AthenaTypeCompiler(dialect)
        struct_type = STRUCT(("id", Integer), ("title", String))
        result = compiler.visit_STRUCT(struct_type)
        assert "ROW(" in result
        assert "id INTEGER" in result
        assert "title STRING" in result or "title VARCHAR" in result
        assert result.endswith(")")

    def test_visit_struct_no_fields_attribute(self):
        # Test struct type without fields attribute
        dialect = Mock()
        compiler = AthenaTypeCompiler(dialect)
        struct_type = type("MockStruct", (), {})()
        result = compiler.visit_struct(struct_type)
        assert result == "ROW()"

    def test_visit_struct_single_field(self):
        dialect = Mock()
        compiler = AthenaTypeCompiler(dialect)
        struct_type = AthenaStruct(("name", String))
        result = compiler.visit_struct(struct_type)
        assert result == "ROW(name STRING)" or result == "ROW(name VARCHAR)"

    def test_visit_map_default(self):
        dialect = Mock()
        compiler = AthenaTypeCompiler(dialect)
        map_type = AthenaMap()
        result = compiler.visit_map(map_type)
        assert result == "MAP<STRING, STRING>"

    def test_visit_map_with_types(self):
        dialect = Mock()
        compiler = AthenaTypeCompiler(dialect)
        map_type = AthenaMap(String, Integer)
        result = compiler.visit_map(map_type)
        assert result == "MAP<STRING, INTEGER>" or result == "MAP<VARCHAR, INTEGER>"

    def test_visit_map_uppercase(self):
        dialect = Mock()
        compiler = AthenaTypeCompiler(dialect)
        map_type = MAP(Integer, String)
        result = compiler.visit_MAP(map_type)
        assert result == "MAP<INTEGER, STRING>" or result == "MAP<INTEGER, VARCHAR>"

    def test_visit_map_no_attributes(self):
        # Test map type without key_type/value_type attributes
        dialect = Mock()
        compiler = AthenaTypeCompiler(dialect)
        map_type = type("MockMap", (), {})()
        result = compiler.visit_map(map_type)
        assert result == "MAP<STRING, STRING>"
