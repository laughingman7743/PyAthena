# -*- coding: utf-8 -*-

from sqlalchemy import Integer, String

from pyathena.sqlalchemy.compiler import AthenaTypeCompiler
from pyathena.sqlalchemy.types import STRUCT, AthenaStruct


class TestAthenaTypeCompiler:
    def test_visit_struct_empty(self):
        compiler = AthenaTypeCompiler()
        struct_type = AthenaStruct()
        result = compiler.visit_struct(struct_type)
        assert result == "ROW()"

    def test_visit_struct_with_fields(self):
        compiler = AthenaTypeCompiler()
        struct_type = AthenaStruct(("name", String), ("age", Integer))
        result = compiler.visit_struct(struct_type)
        # The exact order might vary, so we check that both fields are present
        assert "ROW(" in result
        assert "name STRING" in result or "name VARCHAR" in result
        assert "age INTEGER" in result
        assert result.endswith(")")

    def test_visit_struct_uppercase(self):
        compiler = AthenaTypeCompiler()
        struct_type = STRUCT(("id", Integer), ("title", String))
        result = compiler.visit_STRUCT(struct_type)
        assert "ROW(" in result
        assert "id INTEGER" in result
        assert "title STRING" in result or "title VARCHAR" in result
        assert result.endswith(")")
