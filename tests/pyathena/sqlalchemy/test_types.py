# -*- coding: utf-8 -*-
import pytest
from sqlalchemy import Integer, String
from sqlalchemy.sql import sqltypes

from pyathena.sqlalchemy.types import STRUCT, AthenaStruct


class TestAthenaStruct:
    def test_creation_with_strings(self):
        struct_type = AthenaStruct("name", "age")
        assert "name" in struct_type.fields
        assert "age" in struct_type.fields
        assert isinstance(struct_type.fields["name"], sqltypes.String)
        assert isinstance(struct_type.fields["age"], sqltypes.String)

    def test_creation_with_tuples(self):
        struct_type = AthenaStruct(("name", String), ("age", Integer))
        assert "name" in struct_type.fields
        assert "age" in struct_type.fields
        assert isinstance(struct_type.fields["name"], sqltypes.String)
        assert isinstance(struct_type.fields["age"], sqltypes.Integer)

    def test_creation_with_type_instances(self):
        struct_type = AthenaStruct(("name", String()), ("age", Integer()))
        assert "name" in struct_type.fields
        assert "age" in struct_type.fields
        assert isinstance(struct_type.fields["name"], sqltypes.String)
        assert isinstance(struct_type.fields["age"], sqltypes.Integer)

    def test_field_access_by_key(self):
        struct_type = AthenaStruct(("name", String), ("age", Integer))
        name_field = struct_type["name"]
        assert isinstance(name_field, sqltypes.String)

    def test_python_type(self):
        struct_type = AthenaStruct(("name", String))
        assert struct_type.python_type is dict

    def test_invalid_field_specification(self):
        with pytest.raises(ValueError):
            AthenaStruct(123)  # Invalid field type

    def test_visit_name(self):
        struct_type = AthenaStruct()
        assert struct_type.__visit_name__ == "struct"

    def test_struct_uppercase_visit_name(self):
        struct_type = STRUCT()
        assert struct_type.__visit_name__ == "STRUCT"
