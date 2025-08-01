# -*- coding: utf-8 -*-

from pyathena.converter import DefaultTypeConverter, _to_struct


def test_to_struct_none():
    result = _to_struct(None)
    assert result is None


def test_to_struct_valid_json():
    struct_json = '{"name": "John", "age": 30, "active": true}'
    result = _to_struct(struct_json)
    expected = {"name": "John", "age": 30, "active": True}
    assert result == expected


def test_to_struct_nested_json():
    struct_json = '{"user": {"name": "John", "age": 30}, "settings": {"theme": "dark"}}'
    result = _to_struct(struct_json)
    expected = {"user": {"name": "John", "age": 30}, "settings": {"theme": "dark"}}
    assert result == expected


def test_to_struct_invalid_json():
    invalid_json = "not valid json"
    result = _to_struct(invalid_json)
    assert result is None


class TestDefaultTypeConverter:
    def test_struct_conversion(self):
        converter = DefaultTypeConverter()
        struct_json = '{"name": "Alice", "age": 25}'
        result = converter.convert("row", struct_json)
        expected = {"name": "Alice", "age": 25}
        assert result == expected

    def test_struct_conversion_none(self):
        converter = DefaultTypeConverter()
        result = converter.convert("row", None)
        assert result is None
