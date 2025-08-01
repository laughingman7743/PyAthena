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


def test_to_struct_empty_string():
    result = _to_struct("")
    assert result is None


def test_to_struct_non_dict_json():
    # Arrays and other non-dict JSON should return None
    array_json = '[1, 2, 3]'
    result = _to_struct(array_json)
    assert result is None
    
    string_json = '"just a string"'
    result = _to_struct(string_json)
    assert result is None
    
    number_json = '42'
    result = _to_struct(number_json)
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

    def test_struct_conversion_empty_string(self):
        converter = DefaultTypeConverter()
        result = converter.convert("row", "")
        assert result is None

    def test_struct_conversion_invalid_json(self):
        converter = DefaultTypeConverter()
        result = converter.convert("row", "invalid json")
        assert result is None

    def test_struct_conversion_athena_format(self):
        """Test conversion of actual Athena struct format like {a=1, b=2}"""
        converter = DefaultTypeConverter()
        # This is how Athena actually returns struct data in some cases
        # For now, our converter expects JSON format, but this test documents the behavior
        result = converter.convert("row", "{a=1, b=2}")
        # Currently returns None because it's not valid JSON
        # This could be enhanced in the future to parse Athena's struct format
        assert result is None
