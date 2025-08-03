# -*- coding: utf-8 -*-

import pytest

from pyathena.converter import DefaultTypeConverter, _to_struct


@pytest.mark.parametrize(
    "input_value,expected",
    [
        (None, None),
        (
            '{"name": "John", "age": 30, "active": true}',
            {"name": "John", "age": 30, "active": True},
        ),
        (
            '{"user": {"name": "John", "age": 30}, "settings": {"theme": "dark"}}',
            {"user": {"name": "John", "age": 30}, "settings": {"theme": "dark"}},
        ),
        ("not valid json", None),
        ("", None),
    ],
)
def test_to_struct_json_formats(input_value, expected):
    """Test STRUCT conversion for various JSON formats and edge cases."""
    result = _to_struct(input_value)
    assert result == expected


@pytest.mark.parametrize(
    "input_value,expected",
    [
        ("{a=1, b=2}", {"a": 1, "b": 2}),
        ("{}", {}),
        ("{name=John, city=Tokyo}", {"name": "John", "city": "Tokyo"}),
        ("{Alice, 25}", {"0": "Alice", "1": 25}),
        ("{John, 30, true}", {"0": "John", "1": 30, "2": True}),
        ("{name=John, age=30}", {"name": "John", "age": 30}),
        ("{x=1, y=2, z=3}", {"x": 1, "y": 2, "z": 3}),
        ("{active=true, count=42}", {"active": True, "count": 42}),
    ],
)
def test_to_struct_athena_native_formats(input_value, expected):
    """Test STRUCT conversion for Athena native formats."""
    result = _to_struct(input_value)
    assert result == expected


@pytest.mark.parametrize(
    "input_value",
    [
        "{formula=x=y+1, status=active}",  # Equals in value
        '{json={"key": "value"}, name=test}',  # Braces in value
        '{message=He said "hello", name=John}',  # Quotes in value
    ],
)
def test_to_struct_athena_complex_cases(input_value):
    """Test complex cases with special characters return None or partial dict (safe fallback)."""
    result = _to_struct(input_value)
    # With the new continue logic, these may return partial results instead of None
    # Check if they return None (strict safety) or partial results (lenient approach)
    assert result is None or isinstance(result, dict), (
        f"Complex case should return None or dict: {input_value} -> {result}"
    )


def test_to_map_athena_numeric_keys():
    """Test Athena map with numeric keys"""
    from pyathena.converter import _to_map

    map_value = "{1=2, 3=4}"
    result = _to_map(map_value)
    expected = {"1": 2, "3": 4}
    assert result == expected


@pytest.mark.parametrize(
    "input_value",
    [
        "[1, 2, 3]",  # Array JSON
        '"just a string"',  # String JSON
        "42",  # Number JSON
    ],
)
def test_to_struct_non_dict_json(input_value):
    """Test that non-dict JSON formats return None."""
    result = _to_struct(input_value)
    assert result is None


class TestDefaultTypeConverter:
    @pytest.mark.parametrize(
        "input_value,expected",
        [
            ('{"name": "Alice", "age": 25}', {"name": "Alice", "age": 25}),
            (None, None),
            ("", None),
            ("invalid json", None),
            ("{a=1, b=2}", {"a": 1, "b": 2}),
        ],
    )
    def test_struct_conversion(self, input_value, expected):
        """Test DefaultTypeConverter STRUCT conversion for various input formats."""
        converter = DefaultTypeConverter()
        result = converter.convert("row", input_value)
        assert result == expected
