# -*- coding: utf-8 -*-
import pytest

from pyathena import DataError
from pyathena.util import parse_output_location, strtobool


def test_parse_output_location():
    # valid
    actual = parse_output_location("s3://bucket/path/to")
    assert actual[0] == "bucket"
    assert actual[1] == "path/to"

    # invalid
    with pytest.raises(DataError):
        parse_output_location("http://foobar")


def test_strtobool():
    yes = ("y", "Y", "yes", "True", "t", "true", "True", "On", "on", "1")
    no = ("n", "no", "f", "false", "off", "0", "Off", "No", "N")

    for y in yes:
        assert strtobool(y)

    for n in no:
        assert not strtobool(n)
