# -*- coding: utf-8 -*-
import pytest

from pyathena import DataError
from pyathena.util import parse_output_location


class TestUtil:
    def test_parse_output_location(self):
        # valid
        actual = parse_output_location("s3://bucket/path/to")
        assert actual[0] == "bucket"
        assert actual[1] == "path/to"

        # invalid
        with pytest.raises(DataError):
            parse_output_location("http://foobar")
