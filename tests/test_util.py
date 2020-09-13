# -*- coding: utf-8 -*-
import unittest

from pyathena import DataError
from pyathena.util import parse_output_location
from tests import WithConnect


class TestUtil(unittest.TestCase, WithConnect):
    def test_parse_output_location(self):
        # valid
        actual = parse_output_location("s3://bucket/path/to")
        self.assertEqual(actual[0], "bucket")
        self.assertEqual(actual[1], "path/to")

        # invalid
        with self.assertRaises(DataError):
            parse_output_location("http://foobar")
