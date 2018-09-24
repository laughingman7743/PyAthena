#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import unittest

from pyathena import DataError
from pyathena.result_set import AthenaPandasResultSet


class TestAthenaPandasResultSet(unittest.TestCase):

    def test_parse_output_location(self):
        actual = AthenaPandasResultSet._parse_output_location('s3://bucket/path/to')
        self.assertEqual(actual[0], 'bucket')
        self.assertEqual(actual[1], 'path/to')

    def test_parse_invalid_output_location(self):
        with self.assertRaises(DataError):
            AthenaPandasResultSet._parse_output_location('http://foobar')
