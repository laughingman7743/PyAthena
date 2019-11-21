#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import unittest
from tempfile import NamedTemporaryFile

import pandas as pd
import numpy as np

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

    def test_handling_nas(self):
        with NamedTemporaryFile() as temp_file:
            df = pd.DataFrame({'bool_col_with_na': [True, False, np.nan]})
            df.to_csv(temp_file.name, index=False)
            clean_df = AthenaPandasResultSet._safe_read_dataframe_from_file_buffer(temp_file.name,
                                                                                   {'bool_col_with_na': bool},
                                                                                   converters={},
                                                                                   parse_dates=False)
            self.assertEqual(list(clean_df.columns), ['bool_col_with_na'])
            self.assertEqual(clean_df.dtypes.tolist(), [object])
