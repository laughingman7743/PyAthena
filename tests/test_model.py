# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import unittest
from datetime import datetime

from pyathena.model import AthenaQueryExecution

ATHENA_QUERY_EXECUTION_RESPONSE = {
    "QueryExecution": {
        "Query": "SELECT * FROM test_table",
        "QueryExecutionContext": {
            "Database": "test_database"
        },
        "QueryExecutionId": "12345678-90ab-cdef-1234-567890abcdef",
        "ResultConfiguration": {
            "EncryptionConfiguration": {
                "EncryptionOption": "test_encryption_option",
                "KmsKey": "test_kms_key"
            },
            "OutputLocation": "s3://bucket/path/to/"
        },
        "StatementType": "DML",
        "Statistics": {
            "DataScannedInBytes": 1234567890,
            "EngineExecutionTimeInMillis": 1234567890,
        },
        "Status": {
            "CompletionDateTime": datetime(2019, 1, 1, 0, 0, 0),
            "State": "SUCCEEDED",
            "StateChangeReason": "test_reason",
            "SubmissionDateTime": datetime(2019, 1, 1, 0, 0, 0),
        },
        "WorkGroup": "test_work_group"
    }
}


class TestAthenaQueryExecution(unittest.TestCase):

    def test_init(self):
        actual = AthenaQueryExecution(ATHENA_QUERY_EXECUTION_RESPONSE)
        self.assertEqual(actual.database, 'test_database')
        self.assertEqual(actual.query_id, '12345678-90ab-cdef-1234-567890abcdef')
        self.assertEqual(actual.query, 'SELECT * FROM test_table')
        self.assertEqual(actual.statement_type, 'DML')
        self.assertEqual(actual.state, 'SUCCEEDED')
        self.assertEqual(actual.state_change_reason, 'test_reason')
        self.assertEqual(actual.completion_date_time, datetime(2019, 1, 1, 0, 0, 0))
        self.assertEqual(actual.submission_date_time, datetime(2019, 1, 1, 0, 0, 0))
        self.assertEqual(actual.data_scanned_in_bytes, 1234567890)
        self.assertEqual(actual.execution_time_in_millis, 1234567890)
        self.assertEqual(actual.output_location, 's3://bucket/path/to/')
        self.assertEqual(actual.encryption_option, 'test_encryption_option')
        self.assertEqual(actual.kms_key, 'test_kms_key')
        self.assertEqual(actual.work_group, 'test_work_group')
