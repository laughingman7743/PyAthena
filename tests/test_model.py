# -*- coding: utf-8 -*-
from datetime import datetime

from pyathena.model import AthenaCompression, AthenaQueryExecution, AthenaRowFormat

ATHENA_QUERY_EXECUTION_RESPONSE = {
    "QueryExecution": {
        "Query": "SELECT * FROM test_table",
        "QueryExecutionContext": {"Database": "test_database"},
        "QueryExecutionId": "12345678-90ab-cdef-1234-567890abcdef",
        "ResultConfiguration": {
            "EncryptionConfiguration": {
                "EncryptionOption": "test_encryption_option",
                "KmsKey": "test_kms_key",
            },
            "OutputLocation": "s3://bucket/path/to/",
        },
        "StatementType": "DML",
        "Statistics": {
            "DataScannedInBytes": 1234567890,
            "EngineExecutionTimeInMillis": 234567890,
            "QueryQueueTimeInMillis": 34567890,
            "TotalExecutionTimeInMillis": 4567890,
            "QueryPlanningTimeInMillis": 567890,
            "ServiceProcessingTimeInMillis": 67890,
            "DataManifestLocation": "s3://bucket/path/to/",
        },
        "Status": {
            "CompletionDateTime": datetime(2019, 1, 1, 0, 0, 0),
            "State": "SUCCEEDED",
            "StateChangeReason": "test_reason",
            "SubmissionDateTime": datetime(2019, 1, 1, 0, 0, 0),
        },
        "WorkGroup": "test_work_group",
    }
}


class TestAthenaQueryExecution:
    def test_init(self):
        actual = AthenaQueryExecution(ATHENA_QUERY_EXECUTION_RESPONSE)
        assert actual.database == "test_database"
        assert actual.query_id, "12345678-90ab-cdef-1234-567890abcdef"
        assert actual.query == "SELECT * FROM test_table"
        assert actual.statement_type == "DML"
        assert actual.state == "SUCCEEDED"
        assert actual.state_change_reason == "test_reason"
        assert actual.completion_date_time == datetime(2019, 1, 1, 0, 0, 0)
        assert actual.submission_date_time == datetime(2019, 1, 1, 0, 0, 0)
        assert actual.data_scanned_in_bytes == 1234567890
        assert actual.engine_execution_time_in_millis == 234567890
        assert actual.query_queue_time_in_millis == 34567890
        assert actual.total_execution_time_in_millis == 4567890
        assert actual.query_planning_time_in_millis == 567890
        assert actual.service_processing_time_in_millis == 67890
        assert actual.output_location == "s3://bucket/path/to/"
        assert actual.data_manifest_location == "s3://bucket/path/to/"
        assert actual.encryption_option == "test_encryption_option"
        assert actual.kms_key == "test_kms_key"
        assert actual.work_group == "test_work_group"


class TestAthenaRowFormat:
    def test_is_valid(self):
        assert AthenaRowFormat.is_valid("parquet")
        assert not AthenaRowFormat.is_valid(None)
        assert not AthenaRowFormat.is_valid("")
        assert not AthenaRowFormat.is_valid("foobar")


class TestAthenaCompression:
    def test_is_valid(self):
        assert AthenaCompression.is_valid("snappy")
        assert not AthenaCompression.is_valid(None)
        assert not AthenaCompression.is_valid("")
        assert not AthenaCompression.is_valid("foobar")
