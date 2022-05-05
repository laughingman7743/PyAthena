# -*- coding: utf-8 -*-
import copy
from datetime import datetime

from pyathena.model import (
    AthenaCompression,
    AthenaFileFormat,
    AthenaQueryExecution,
    AthenaRowFormatSerde,
    AthenaTableMetadata,
)

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
ATHENA_TABLE_METADATA_RESPONSE = {
    "TableMetadata": {
        "Name": "test_name",
        "CreateTime": datetime(2015, 1, 1, 0, 0, 0),
        "LastAccessTime": datetime(2015, 1, 1, 0, 0, 0),
        "TableType": "test_table_type",
        "Columns": [
            {"Name": "test_name_1", "Type": "test_type_1", "Comment": "test_comment_1"},
            {"Name": "test_name_2", "Type": "test_type_2", "Comment": "test_comment_2"},
        ],
        "PartitionKeys": [
            {"Name": "test_name_1", "Type": "test_type_1", "Comment": "test_comment_1"},
            {"Name": "test_name_2", "Type": "test_type_2", "Comment": "test_comment_2"},
        ],
        "Parameters": {"comment": "test_comment"},
    }
}
ATHENA_TABLE_METADATA_PARAMETERS_TEXT = {
    "EXTERNAL": "TRUE",
    "has_encrypted_data": "false",
    "inputformat": "org.apache.hadoop.mapred.TextInputFormat",
    "location": "s3://bucket/path/to",
    "outputformat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
    "presto_query_id": "20220101_123456_98765_abcde",
    "serde.serialization.lib": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
    "write.compression": "SNAPPY",
}
ATHENA_TABLE_METADATA_PARAMETERS_JSON = {
    "EXTERNAL": "TRUE",
    "inputformat": "org.apache.hadoop.mapred.TextInputFormat",
    "location": "s3://bucket/path/to",
    "outputformat": "org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat",
    "serde.param.serialization.format": "1",
    "serde.param.write.compression": "GZIP",
    "serde.serialization.lib": "org.openx.data.jsonserde.JsonSerDe",
    "transient_lastDdlTime": "1234567890",
}
ATHENA_TABLE_METADATA_PARAMETERS_JSON_HCATALOG = {
    "EXTERNAL": "TRUE",
    "has_encrypted_data": "false",
    "inputformat": "org.apache.hadoop.mapred.TextInputFormat",
    "location": "s3://bucket/path/to",
    "outputformat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
    "presto_query_id": "20220101_123456_98765_abcde",
    "serde.serialization.lib": "org.apache.hive.hcatalog.data.JsonSerDe",
    "write.compression": "SNAPPY",
}
ATHENA_TABLE_METADATA_PARAMETERS_PARQUET = {
    "EXTERNAL": "TRUE",
    "inputformat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
    "location": "s3://bucket/path/to",
    "outputformat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
    "parquet.compress": "SNAPPY",
    "serde.param.serialization.format": "1",
    "serde.serialization.lib": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
    "transient_lastDdlTime": "1234567890",
}
ATHENA_TABLE_METADATA_PARAMETERS_ORC = {
    "EXTERNAL": "TRUE",
    "has_encrypted_data": "false",
    "inputformat": "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat",
    "location": "s3://bucket/path/to",
    "orc.compress": "SNAPPY",
    "outputformat": "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat",
    "presto_query_id": "20220101_123456_98765_abcde",
    "serde.serialization.lib": "org.apache.hadoop.hive.ql.io.orc.OrcSerde",
}
ATHENA_TABLE_METADATA_PARAMETERS_AVRO = {
    "EXTERNAL": "TRUE",
    "has_encrypted_data": "false",
    "inputformat": "org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat",
    "location": "s3://bucket/path/to",
    "outputformat": "org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat",
    "presto_query_id": "20220101_123456_98765_abcde",
    "serde.serialization.lib": "org.apache.hadoop.hive.serde2.avro.AvroSerDe",
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


class TestAthenaTableMetadata:
    def test_init(self):
        actual = AthenaTableMetadata(ATHENA_TABLE_METADATA_RESPONSE)
        assert actual.name == "test_name"
        assert actual.create_time == datetime(2015, 1, 1, 0, 0, 0)
        assert actual.last_access_time == datetime(2015, 1, 1, 0, 0, 0)
        assert actual.table_type == "test_table_type"

        columns = actual.columns
        assert len(columns) == 2
        assert columns[0].name == "test_name_1"
        assert columns[0].type == "test_type_1"
        assert columns[0].comment == "test_comment_1"
        assert columns[1].name == "test_name_2"
        assert columns[1].type == "test_type_2"
        assert columns[1].comment == "test_comment_2"

        partition_keys = actual.partition_keys
        assert len(partition_keys) == 2
        assert partition_keys[0].name == "test_name_1"
        assert partition_keys[0].type == "test_type_1"
        assert partition_keys[0].comment == "test_comment_1"
        assert partition_keys[1].name == "test_name_2"
        assert partition_keys[1].type == "test_type_2"
        assert partition_keys[1].comment == "test_comment_2"

        assert actual.parameters == {"comment": "test_comment"}
        assert actual.comment == "test_comment"
        assert actual.location is None
        assert actual.input_format is None
        assert actual.output_format is None
        assert actual.serde_serialization_lib is None
        assert actual.compression is None

    def test_init_text(self):
        response = copy.deepcopy(ATHENA_TABLE_METADATA_RESPONSE)
        response["TableMetadata"]["Parameters"] = ATHENA_TABLE_METADATA_PARAMETERS_TEXT
        actual = AthenaTableMetadata(response)
        assert actual.parameters == ATHENA_TABLE_METADATA_PARAMETERS_TEXT
        assert actual.comment is None
        assert actual.location == "s3://bucket/path/to"
        assert actual.input_format == "org.apache.hadoop.mapred.TextInputFormat"
        assert (
            actual.output_format
            == "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
        )
        assert (
            actual.serde_serialization_lib
            == "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"
        )
        assert actual.compression == "SNAPPY"
        assert (
            actual.row_format
            == "SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'"
        )
        assert (
            actual.file_format
            == "INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' "
            "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'"
        )

    def test_init_json(self):
        response = copy.deepcopy(ATHENA_TABLE_METADATA_RESPONSE)
        response["TableMetadata"]["Parameters"] = ATHENA_TABLE_METADATA_PARAMETERS_JSON
        actual = AthenaTableMetadata(response)
        assert actual.parameters == ATHENA_TABLE_METADATA_PARAMETERS_JSON
        assert actual.comment is None
        assert actual.location == "s3://bucket/path/to"
        assert actual.input_format == "org.apache.hadoop.mapred.TextInputFormat"
        assert (
            actual.output_format
            == "org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat"
        )
        assert actual.serde_serialization_lib == "org.openx.data.jsonserde.JsonSerDe"
        assert actual.compression == "GZIP"
        assert actual.serde_properties == {
            "serialization.format": "1",
            "write.compression": "GZIP",
        }
        for key in actual.table_properties.keys():
            assert not key.startswith("serde.param.")
        assert actual.row_format == "SERDE 'org.openx.data.jsonserde.JsonSerDe'"
        assert (
            actual.file_format
            == "INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' "
            "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat'"
        )

    def test_init_json_hcatalog(self):
        response = copy.deepcopy(ATHENA_TABLE_METADATA_RESPONSE)
        response["TableMetadata"][
            "Parameters"
        ] = ATHENA_TABLE_METADATA_PARAMETERS_JSON_HCATALOG
        actual = AthenaTableMetadata(response)
        assert actual.parameters == ATHENA_TABLE_METADATA_PARAMETERS_JSON_HCATALOG
        assert actual.comment is None
        assert actual.location == "s3://bucket/path/to"
        assert actual.input_format == "org.apache.hadoop.mapred.TextInputFormat"
        assert (
            actual.output_format
            == "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
        )
        assert (
            actual.serde_serialization_lib == "org.apache.hive.hcatalog.data.JsonSerDe"
        )
        assert actual.compression == "SNAPPY"
        assert not actual.serde_properties
        for key in actual.table_properties.keys():
            assert not key.startswith("serde.param.")
        assert actual.row_format == "SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'"
        assert (
            actual.file_format
            == "INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' "
            "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'"
        )

    def test_init_parquet(self):
        response = copy.deepcopy(ATHENA_TABLE_METADATA_RESPONSE)
        response["TableMetadata"][
            "Parameters"
        ] = ATHENA_TABLE_METADATA_PARAMETERS_PARQUET
        actual = AthenaTableMetadata(response)
        assert actual.parameters == ATHENA_TABLE_METADATA_PARAMETERS_PARQUET
        assert actual.comment is None
        assert actual.location == "s3://bucket/path/to"
        assert (
            actual.input_format
            == "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
        )
        assert (
            actual.output_format
            == "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
        )
        assert (
            actual.serde_serialization_lib
            == "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
        )
        assert actual.compression == "SNAPPY"
        assert actual.serde_properties == {
            "serialization.format": "1",
        }
        for key in actual.table_properties.keys():
            assert not key.startswith("serde.param.")
        assert (
            actual.row_format
            == "SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'"
        )
        assert (
            actual.file_format
            == "INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' "
            "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'"
        )

    def test_init_orc(self):
        response = copy.deepcopy(ATHENA_TABLE_METADATA_RESPONSE)
        response["TableMetadata"]["Parameters"] = ATHENA_TABLE_METADATA_PARAMETERS_ORC
        actual = AthenaTableMetadata(response)
        assert actual.parameters == ATHENA_TABLE_METADATA_PARAMETERS_ORC
        assert actual.comment is None
        assert actual.location == "s3://bucket/path/to"
        assert actual.input_format == "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat"
        assert (
            actual.output_format == "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat"
        )
        assert (
            actual.serde_serialization_lib
            == "org.apache.hadoop.hive.ql.io.orc.OrcSerde"
        )
        assert actual.compression == "SNAPPY"
        assert not actual.serde_properties
        for key in actual.table_properties.keys():
            assert not key.startswith("serde.param.")
        assert actual.row_format == "SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'"
        assert (
            actual.file_format
            == "INPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' "
            "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'"
        )

    def test_init_avro(self):
        response = copy.deepcopy(ATHENA_TABLE_METADATA_RESPONSE)
        response["TableMetadata"]["Parameters"] = ATHENA_TABLE_METADATA_PARAMETERS_AVRO
        actual = AthenaTableMetadata(response)
        assert actual.parameters == ATHENA_TABLE_METADATA_PARAMETERS_AVRO
        assert actual.comment is None
        assert actual.location == "s3://bucket/path/to"
        assert (
            actual.input_format
            == "org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat"
        )
        assert (
            actual.output_format
            == "org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat"
        )
        assert (
            actual.serde_serialization_lib
            == "org.apache.hadoop.hive.serde2.avro.AvroSerDe"
        )
        assert actual.compression is None
        assert not actual.serde_properties
        for key in actual.table_properties.keys():
            assert not key.startswith("serde.param.")
        assert (
            actual.row_format == "SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'"
        )
        assert (
            actual.file_format
            == "INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat' "
            "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'"
        )


class TestAthenaFileFormat:
    def test_is_parquet(self):
        assert AthenaFileFormat.is_parquet("parquet")
        assert AthenaFileFormat.is_parquet("PARQUET")
        assert not AthenaFileFormat.is_parquet("")
        assert not AthenaFileFormat.is_parquet("foobar")

    def test_is_orc(self):
        assert AthenaFileFormat.is_orc("orc")
        assert AthenaFileFormat.is_orc("ORC")
        assert not AthenaFileFormat.is_orc("")
        assert not AthenaFileFormat.is_orc("foobar")


class TestAthenaRowFormatSerde:
    def test_is_parquet(self):
        assert AthenaRowFormatSerde.is_parquet(
            "SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'"
        )
        assert AthenaRowFormatSerde.is_parquet(
            "SerDe 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'"
        )
        assert AthenaRowFormatSerde.is_parquet(
            "Serde 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'"
        )
        assert AthenaRowFormatSerde.is_parquet(
            "serde 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'"
        )
        assert not AthenaRowFormatSerde.is_parquet("")
        assert not AthenaRowFormatSerde.is_parquet(
            "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
        )
        assert not AthenaRowFormatSerde.is_parquet("foobar")

    def test_is_orc(self):
        assert AthenaRowFormatSerde.is_orc(
            "SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'"
        )
        assert AthenaRowFormatSerde.is_orc(
            "SerDe 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'"
        )
        assert AthenaRowFormatSerde.is_orc(
            "Serde 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'"
        )
        assert AthenaRowFormatSerde.is_orc(
            "serde 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'"
        )
        assert not AthenaRowFormatSerde.is_orc("")
        assert not AthenaRowFormatSerde.is_orc(
            "org.apache.hadoop.hive.ql.io.orc.OrcSerde"
        )
        assert not AthenaRowFormatSerde.is_orc("foobar")


class TestAthenaCompression:
    def test_is_valid(self):
        assert AthenaCompression.is_valid("snappy")
        assert AthenaCompression.is_valid("SNAPPY")
        assert not AthenaCompression.is_valid("")
        assert not AthenaCompression.is_valid("foobar")
