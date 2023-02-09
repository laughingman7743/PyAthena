# -*- coding: utf-8 -*-
import logging
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Pattern

from pyathena.error import DataError

_logger = logging.getLogger(__name__)  # type: ignore


class AthenaQueryExecution:
    STATE_QUEUED: str = "QUEUED"
    STATE_RUNNING: str = "RUNNING"
    STATE_SUCCEEDED: str = "SUCCEEDED"
    STATE_FAILED: str = "FAILED"
    STATE_CANCELLED: str = "CANCELLED"

    STATEMENT_TYPE_DDL: str = "DDL"
    STATEMENT_TYPE_DML: str = "DML"
    STATEMENT_TYPE_UTILITY: str = "UTILITY"

    ENCRYPTION_OPTION_SSE_S3: str = "SSE_S3"
    ENCRYPTION_OPTION_SSE_KMS: str = "SSE_KMS"
    ENCRYPTION_OPTION_CSE_KMS: str = "CSE_KMS"

    ERROR_CATEGORY_SYSTEM: int = 1
    ERROR_CATEGORY_USER: int = 2
    ERROR_CATEGORY_OTHER: int = 3

    S3_ACL_OPTION_BUCKET_OWNER_FULL_CONTROL = "BUCKET_OWNER_FULL_CONTROL"

    def __init__(self, response: Dict[str, Any]) -> None:
        query_execution = response.get("QueryExecution", None)
        if not query_execution:
            raise DataError("KeyError `QueryExecution`")

        query_execution_context = query_execution.get("QueryExecutionContext", {})
        self._database: Optional[str] = query_execution_context.get("Database", None)
        self._catalog: Optional[str] = query_execution_context.get("Catalog", None)

        self._query_id: Optional[str] = query_execution.get("QueryExecutionId", None)
        if not self._query_id:
            raise DataError("KeyError `QueryExecutionId`")
        self._query: Optional[str] = query_execution.get("Query", None)
        if not self._query:
            raise DataError("KeyError `Query`")
        self._statement_type: Optional[str] = query_execution.get("StatementType", None)
        self._work_group: Optional[str] = query_execution.get("WorkGroup", None)
        self._execution_parameters: List[str] = query_execution.get("ExecutionParameters", [])

        status = query_execution.get("Status", None)
        if not status:
            raise DataError("KeyError `Status`")
        self._state: Optional[str] = status.get("State", None)
        self._state_change_reason: Optional[str] = status.get("StateChangeReason", None)
        self._submission_date_time: Optional[datetime] = status.get("SubmissionDateTime", None)
        self._completion_date_time: Optional[datetime] = status.get("CompletionDateTime", None)
        athena_error = status.get("AthenaError", {})
        self._error_category: Optional[int] = athena_error.get("ErrorCategory", None)
        self._error_type: Optional[int] = athena_error.get("ErrorType", None)
        self._retryable: Optional[bool] = athena_error.get("Retryable", None)
        self._error_message: Optional[str] = athena_error.get("ErrorMessage", None)

        statistics = query_execution.get("Statistics", {})
        self._data_scanned_in_bytes: Optional[int] = statistics.get("DataScannedInBytes", None)
        self._engine_execution_time_in_millis: Optional[int] = statistics.get(
            "EngineExecutionTimeInMillis", None
        )
        self._query_queue_time_in_millis: Optional[int] = statistics.get(
            "QueryQueueTimeInMillis", None
        )
        self._total_execution_time_in_millis: Optional[int] = statistics.get(
            "TotalExecutionTimeInMillis", None
        )
        self._query_planning_time_in_millis: Optional[int] = statistics.get(
            "QueryPlanningTimeInMillis", None
        )
        self._service_processing_time_in_millis: Optional[int] = statistics.get(
            "ServiceProcessingTimeInMillis", None
        )
        self._data_manifest_location: Optional[str] = statistics.get("DataManifestLocation", None)
        reuse_info = statistics.get("ResultReuseInformation", {})
        self._reused_previous_result: Optional[bool] = reuse_info.get("ReusedPreviousResult", None)

        result_conf = query_execution.get("ResultConfiguration", {})
        self._output_location: Optional[str] = result_conf.get("OutputLocation", None)
        encryption_conf = result_conf.get("EncryptionConfiguration", {})
        self._encryption_option: Optional[str] = encryption_conf.get("EncryptionOption", None)
        self._kms_key: Optional[str] = encryption_conf.get("KmsKey", None)
        self._expected_bucket_owner: Optional[str] = result_conf.get("ExpectedBucketOwner", None)
        acl_conf = result_conf.get("AclConfiguration", {})
        self._s3_acl_option: Optional[str] = acl_conf.get("S3AclOption", None)

        engine_version = query_execution.get("EngineVersion", {})
        self._selected_engine_version: Optional[str] = engine_version.get(
            "SelectedEngineVersion", None
        )
        self._effective_engine_version: Optional[str] = engine_version.get(
            "EffectiveEngineVersion", None
        )

        reuse_conf = query_execution.get("ResultReuseConfiguration", {})
        reuse_age_conf = reuse_conf.get("ResultReuseByAgeConfiguration", {})
        self._result_reuse_enabled: Optional[bool] = reuse_age_conf.get("Enabled", None)
        self._result_reuse_minutes: Optional[int] = reuse_age_conf.get("MaxAgeInMinutes", None)

    @property
    def database(self) -> Optional[str]:
        return self._database

    @property
    def catalog(self) -> Optional[str]:
        return self._catalog

    @property
    def query_id(self) -> Optional[str]:
        return self._query_id

    @property
    def query(self) -> Optional[str]:
        return self._query

    @property
    def statement_type(self) -> Optional[str]:
        return self._statement_type

    @property
    def work_group(self) -> Optional[str]:
        return self._work_group

    @property
    def execution_parameters(self) -> List[str]:
        return self._execution_parameters

    @property
    def state(self) -> Optional[str]:
        return self._state

    @property
    def state_change_reason(self) -> Optional[str]:
        return self._state_change_reason

    @property
    def submission_date_time(self) -> Optional[datetime]:
        return self._submission_date_time

    @property
    def completion_date_time(self) -> Optional[datetime]:
        return self._completion_date_time

    @property
    def error_category(self) -> Optional[int]:
        return self._error_category

    @property
    def error_type(self) -> Optional[int]:
        return self._error_type

    @property
    def retryable(self) -> Optional[bool]:
        return self._retryable

    @property
    def error_message(self) -> Optional[str]:
        return self._error_message

    @property
    def data_scanned_in_bytes(self) -> Optional[int]:
        return self._data_scanned_in_bytes

    @property
    def engine_execution_time_in_millis(self) -> Optional[int]:
        return self._engine_execution_time_in_millis

    @property
    def query_queue_time_in_millis(self) -> Optional[int]:
        return self._query_queue_time_in_millis

    @property
    def total_execution_time_in_millis(self) -> Optional[int]:
        return self._total_execution_time_in_millis

    @property
    def query_planning_time_in_millis(self) -> Optional[int]:
        return self._query_planning_time_in_millis

    @property
    def service_processing_time_in_millis(self) -> Optional[int]:
        return self._service_processing_time_in_millis

    @property
    def output_location(self) -> Optional[str]:
        return self._output_location

    @property
    def data_manifest_location(self) -> Optional[str]:
        return self._data_manifest_location

    @property
    def reused_previous_result(self) -> Optional[bool]:
        return self._reused_previous_result

    @property
    def encryption_option(self) -> Optional[str]:
        return self._encryption_option

    @property
    def kms_key(self) -> Optional[str]:
        return self._kms_key

    @property
    def expected_bucket_owner(self) -> Optional[str]:
        return self._expected_bucket_owner

    @property
    def s3_acl_option(self) -> Optional[str]:
        return self._s3_acl_option

    @property
    def selected_engine_version(self) -> Optional[str]:
        return self._selected_engine_version

    @property
    def effective_engine_version(self) -> Optional[str]:
        return self._effective_engine_version

    @property
    def result_reuse_enabled(self) -> Optional[bool]:
        return self._result_reuse_enabled

    @property
    def result_reuse_minutes(self) -> Optional[int]:
        return self._result_reuse_minutes


class AthenaDatabase:
    def __init__(self, response):
        database = response.get("Database", None)
        if not database:
            raise DataError("KeyError `Database`")

        self._name: Optional[str] = database.get("Name", None)
        self._description: Optional[str] = database.get("Description", None)
        self._parameters: Dict[str, str] = database.get("Parameters", {})

    @property
    def name(self) -> Optional[str]:
        return self._name

    @property
    def description(self) -> Optional[str]:
        return self._description

    @property
    def parameters(self) -> Dict[str, str]:
        return self._parameters


class AthenaTableMetadataColumn:
    def __init__(self, response):
        self._name: Optional[str] = response.get("Name", None)
        self._type: Optional[str] = response.get("Type", None)
        self._comment: Optional[str] = response.get("Comment", None)

    @property
    def name(self) -> Optional[str]:
        return self._name

    @property
    def type(self) -> Optional[str]:
        return self._type

    @property
    def comment(self) -> Optional[str]:
        return self._comment


class AthenaTableMetadataPartitionKey:
    def __init__(self, response):
        self._name: Optional[str] = response.get("Name", None)
        self._type: Optional[str] = response.get("Type", None)
        self._comment: Optional[str] = response.get("Comment", None)

    @property
    def name(self) -> Optional[str]:
        return self._name

    @property
    def type(self) -> Optional[str]:
        return self._type

    @property
    def comment(self) -> Optional[str]:
        return self._comment


class AthenaTableMetadata:
    def __init__(self, response):
        table_metadata = response.get("TableMetadata", None)
        if not table_metadata:
            raise DataError("KeyError `TableMetadata`")

        self._name: Optional[str] = table_metadata.get("Name", None)
        self._create_time: Optional[datetime] = table_metadata.get("CreateTime", None)
        self._last_access_time: Optional[datetime] = table_metadata.get("LastAccessTime", None)
        self._table_type: Optional[str] = table_metadata.get("TableType", None)

        columns = table_metadata.get("Columns", [])
        self._columns: List[AthenaTableMetadataColumn] = []
        for column in columns:
            self._columns.append(AthenaTableMetadataColumn(column))

        partition_keys = table_metadata.get("PartitionKeys", [])
        self._partition_keys: List[AthenaTableMetadataPartitionKey] = []
        for key in partition_keys:
            self._partition_keys.append(AthenaTableMetadataPartitionKey(key))

        self._parameters: Dict[str, str] = table_metadata.get("Parameters", {})

    @property
    def name(self) -> Optional[str]:
        return self._name

    @property
    def create_time(self) -> Optional[datetime]:
        return self._create_time

    @property
    def last_access_time(self) -> Optional[datetime]:
        return self._last_access_time

    @property
    def table_type(self) -> Optional[str]:
        return self._table_type

    @property
    def columns(self) -> List[AthenaTableMetadataColumn]:
        return self._columns

    @property
    def partition_keys(self) -> List[AthenaTableMetadataPartitionKey]:
        return self._partition_keys

    @property
    def parameters(self) -> Dict[str, str]:
        return self._parameters

    @property
    def comment(self) -> Optional[str]:
        return self._parameters.get("comment", None)

    @property
    def location(self) -> Optional[str]:
        return self._parameters.get("location", None)

    @property
    def input_format(self) -> Optional[str]:
        return self._parameters.get("inputformat", None)

    @property
    def output_format(self) -> Optional[str]:
        return self._parameters.get("outputformat", None)

    @property
    def row_format(self) -> Optional[str]:
        serde = self.serde_serialization_lib
        if serde:
            return f"SERDE '{serde}'"
        return None

    @property
    def file_format(self) -> Optional[str]:
        input = self.input_format
        output = self.output_format
        if input and output:
            return f"INPUTFORMAT '{input}' OUTPUTFORMAT '{output}'"
        return None

    @property
    def serde_serialization_lib(self) -> Optional[str]:
        return self._parameters.get("serde.serialization.lib", None)

    @property
    def compression(self) -> Optional[str]:
        if "write.compression" in self._parameters:  # text or json
            return self._parameters["write.compression"]
        elif "serde.param.write.compression" in self._parameters:  # text or json
            return self._parameters["serde.param.write.compression"]
        elif "parquet.compress" in self._parameters:  # parquet
            return self._parameters["parquet.compress"]
        elif "orc.compress" in self._parameters:  # orc
            return self._parameters["orc.compress"]
        else:
            return None

    @property
    def serde_properties(self) -> Dict[str, str]:
        return {
            k.replace("serde.param.", ""): v
            for k, v in self._parameters.items()
            if k.startswith("serde.param.")
        }

    @property
    def table_properties(self) -> Dict[str, str]:
        return {k: v for k, v in self._parameters.items() if not k.startswith("serde.param.")}


class AthenaFileFormat:
    FILE_FORMAT_SEQUENCEFILE: str = "SEQUENCEFILE"
    FILE_FORMAT_TEXTFILE: str = "TEXTFILE"
    FILE_FORMAT_RCFILE: str = "RCFILE"
    FILE_FORMAT_ORC: str = "ORC"
    FILE_FORMAT_PARQUET: str = "PARQUET"
    FILE_FORMAT_AVRO: str = "AVRO"
    FILE_FORMAT_ION: str = "ION"

    @staticmethod
    def is_parquet(value: str) -> bool:
        return value.upper() == AthenaFileFormat.FILE_FORMAT_PARQUET

    @staticmethod
    def is_orc(value: str) -> bool:
        return value.upper() == AthenaFileFormat.FILE_FORMAT_ORC


class AthenaRowFormatSerde:
    PATTERN_ROW_FORMAT_SERDE: Pattern[str] = re.compile(r"^(?i:serde) '(?P<serde>.+)'$")

    ROW_FORMAT_SERDE_CSV: str = "org.apache.hadoop.hive.serde2.OpenCSVSerde"
    ROW_FORMAT_SERDE_REGEX: str = "org.apache.hadoop.hive.serde2.RegexSerDe"
    ROW_FORMAT_SERDE_LAZY_SIMPLE: str = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"
    ROW_FORMAT_SERDE_CLOUD_TRAIL: str = "com.amazon.emr.hive.serde.CloudTrailSerde"
    ROW_FORMAT_SERDE_GROK: str = "com.amazonaws.glue.serde.GrokSerDe"
    ROW_FORMAT_SERDE_JSON: str = "org.openx.data.jsonserde.JsonSerDe"
    ROW_FORMAT_SERDE_JSON_HCATALOG: str = "org.apache.hive.hcatalog.data.JsonSerDe"
    ROW_FORMAT_SERDE_PARQUET: str = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    ROW_FORMAT_SERDE_ORC: str = "org.apache.hadoop.hive.ql.io.orc.OrcSerde"
    ROW_FORMAT_SERDE_AVRO: str = "org.apache.hadoop.hive.serde2.avro.AvroSerDe"

    @staticmethod
    def is_parquet(value: str) -> bool:
        match = AthenaRowFormatSerde.PATTERN_ROW_FORMAT_SERDE.search(value)
        if match:
            serde = match.group("serde")
            if serde == AthenaRowFormatSerde.ROW_FORMAT_SERDE_PARQUET:
                return True
        return False

    @staticmethod
    def is_orc(value: str) -> bool:
        match = AthenaRowFormatSerde.PATTERN_ROW_FORMAT_SERDE.search(value)
        if match:
            serde = match.group("serde")
            if serde == AthenaRowFormatSerde.ROW_FORMAT_SERDE_ORC:
                return True
        return False


class AthenaCompression:
    COMPRESSION_BZIP2: str = "BZIP2"
    COMPRESSION_DEFLATE: str = "DEFLATE"
    COMPRESSION_GZIP: str = "GZIP"
    COMPRESSION_LZ4: str = "LZ4"
    COMPRESSION_LZO: str = "LZO"
    COMPRESSION_SNAPPY: str = "SNAPPY"
    COMPRESSION_ZLIB: str = "ZLIB"
    COMPRESSION_ZSTD: str = "ZSTD"

    @staticmethod
    def is_valid(value: str) -> bool:
        return value.upper() in [
            AthenaCompression.COMPRESSION_BZIP2,
            AthenaCompression.COMPRESSION_DEFLATE,
            AthenaCompression.COMPRESSION_GZIP,
            AthenaCompression.COMPRESSION_LZ4,
            AthenaCompression.COMPRESSION_LZO,
            AthenaCompression.COMPRESSION_SNAPPY,
            AthenaCompression.COMPRESSION_ZLIB,
            AthenaCompression.COMPRESSION_ZSTD,
        ]
