# -*- coding: utf-8 -*-
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from pyathena.error import DataError

_logger = logging.getLogger(__name__)  # type: ignore


class AthenaQueryExecution(object):

    STATE_QUEUED: str = "QUEUED"
    STATE_RUNNING: str = "RUNNING"
    STATE_SUCCEEDED: str = "SUCCEEDED"
    STATE_FAILED: str = "FAILED"
    STATE_CANCELLED: str = "CANCELLED"

    STATEMENT_TYPE_DDL: str = "DDL"
    STATEMENT_TYPE_DML: str = "DML"
    STATEMENT_TYPE_UTILITY: str = "UTILITY"

    def __init__(self, response: Dict[str, Any]) -> None:
        query_execution = response.get("QueryExecution", None)
        if not query_execution:
            raise DataError("KeyError `QueryExecution`")

        query_execution_context = query_execution.get("QueryExecutionContext", {})
        self._database: Optional[str] = query_execution_context.get("Database", None)

        self._query_id: Optional[str] = query_execution.get("QueryExecutionId", None)
        if not self._query_id:
            raise DataError("KeyError `QueryExecutionId`")

        self._query: Optional[str] = query_execution.get("Query", None)
        if not self._query:
            raise DataError("KeyError `Query`")
        self._statement_type: Optional[str] = query_execution.get("StatementType", None)

        status = query_execution.get("Status", None)
        if not status:
            raise DataError("KeyError `Status`")
        self._state: Optional[str] = status.get("State", None)
        self._state_change_reason: Optional[str] = status.get("StateChangeReason", None)
        self._completion_date_time: Optional[datetime] = status.get(
            "CompletionDateTime", None
        )
        self._submission_date_time: Optional[datetime] = status.get(
            "SubmissionDateTime", None
        )

        statistics = query_execution.get("Statistics", {})
        self._data_scanned_in_bytes: Optional[int] = statistics.get(
            "DataScannedInBytes", None
        )
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
        self._data_manifest_location: Optional[str] = statistics.get(
            "DataManifestLocation", None
        )

        result_conf = query_execution.get("ResultConfiguration", {})
        self._output_location: Optional[str] = result_conf.get("OutputLocation", None)

        encryption_conf = result_conf.get("EncryptionConfiguration", {})
        self._encryption_option: Optional[str] = encryption_conf.get(
            "EncryptionOption", None
        )
        self._kms_key: Optional[str] = encryption_conf.get("KmsKey", None)

        self._work_group: Optional[str] = query_execution.get("WorkGroup", None)

    @property
    def database(self) -> Optional[str]:
        return self._database

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
    def state(self) -> Optional[str]:
        return self._state

    @property
    def state_change_reason(self) -> Optional[str]:
        return self._state_change_reason

    @property
    def completion_date_time(self) -> Optional[datetime]:
        return self._completion_date_time

    @property
    def submission_date_time(self) -> Optional[datetime]:
        return self._submission_date_time

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
    def encryption_option(self) -> Optional[str]:
        return self._encryption_option

    @property
    def kms_key(self) -> Optional[str]:
        return self._kms_key

    @property
    def work_group(self) -> Optional[str]:
        return self._work_group


class AthenaTableMetadataColumn(object):
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


class AthenaTableMetadataPartitionKey(object):
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


class AthenaTableMetadata(object):
    def __init__(self, response):
        table_metadata = response.get("TableMetadata", None)
        if not table_metadata:
            raise DataError("KeyError `TableMetadata`")

        self._name: Optional[str] = table_metadata.get("Name", None)
        self._create_time: Optional[datetime] = table_metadata.get("CreateTime", None)
        self._last_access_time: Optional[datetime] = table_metadata.get(
            "LastAccessTime", None
        )
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
    def serde_serialization_lib(self) -> Optional[str]:
        return self._parameters.get("'serde.serialization.lib", None)


class AthenaRowFormat(object):

    ROW_FORMAT_PARQUET: str = "parquet"
    ROW_FORMAT_ORC: str = "orc"
    ROW_FORMAT_CSV: str = "csv"
    ROW_FORMAT_JSON: str = "json"
    ROW_FORMAT_AVRO: str = "avro"

    @staticmethod
    def is_valid(value: Optional[str]) -> bool:
        return value in [
            AthenaRowFormat.ROW_FORMAT_PARQUET,
            AthenaRowFormat.ROW_FORMAT_ORC,
            AthenaRowFormat.ROW_FORMAT_CSV,
            AthenaRowFormat.ROW_FORMAT_JSON,
            AthenaRowFormat.ROW_FORMAT_AVRO,
        ]


class AthenaCompression(object):

    COMPRESSION_SNAPPY: str = "snappy"
    COMPRESSION_ZLIB: str = "zlib"
    COMPRESSION_LZO: str = "lzo"
    COMPRESSION_GZIP: str = "gzip"

    @staticmethod
    def is_valid(value: Optional[str]) -> bool:
        return value in [
            AthenaCompression.COMPRESSION_SNAPPY,
            AthenaCompression.COMPRESSION_ZLIB,
            AthenaCompression.COMPRESSION_LZO,
            AthenaCompression.COMPRESSION_GZIP,
        ]
