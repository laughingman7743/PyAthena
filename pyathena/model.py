# -*- coding: utf-8 -*-
from __future__ import absolute_import, unicode_literals

import logging

from pyathena.error import DataError

_logger = logging.getLogger(__name__)


class AthenaQueryExecution(object):

    STATE_QUEUED = "QUEUED"
    STATE_RUNNING = "RUNNING"
    STATE_SUCCEEDED = "SUCCEEDED"
    STATE_FAILED = "FAILED"
    STATE_CANCELLED = "CANCELLED"

    STATEMENT_TYPE_DDL = "DDL"
    STATEMENT_TYPE_DML = "DML"
    STATEMENT_TYPE_UTILITY = "UTILITY"

    def __init__(self, response):
        query_execution = response.get("QueryExecution", None)
        if not query_execution:
            raise DataError("KeyError `QueryExecution`")

        query_execution_context = query_execution.get("QueryExecutionContext", {})
        self._database = query_execution_context.get("Database", None)

        self._query_id = query_execution.get("QueryExecutionId", None)
        if not self._query_id:
            raise DataError("KeyError `QueryExecutionId`")

        self._query = query_execution.get("Query", None)
        if not self._query:
            raise DataError("KeyError `Query`")
        self._statement_type = query_execution.get("StatementType", None)

        status = query_execution.get("Status", None)
        if not status:
            raise DataError("KeyError `Status`")
        self._state = status.get("State", None)
        self._state_change_reason = status.get("StateChangeReason", None)
        self._completion_date_time = status.get("CompletionDateTime", None)
        self._submission_date_time = status.get("SubmissionDateTime", None)

        statistics = query_execution.get("Statistics", {})
        self._data_scanned_in_bytes = statistics.get("DataScannedInBytes", None)
        self._engine_execution_time_in_millis = statistics.get(
            "EngineExecutionTimeInMillis", None
        )
        self._query_queue_time_in_millis = statistics.get(
            "QueryQueueTimeInMillis", None
        )
        self._total_execution_time_in_millis = statistics.get(
            "TotalExecutionTimeInMillis", None
        )
        self._query_planning_time_in_millis = statistics.get(
            "QueryPlanningTimeInMillis", None
        )
        self._service_processing_time_in_millis = statistics.get(
            "ServiceProcessingTimeInMillis", None
        )
        self._data_manifest_location = statistics.get("DataManifestLocation", None)

        result_conf = query_execution.get("ResultConfiguration", {})
        self._output_location = result_conf.get("OutputLocation", None)

        encryption_conf = result_conf.get("EncryptionConfiguration", {})
        self._encryption_option = encryption_conf.get("EncryptionOption", None)
        self._kms_key = encryption_conf.get("KmsKey", None)

        self._work_group = query_execution.get("WorkGroup", None)

    @property
    def database(self):
        return self._database

    @property
    def query_id(self):
        return self._query_id

    @property
    def query(self):
        return self._query

    @property
    def statement_type(self):
        return self._statement_type

    @property
    def state(self):
        return self._state

    @property
    def state_change_reason(self):
        return self._state_change_reason

    @property
    def completion_date_time(self):
        return self._completion_date_time

    @property
    def submission_date_time(self):
        return self._submission_date_time

    @property
    def data_scanned_in_bytes(self):
        return self._data_scanned_in_bytes

    @property
    def execution_time_in_millis(self):
        return self.engine_execution_time_in_millis

    @property
    def engine_execution_time_in_millis(self):
        return self._engine_execution_time_in_millis

    @property
    def query_queue_time_in_millis(self):
        return self._query_queue_time_in_millis

    @property
    def total_execution_time_in_millis(self):
        return self._total_execution_time_in_millis

    @property
    def query_planning_time_in_millis(self):
        return self._query_planning_time_in_millis

    @property
    def service_processing_time_in_millis(self):
        return self._service_processing_time_in_millis

    @property
    def output_location(self):
        return self._output_location

    @property
    def data_manifest_location(self):
        return self._data_manifest_location

    @property
    def encryption_option(self):
        return self._encryption_option

    @property
    def kms_key(self):
        return self._kms_key

    @property
    def work_group(self):
        return self._work_group


class AthenaRowFormat(object):

    ROW_FORMAT_PARQUET = "parquet"
    ROW_FORMAT_ORC = "orc"
    ROW_FORMAT_CSV = "csv"
    ROW_FORMAT_JSON = "json"
    ROW_FORMAT_AVRO = "avro"

    @staticmethod
    def is_valid(value):
        return value in [
            AthenaRowFormat.ROW_FORMAT_PARQUET,
            AthenaRowFormat.ROW_FORMAT_ORC,
            AthenaRowFormat.ROW_FORMAT_CSV,
            AthenaRowFormat.ROW_FORMAT_JSON,
            AthenaRowFormat.ROW_FORMAT_AVRO,
        ]


class AthenaCompression(object):

    COMPRESSION_SNAPPY = "snappy"
    COMPRESSION_ZLIB = "zlib"
    COMPRESSION_LZO = "lzo"
    COMPRESSION_GZIP = "gzip"

    @staticmethod
    def is_valid(value):
        return value in [
            AthenaCompression.COMPRESSION_SNAPPY,
            AthenaCompression.COMPRESSION_ZLIB,
            AthenaCompression.COMPRESSION_LZO,
            AthenaCompression.COMPRESSION_GZIP,
        ]
