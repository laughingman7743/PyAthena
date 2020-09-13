# -*- coding: utf-8 -*-
import collections
import logging

from pyathena.common import CursorIterator
from pyathena.error import DataError, OperationalError, ProgrammingError
from pyathena.model import AthenaQueryExecution
from pyathena.util import retry_api_call

_logger = logging.getLogger(__name__)


class WithResultSet(object):
    def __init__(self):
        super(WithResultSet, self).__init__()
        self._query_id = None
        self._result_set = None

    def _reset_state(self):
        self._query_id = None
        if self._result_set and not self._result_set.is_closed:
            self._result_set.close()
        self._result_set = None

    @property
    def has_result_set(self):
        return self._result_set is not None

    @property
    def description(self):
        if not self.has_result_set:
            return None
        return self._result_set.description

    @property
    def database(self):
        if not self.has_result_set:
            return None
        return self._result_set.database

    @property
    def query_id(self):
        return self._query_id

    @property
    def query(self):
        if not self.has_result_set:
            return None
        return self._result_set.query

    @property
    def statement_type(self):
        if not self.has_result_set:
            return None
        return self._result_set.statement_type

    @property
    def state(self):
        if not self.has_result_set:
            return None
        return self._result_set.state

    @property
    def state_change_reason(self):
        if not self.has_result_set:
            return None
        return self._result_set.state_change_reason

    @property
    def completion_date_time(self):
        if not self.has_result_set:
            return None
        return self._result_set.completion_date_time

    @property
    def submission_date_time(self):
        if not self.has_result_set:
            return None
        return self._result_set.submission_date_time

    @property
    def data_scanned_in_bytes(self):
        if not self.has_result_set:
            return None
        return self._result_set.data_scanned_in_bytes

    @property
    def execution_time_in_millis(self):
        if not self.has_result_set:
            return None
        return self._result_set.execution_time_in_millis

    @property
    def engine_execution_time_in_millis(self):
        if not self.has_result_set:
            return None
        return self._result_set.engine_execution_time_in_millis

    @property
    def query_queue_time_in_millis(self):
        if not self.has_result_set:
            return None
        return self._result_set.query_queue_time_in_millis

    @property
    def total_execution_time_in_millis(self):
        if not self.has_result_set:
            return None
        return self._result_set.total_execution_time_in_millis

    @property
    def query_planning_time_in_millis(self):
        if not self.has_result_set:
            return None
        return self._result_set.query_planning_time_in_millis

    @property
    def service_processing_time_in_millis(self):
        if not self.has_result_set:
            return None
        return self._result_set.service_processing_time_in_millis

    @property
    def output_location(self):
        if not self.has_result_set:
            return None
        return self._result_set.output_location

    @property
    def data_manifest_location(self):
        if not self.has_result_set:
            return None
        return self._result_set.data_manifest_location

    @property
    def encryption_option(self):
        if not self.has_result_set:
            return None
        return self._result_set.encryption_option

    @property
    def kms_key(self):
        if not self.has_result_set:
            return None
        return self._result_set.kms_key

    @property
    def work_group(self):
        if not self.has_result_set:
            return None
        return self._result_set.work_group


class AthenaResultSet(CursorIterator):
    def __init__(self, connection, converter, query_execution, arraysize, retry_config):
        super(AthenaResultSet, self).__init__(arraysize=arraysize)
        self._connection = connection
        self._converter = converter
        self._query_execution = query_execution
        assert self._query_execution, "Required argument `query_execution` not found."
        self._retry_config = retry_config

        self._meta_data = None
        self._rows = collections.deque()
        self._next_token = None

        if self.state == AthenaQueryExecution.STATE_SUCCEEDED:
            self._rownumber = 0
            self._pre_fetch()

    @property
    def database(self):
        return self._query_execution.database

    @property
    def query_id(self):
        return self._query_execution.query_id

    @property
    def query(self):
        return self._query_execution.query

    @property
    def statement_type(self):
        return self._query_execution.statement_type

    @property
    def state(self):
        return self._query_execution.state

    @property
    def state_change_reason(self):
        return self._query_execution.state_change_reason

    @property
    def completion_date_time(self):
        return self._query_execution.completion_date_time

    @property
    def submission_date_time(self):
        return self._query_execution.submission_date_time

    @property
    def data_scanned_in_bytes(self):
        return self._query_execution.data_scanned_in_bytes

    @property
    def execution_time_in_millis(self):
        return self._query_execution.execution_time_in_millis

    @property
    def engine_execution_time_in_millis(self):
        return self._query_execution.engine_execution_time_in_millis

    @property
    def query_queue_time_in_millis(self):
        return self._query_execution.query_queue_time_in_millis

    @property
    def total_execution_time_in_millis(self):
        return self._query_execution.total_execution_time_in_millis

    @property
    def query_planning_time_in_millis(self):
        return self._query_execution.query_planning_time_in_millis

    @property
    def service_processing_time_in_millis(self):
        return self._query_execution.service_processing_time_in_millis

    @property
    def output_location(self):
        return self._query_execution.output_location

    @property
    def data_manifest_location(self):
        return self._query_execution.data_manifest_location

    @property
    def encryption_option(self):
        return self._query_execution.encryption_option

    @property
    def kms_key(self):
        return self._query_execution.kms_key

    @property
    def work_group(self):
        return self._query_execution.work_group

    @property
    def description(self):
        if self._meta_data is None:
            return None
        return [
            (
                m.get("Name", None),
                m.get("Type", None),
                None,
                None,
                m.get("Precision", None),
                m.get("Scale", None),
                m.get("Nullable", None),
            )
            for m in self._meta_data
        ]

    def __fetch(self, next_token=None):
        if not self.query_id:
            raise ProgrammingError("QueryExecutionId is none or empty.")
        if self.state != AthenaQueryExecution.STATE_SUCCEEDED:
            raise ProgrammingError("QueryExecutionState is not SUCCEEDED.")
        request = {
            "QueryExecutionId": self.query_id,
            "MaxResults": self._arraysize,
        }
        if next_token:
            request.update({"NextToken": next_token})
        try:
            response = retry_api_call(
                self._connection.client.get_query_results,
                config=self._retry_config,
                logger=_logger,
                **request
            )
        except Exception as e:
            _logger.exception("Failed to fetch result set.")
            raise OperationalError(*e.args) from e
        else:
            return response

    def _fetch(self):
        if not self._next_token:
            raise ProgrammingError("NextToken is none or empty.")
        response = self.__fetch(self._next_token)
        self._process_rows(response)

    def _pre_fetch(self):
        response = self.__fetch()
        self._process_meta_data(response)
        self._process_rows(response)

    def fetchone(self):
        if not self._rows and self._next_token:
            self._fetch()
        if not self._rows:
            return None
        else:
            self._rownumber += 1
            return self._rows.popleft()

    def fetchmany(self, size=None):
        if not size or size <= 0:
            size = self._arraysize
        rows = []
        for _ in range(size):
            row = self.fetchone()
            if row:
                rows.append(row)
            else:
                break
        return rows

    def fetchall(self):
        rows = []
        while True:
            row = self.fetchone()
            if row:
                rows.append(row)
            else:
                break
        return rows

    def _process_meta_data(self, response):
        result_set = response.get("ResultSet", None)
        if not result_set:
            raise DataError("KeyError `ResultSet`")
        meta_data = result_set.get("ResultSetMetadata", None)
        if not meta_data:
            raise DataError("KeyError `ResultSetMetadata`")
        column_info = meta_data.get("ColumnInfo", None)
        if column_info is None:
            raise DataError("KeyError `ColumnInfo`")
        self._meta_data = tuple(column_info)

    def _process_rows(self, response):
        result_set = response.get("ResultSet", None)
        if not result_set:
            raise DataError("KeyError `ResultSet`")
        rows = result_set.get("Rows", None)
        if rows is None:
            raise DataError("KeyError `Rows`")
        processed_rows = []
        if len(rows) > 0:
            offset = (
                1
                if not self._next_token and self._is_first_row_column_labels(rows)
                else 0
            )
            processed_rows = [
                tuple(
                    [
                        self._converter.convert(
                            meta.get("Type", None), row.get("VarCharValue", None)
                        )
                        for meta, row in zip(self._meta_data, rows[i].get("Data", []))
                    ]
                )
                for i in range(offset, len(rows))
            ]
        self._rows.extend(processed_rows)
        self._next_token = response.get("NextToken", None)

    def _is_first_row_column_labels(self, rows):
        first_row_data = rows[0].get("Data", [])
        for meta, data in zip(self._meta_data, first_row_data):
            if meta.get("Name", None) != data.get("VarCharValue", None):
                return False
        return True

    @property
    def is_closed(self):
        return self._connection is None

    def close(self):
        self._connection = None
        self._query_execution = None
        self._meta_data = None
        self._rows = None
        self._next_token = None
        self._rownumber = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
