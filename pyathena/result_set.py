# -*- coding: utf-8 -*-
import collections
import logging
from abc import abstractmethod
from datetime import datetime
from typing import (
    TYPE_CHECKING,
    Any,
    Deque,
    Dict,
    List,
    Optional,
    Tuple,
    Type,
    Union,
    cast,
)

from pyathena.common import CursorIterator
from pyathena.converter import Converter
from pyathena.error import DataError, OperationalError, ProgrammingError
from pyathena.model import AthenaQueryExecution
from pyathena.util import RetryConfig, retry_api_call

if TYPE_CHECKING:
    from pyathena.connection import Connection

_logger = logging.getLogger(__name__)  # type: ignore


class AthenaResultSet(CursorIterator):
    def __init__(
        self,
        connection: "Connection",
        converter: Converter,
        query_execution: AthenaQueryExecution,
        arraysize: int,
        retry_config: RetryConfig,
    ) -> None:
        super(AthenaResultSet, self).__init__(arraysize=arraysize)
        self._connection: Optional["Connection"] = connection
        self._converter = converter
        self._query_execution: Optional[AthenaQueryExecution] = query_execution
        assert self._query_execution, "Required argument `query_execution` not found."
        self._retry_config = retry_config

        self._meta_data: Optional[Tuple[Any, ...]] = None
        self._rows: Deque[
            Union[Tuple[Optional[Any], ...], Dict[Any, Optional[Any]]]
        ] = collections.deque()
        self._next_token: Optional[str] = None

        if self.state == AthenaQueryExecution.STATE_SUCCEEDED:
            self._rownumber = 0
            self._pre_fetch()

    @property
    def database(self) -> Optional[str]:
        if not self._query_execution:
            return None
        return self._query_execution.database

    @property
    def query_id(self) -> Optional[str]:
        if not self._query_execution:
            return None
        return self._query_execution.query_id

    @property
    def query(self) -> Optional[str]:
        if not self._query_execution:
            return None
        return self._query_execution.query

    @property
    def statement_type(self) -> Optional[str]:
        if not self._query_execution:
            return None
        return self._query_execution.statement_type

    @property
    def state(self) -> Optional[str]:
        if not self._query_execution:
            return None
        return self._query_execution.state

    @property
    def state_change_reason(self) -> Optional[str]:
        if not self._query_execution:
            return None
        return self._query_execution.state_change_reason

    @property
    def completion_date_time(self) -> Optional[datetime]:
        if not self._query_execution:
            return None
        return self._query_execution.completion_date_time

    @property
    def submission_date_time(self) -> Optional[datetime]:
        if not self._query_execution:
            return None
        return self._query_execution.submission_date_time

    @property
    def data_scanned_in_bytes(self) -> Optional[int]:
        if not self._query_execution:
            return None
        return self._query_execution.data_scanned_in_bytes

    @property
    def engine_execution_time_in_millis(self) -> Optional[int]:
        if not self._query_execution:
            return None
        return self._query_execution.engine_execution_time_in_millis

    @property
    def query_queue_time_in_millis(self) -> Optional[int]:
        if not self._query_execution:
            return None
        return self._query_execution.query_queue_time_in_millis

    @property
    def total_execution_time_in_millis(self) -> Optional[int]:
        if not self._query_execution:
            return None
        return self._query_execution.total_execution_time_in_millis

    @property
    def query_planning_time_in_millis(self) -> Optional[int]:
        if not self._query_execution:
            return None
        return self._query_execution.query_planning_time_in_millis

    @property
    def service_processing_time_in_millis(self) -> Optional[int]:
        if not self._query_execution:
            return None
        return self._query_execution.service_processing_time_in_millis

    @property
    def output_location(self) -> Optional[str]:
        if not self._query_execution:
            return None
        return self._query_execution.output_location

    @property
    def data_manifest_location(self) -> Optional[str]:
        if not self._query_execution:
            return None
        return self._query_execution.data_manifest_location

    @property
    def encryption_option(self) -> Optional[str]:
        if not self._query_execution:
            return None
        return self._query_execution.encryption_option

    @property
    def kms_key(self) -> Optional[str]:
        if not self._query_execution:
            return None
        return self._query_execution.kms_key

    @property
    def work_group(self) -> Optional[str]:
        if not self._query_execution:
            return None
        return self._query_execution.work_group

    @property
    def description(
        self,
    ) -> Optional[
        List[
            Tuple[
                Optional[Any],
                Optional[Any],
                None,
                None,
                Optional[Any],
                Optional[Any],
                Optional[Any],
            ]
        ]
    ]:
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

    def __fetch(self, next_token: Optional[str] = None):
        if not self.query_id:
            raise ProgrammingError("QueryExecutionId is none or empty.")
        if self.state != AthenaQueryExecution.STATE_SUCCEEDED:
            raise ProgrammingError("QueryExecutionState is not SUCCEEDED.")
        if self.is_closed:
            raise ProgrammingError("AthenaResultSet is closed.")
        request = {
            "QueryExecutionId": self.query_id,
            "MaxResults": self._arraysize,
        }
        if next_token:
            request.update({"NextToken": next_token})
        try:
            connection = cast("Connection", self._connection)
            response = retry_api_call(
                connection.client.get_query_results,
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
            if self._rownumber is None:
                self._rownumber = 0
            self._rownumber += 1
            return self._rows.popleft()

    def fetchmany(self, size: Optional[int] = None):
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

    def _process_meta_data(self, response: Dict[str, Any]) -> None:
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

    def _get_rows(
        self, offset: int, meta_data: Tuple[Any, ...], rows: List[Dict[str, Any]]
    ) -> List[Union[Tuple[Optional[Any], ...], Dict[Any, Optional[Any]]]]:
        return [
            tuple(
                [
                    self._converter.convert(
                        meta.get("Type", None), row.get("VarCharValue", None)
                    )
                    for meta, row in zip(meta_data, rows[i].get("Data", []))
                ]
            )
            for i in range(offset, len(rows))
        ]

    def _process_rows(self, response: Dict[str, Any]) -> None:
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
            meta_data = cast(Tuple[Any, ...], self._meta_data)
            processed_rows = self._get_rows(offset, meta_data, rows)
        self._rows.extend(processed_rows)
        self._next_token = response.get("NextToken", None)

    def _is_first_row_column_labels(self, rows: List[Dict[str, Any]]) -> bool:
        first_row_data = rows[0].get("Data", [])
        meta_data = cast(Tuple[Any, Any], self._meta_data)
        for meta, data in zip(meta_data, first_row_data):
            if meta.get("Name", None) != data.get("VarCharValue", None):
                return False
        return True

    @property
    def is_closed(self) -> bool:
        return self._connection is None

    def close(self) -> None:
        self._connection = None
        self._query_execution = None
        self._meta_data = None
        self._rows.clear()
        self._next_token = None
        self._rownumber = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class AthenaDictResultSet(AthenaResultSet):

    # You can override this to use OrderedDict or other dict-like types.
    dict_type: Type[Any] = dict

    def _get_rows(
        self, offset: int, meta_data: Tuple[Any, ...], rows: List[Dict[str, Any]]
    ) -> List[Union[Tuple[Optional[Any], ...], Dict[Any, Optional[Any]]]]:
        return [
            self.dict_type(
                [
                    (
                        meta.get("Name"),
                        self._converter.convert(
                            meta.get("Type", None), row.get("VarCharValue", None)
                        ),
                    )
                    for meta, row in zip(meta_data, rows[i].get("Data", []))
                ]
            )
            for i in range(offset, len(rows))
        ]


class WithResultSet(object):
    def __init__(self):
        super(WithResultSet, self).__init__()

    def _reset_state(self) -> None:
        self.query_id = None  # type: ignore
        if self.result_set and not self.result_set.is_closed:
            self.result_set.close()
        self.result_set = None  # type: ignore

    @property  # type: ignore
    @abstractmethod
    def result_set(self) -> Optional[AthenaResultSet]:
        raise NotImplementedError  # pragma: no cover

    @result_set.setter  # type: ignore
    @abstractmethod
    def result_set(self, val: Optional[AthenaResultSet]) -> None:
        raise NotImplementedError  # pragma: no cover

    @property
    def has_result_set(self) -> bool:
        return self.result_set is not None

    @property
    def description(
        self,
    ) -> Optional[
        List[
            Tuple[
                Optional[Any],
                Optional[Any],
                None,
                None,
                Optional[Any],
                Optional[Any],
                Optional[Any],
            ]
        ]
    ]:
        if not self.has_result_set:
            return None
        result_set = cast(AthenaResultSet, self.result_set)
        return result_set.description

    @property
    def database(self) -> Optional[str]:
        if not self.has_result_set:
            return None
        result_set = cast(AthenaResultSet, self.result_set)
        return result_set.database

    @property  # type: ignore
    @abstractmethod
    def query_id(self) -> Optional[str]:
        raise NotImplementedError  # pragma: no cover

    @query_id.setter  # type: ignore
    @abstractmethod
    def query_id(self, val: Optional[str]) -> None:
        raise NotImplementedError  # pragma: no cover

    @property
    def query(self) -> Optional[str]:
        if not self.has_result_set:
            return None
        result_set = cast(AthenaResultSet, self.result_set)
        return result_set.query

    @property
    def statement_type(self) -> Optional[str]:
        if not self.has_result_set:
            return None
        result_set = cast(AthenaResultSet, self.result_set)
        return result_set.statement_type

    @property
    def state(self) -> Optional[str]:
        if not self.has_result_set:
            return None
        result_set = cast(AthenaResultSet, self.result_set)
        return result_set.state

    @property
    def state_change_reason(self) -> Optional[str]:
        if not self.has_result_set:
            return None
        result_set = cast(AthenaResultSet, self.result_set)
        return result_set.state_change_reason

    @property
    def completion_date_time(self) -> Optional[datetime]:
        if not self.has_result_set:
            return None
        result_set = cast(AthenaResultSet, self.result_set)
        return result_set.completion_date_time

    @property
    def submission_date_time(self) -> Optional[datetime]:
        if not self.has_result_set:
            return None
        result_set = cast(AthenaResultSet, self.result_set)
        return result_set.submission_date_time

    @property
    def data_scanned_in_bytes(self) -> Optional[int]:
        if not self.has_result_set:
            return None
        result_set = cast(AthenaResultSet, self.result_set)
        return result_set.data_scanned_in_bytes

    @property
    def engine_execution_time_in_millis(self) -> Optional[int]:
        if not self.has_result_set:
            return None
        result_set = cast(AthenaResultSet, self.result_set)
        return result_set.engine_execution_time_in_millis

    @property
    def query_queue_time_in_millis(self) -> Optional[int]:
        if not self.has_result_set:
            return None
        result_set = cast(AthenaResultSet, self.result_set)
        return result_set.query_queue_time_in_millis

    @property
    def total_execution_time_in_millis(self) -> Optional[int]:
        if not self.has_result_set:
            return None
        result_set = cast(AthenaResultSet, self.result_set)
        return result_set.total_execution_time_in_millis

    @property
    def query_planning_time_in_millis(self) -> Optional[int]:
        if not self.has_result_set:
            return None
        result_set = cast(AthenaResultSet, self.result_set)
        return result_set.query_planning_time_in_millis

    @property
    def service_processing_time_in_millis(self) -> Optional[int]:
        if not self.has_result_set:
            return None
        result_set = cast(AthenaResultSet, self.result_set)
        return result_set.service_processing_time_in_millis

    @property
    def output_location(self) -> Optional[str]:
        if not self.has_result_set:
            return None
        result_set = cast(AthenaResultSet, self.result_set)
        return result_set.output_location

    @property
    def data_manifest_location(self) -> Optional[str]:
        if not self.has_result_set:
            return None
        result_set = cast(AthenaResultSet, self.result_set)
        return result_set.data_manifest_location

    @property
    def encryption_option(self) -> Optional[str]:
        if not self.has_result_set:
            return None
        result_set = cast(AthenaResultSet, self.result_set)
        return result_set.encryption_option

    @property
    def kms_key(self) -> Optional[str]:
        if not self.has_result_set:
            return None
        result_set = cast(AthenaResultSet, self.result_set)
        return result_set.kms_key

    @property
    def work_group(self) -> Optional[str]:
        if not self.has_result_set:
            return None
        result_set = cast(AthenaResultSet, self.result_set)
        return result_set.work_group
