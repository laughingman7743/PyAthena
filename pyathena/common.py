# -*- coding: utf-8 -*-
import logging
import sys
import time
from abc import ABCMeta, abstractmethod
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

from pyathena.converter import Converter
from pyathena.error import DatabaseError, OperationalError, ProgrammingError
from pyathena.formatter import Formatter
from pyathena.model import AthenaQueryExecution, AthenaTableMetadata
from pyathena.util import RetryConfig, retry_api_call

if TYPE_CHECKING:
    from pyathena.connection import Connection

_logger = logging.getLogger(__name__)  # type: ignore


class CursorIterator(object, metaclass=ABCMeta):

    DEFAULT_FETCH_SIZE: int = 1000

    def __init__(self, **kwargs) -> None:
        super(CursorIterator, self).__init__()
        self.arraysize: int = kwargs.get("arraysize", self.DEFAULT_FETCH_SIZE)
        self._rownumber: Optional[int] = None

    @property
    def arraysize(self) -> int:
        return self._arraysize

    @arraysize.setter
    def arraysize(self, value: int) -> None:
        if value <= 0 or value > self.DEFAULT_FETCH_SIZE:
            raise ProgrammingError(
                "MaxResults is more than maximum allowed length {0}.".format(
                    self.DEFAULT_FETCH_SIZE
                )
            )
        self._arraysize = value

    @property
    def rownumber(self) -> Optional[int]:
        return self._rownumber

    @property
    def rowcount(self) -> int:
        """By default, return -1 to indicate that this is not supported."""
        return -1

    @abstractmethod
    def fetchone(self):
        raise NotImplementedError  # pragma: no cover

    @abstractmethod
    def fetchmany(self):
        raise NotImplementedError  # pragma: no cover

    @abstractmethod
    def fetchall(self):
        raise NotImplementedError  # pragma: no cover

    def __next__(self):
        row = self.fetchone()
        if row is None:
            raise StopIteration
        else:
            return row

    next = __next__

    def __iter__(self):
        return self


class BaseCursor(object, metaclass=ABCMeta):

    # https://docs.aws.amazon.com/athena/latest/APIReference/API_ListQueryExecutions.html
    # Valid Range: Minimum value of 0. Maximum value of 50.
    LIST_QUERY_EXECUTIONS_MAX_RESULTS = 50
    # https://docs.aws.amazon.com/athena/latest/APIReference/API_ListTableMetadata.html
    # Valid Range: Minimum value of 1. Maximum value of 50.
    LIST_TABLE_METADATA_MAX_RESULTS = 50

    def __init__(
        self,
        connection: "Connection",
        s3_staging_dir: Optional[str],
        schema_name: Optional[str],
        catalog_name: Optional[str],
        work_group: Optional[str],
        poll_interval: float,
        encryption_option: Optional[str],
        kms_key: Optional[str],
        converter: Converter,
        formatter: Formatter,
        retry_config: RetryConfig,
        kill_on_interrupt: bool,
        **kwargs
    ) -> None:
        super(BaseCursor, self).__init__()
        self._connection = connection
        self._s3_staging_dir = s3_staging_dir
        self._schema_name = schema_name
        self._catalog_name = catalog_name
        self._work_group = work_group
        self._poll_interval = poll_interval
        self._encryption_option = encryption_option
        self._kms_key = kms_key
        self._converter = converter
        self._formatter = formatter
        self._retry_config = retry_config
        self._kill_on_interrupt = kill_on_interrupt

    @property
    def connection(self) -> "Connection":
        return self._connection

    def _get_query_execution(self, query_id: str) -> AthenaQueryExecution:
        request = {"QueryExecutionId": query_id}
        try:
            response = retry_api_call(
                self._connection.client.get_query_execution,
                config=self._retry_config,
                logger=_logger,
                **request
            )
        except Exception as e:
            _logger.exception("Failed to get query execution.")
            raise OperationalError(*e.args) from e
        else:
            return AthenaQueryExecution(response)

    def _get_table_metadata(
        self,
        table_name: str,
        catalog_name: Optional[str] = None,
        schema_name: Optional[str] = None,
    ):
        request = {
            "CatalogName": catalog_name if catalog_name else self._catalog_name,
            "DatabaseName": schema_name if schema_name else self._schema_name,
            "TableName": table_name,
        }
        try:
            response = retry_api_call(
                self._connection.client.get_table_metadata,
                config=self._retry_config,
                logger=_logger,
                **request
            )
        except Exception as e:
            _logger.exception("Failed to get table metadata.")
            raise OperationalError(*e.args) from e
        else:
            return AthenaTableMetadata(response)

    def _batch_get_query_execution(
        self, query_ids: List[str]
    ) -> List[AthenaQueryExecution]:
        try:
            response = retry_api_call(
                self.connection._client.batch_get_query_execution,
                config=self._retry_config,
                logger=_logger,
                QueryExecutionIds=query_ids,
            )
        except Exception as e:
            _logger.exception("Failed to batch get query execution.")
            raise OperationalError(*e.args) from e
        else:
            return [
                AthenaQueryExecution({"QueryExecution": r})
                for r in response.get("QueryExecutions", [])
            ]

    def _list_query_executions(
        self,
        max_results: Optional[int] = None,
        work_group: Optional[str] = None,
        next_token: Optional[str] = None,
    ) -> Tuple[Optional[str], List[AthenaQueryExecution]]:
        request = self._build_list_query_executions_request(
            max_results=max_results, work_group=work_group, next_token=next_token
        )
        try:
            response = retry_api_call(
                self.connection._client.list_query_executions,
                config=self._retry_config,
                logger=_logger,
                **request
            )
        except Exception as e:
            _logger.exception("Failed to list query executions.")
            raise OperationalError(*e.args) from e
        else:
            next_token = response.get("NextToken", None)
            query_ids = response.get("QueryExecutionIds", None)
            if not query_ids:
                return next_token, []
            return next_token, self._batch_get_query_execution(query_ids)

    def _list_table_metadata(
        self,
        max_results: Optional[int] = None,
        catalog_name: Optional[str] = None,
        schema_name: Optional[str] = None,
        expression: Optional[str] = None,
        next_token: Optional[str] = None,
    ):
        request = self._build_list_table_metadata_request(
            max_results=max_results,
            catalog_name=catalog_name,
            schema_name=schema_name,
            expression=expression,
            next_token=next_token,
        )
        try:
            response = retry_api_call(
                self.connection._client.list_table_metadata,
                config=self._retry_config,
                logger=_logger,
                **request
            )
        except Exception as e:
            _logger.exception("Failed to list table metadata.")
            raise OperationalError(*e.args) from e
        else:
            return response.get("NextToken", None), [
                AthenaTableMetadata({"TableMetadata": r})
                for r in response.get("TableMetadataList", [])
            ]

    def __poll(self, query_id: str) -> AthenaQueryExecution:
        while True:
            query_execution = self._get_query_execution(query_id)
            if query_execution.state in [
                AthenaQueryExecution.STATE_SUCCEEDED,
                AthenaQueryExecution.STATE_FAILED,
                AthenaQueryExecution.STATE_CANCELLED,
            ]:
                return query_execution
            else:
                time.sleep(self._poll_interval)

    def _poll(self, query_id: str) -> AthenaQueryExecution:
        try:
            query_execution = self.__poll(query_id)
        except KeyboardInterrupt as e:
            if self._kill_on_interrupt:
                _logger.warning("Query canceled by user.")
                self._cancel(query_id)
                query_execution = self.__poll(query_id)
            else:
                raise e
        return query_execution

    def _build_start_query_execution_request(
        self,
        query: str,
        work_group: Optional[str] = None,
        s3_staging_dir: Optional[str] = None,
    ) -> Dict[str, Any]:
        request: Dict[str, Any] = {
            "QueryString": query,
            "QueryExecutionContext": {},
            "ResultConfiguration": {},
        }
        if self._schema_name:
            request["QueryExecutionContext"].update({"Database": self._schema_name})
        if self._catalog_name:
            request["QueryExecutionContext"].update({"Catalog": self._catalog_name})
        if self._s3_staging_dir or s3_staging_dir:
            request["ResultConfiguration"].update(
                {
                    "OutputLocation": s3_staging_dir
                    if s3_staging_dir
                    else self._s3_staging_dir
                }
            )
        if self._work_group or work_group:
            request.update(
                {"WorkGroup": work_group if work_group else self._work_group}
            )
        if self._encryption_option:
            enc_conf = {
                "EncryptionOption": self._encryption_option,
            }
            if self._kms_key:
                enc_conf.update({"KmsKey": self._kms_key})
            request["ResultConfiguration"].update({"EncryptionConfiguration": enc_conf})
        return request

    def _build_list_query_executions_request(
        self,
        max_results: Optional[int],
        work_group: Optional[str],
        next_token: Optional[str] = None,
    ) -> Dict[str, Any]:
        request: Dict[str, Any] = {
            "MaxResults": max_results
            if max_results
            else self.LIST_QUERY_EXECUTIONS_MAX_RESULTS
        }
        if self._work_group or work_group:
            request.update(
                {"WorkGroup": work_group if work_group else self._work_group}
            )
        if next_token:
            request.update({"NextToken": next_token})
        return request

    def _build_list_table_metadata_request(
        self,
        max_results: Optional[int],
        catalog_name: Optional[str],
        schema_name: Optional[str],
        expression: Optional[str] = None,
        next_token: Optional[str] = None,
    ) -> Dict[str, Any]:
        request: Dict[str, Any] = {
            "MaxResults": max_results
            if max_results
            else self.LIST_TABLE_METADATA_MAX_RESULTS,
            "CatalogName": catalog_name if catalog_name else self._catalog_name,
            "DatabaseName": schema_name if schema_name else self._schema_name,
        }
        if expression:
            request.update({"Expression": expression})
        if next_token:
            request.update({"NextToken": next_token})
        return request

    def _find_previous_query_id(
        self,
        query: str,
        work_group: Optional[str],
        cache_size: int = 0,
        cache_expiration_time: int = 0,
    ) -> Optional[str]:
        query_id = None
        if cache_size == 0 and cache_expiration_time > 0:
            cache_size = sys.maxsize
        if cache_expiration_time > 0:
            expiration_time = datetime.now(timezone.utc) - timedelta(
                seconds=cache_expiration_time
            )
        else:
            expiration_time = datetime.now(timezone.utc)
        try:
            next_token = None
            while cache_size > 0:
                max_results = min(cache_size, self.LIST_QUERY_EXECUTIONS_MAX_RESULTS)
                cache_size -= max_results
                next_token, query_executions = self._list_query_executions(
                    max_results, work_group, next_token=next_token
                )
                for execution in sorted(
                    (
                        e
                        for e in query_executions
                        if e.state == AthenaQueryExecution.STATE_SUCCEEDED
                        and e.statement_type == AthenaQueryExecution.STATEMENT_TYPE_DML
                    ),
                    # https://github.com/python/mypy/issues/9656
                    key=lambda e: e.completion_date_time,  # type: ignore
                    reverse=True,
                ):
                    if (
                        cache_expiration_time > 0
                        and execution.completion_date_time
                        and execution.completion_date_time.astimezone(timezone.utc)
                        < expiration_time
                    ):
                        next_token = None
                        break
                    if execution.query == query:
                        query_id = execution.query_id
                        break
                if query_id or next_token is None:
                    break
        except Exception:
            _logger.warning(
                "Failed to check the cache. Moving on without cache.", exc_info=True
            )
        return query_id

    def _execute(
        self,
        operation: str,
        parameters: Optional[Dict[str, Any]] = None,
        work_group: Optional[str] = None,
        s3_staging_dir: Optional[str] = None,
        cache_size: int = 0,
        cache_expiration_time: int = 0,
    ) -> str:
        query = self._formatter.format(operation, parameters)
        _logger.debug(query)

        request = self._build_start_query_execution_request(
            query, work_group, s3_staging_dir
        )
        query_id = self._find_previous_query_id(
            query,
            work_group,
            cache_size=cache_size,
            cache_expiration_time=cache_expiration_time,
        )
        if query_id is None:
            try:
                query_id = retry_api_call(
                    self._connection.client.start_query_execution,
                    config=self._retry_config,
                    logger=_logger,
                    **request
                ).get("QueryExecutionId", None)
            except Exception as e:
                _logger.exception("Failed to execute query.")
                raise DatabaseError(*e.args) from e
        return query_id

    @abstractmethod
    def execute(
        self,
        operation: str,
        parameters: Optional[Dict[str, Any]] = None,
        work_group: Optional[str] = None,
        s3_staging_dir: Optional[str] = None,
        cache_size: int = 0,
        cache_expiration_time: int = 0,
    ):
        raise NotImplementedError  # pragma: no cover

    @abstractmethod
    def executemany(
        self, operation: str, seq_of_parameters: List[Optional[Dict[str, Any]]]
    ):
        raise NotImplementedError  # pragma: no cover

    @abstractmethod
    def close(self) -> None:
        raise NotImplementedError  # pragma: no cover

    def _cancel(self, query_id: str) -> None:
        request = {"QueryExecutionId": query_id}
        try:
            retry_api_call(
                self._connection.client.stop_query_execution,
                config=self._retry_config,
                logger=_logger,
                **request
            )
        except Exception as e:
            _logger.exception("Failed to cancel query.")
            raise OperationalError(*e.args) from e

    def setinputsizes(self, sizes):
        """Does nothing by default"""
        pass

    def setoutputsize(self, size, column=None):
        """Does nothing by default"""
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
