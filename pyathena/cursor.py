# -*- coding: utf-8 -*-
import logging
from typing import TYPE_CHECKING, Any, Dict, List, Optional, cast

from pyathena.common import BaseCursor, CursorIterator
from pyathena.converter import Converter
from pyathena.error import OperationalError, ProgrammingError
from pyathena.formatter import Formatter
from pyathena.model import AthenaQueryExecution
from pyathena.result_set import AthenaDictResultSet, AthenaResultSet, WithResultSet
from pyathena.util import RetryConfig, synchronized

if TYPE_CHECKING:
    from pyathena.connection import Connection

_logger = logging.getLogger(__name__)  # type: ignore


class Cursor(BaseCursor, CursorIterator, WithResultSet):
    def __init__(
        self,
        connection: "Connection",
        s3_staging_dir: str,
        poll_interval: float,
        encryption_option: str,
        kms_key: str,
        converter: Converter,
        formatter: Formatter,
        retry_config: RetryConfig,
        schema_name: Optional[str] = None,
        catalog_name: Optional[str] = None,
        work_group: Optional[str] = None,
        kill_on_interrupt: bool = True,
        **kwargs
    ) -> None:
        super(Cursor, self).__init__(
            connection=connection,
            s3_staging_dir=s3_staging_dir,
            poll_interval=poll_interval,
            encryption_option=encryption_option,
            kms_key=kms_key,
            converter=converter,
            formatter=formatter,
            retry_config=retry_config,
            schema_name=schema_name,
            catalog_name=catalog_name,
            work_group=work_group,
            kill_on_interrupt=kill_on_interrupt,
            **kwargs
        )
        self._query_id: Optional[str] = None
        self._result_set: Optional[AthenaResultSet] = None
        self._result_set_class = AthenaResultSet

    @property
    def result_set(self) -> Optional[AthenaResultSet]:
        return self._result_set

    @result_set.setter
    def result_set(self, val) -> None:
        self._result_set = val

    @property
    def query_id(self) -> Optional[str]:
        return self._query_id

    @query_id.setter
    def query_id(self, val) -> None:
        self._query_id = val

    @property
    def rownumber(self) -> Optional[int]:
        return self.result_set.rownumber if self.result_set else None

    def close(self) -> None:
        if self.result_set and not self.result_set.is_closed:
            self.result_set.close()

    @synchronized
    def execute(
        self,
        operation: str,
        parameters: Optional[Dict[str, Any]] = None,
        work_group: Optional[str] = None,
        s3_staging_dir: Optional[str] = None,
        cache_size: int = 0,
        cache_expiration_time: int = 0,
    ):
        self._reset_state()
        self.query_id = self._execute(
            operation,
            parameters=parameters,
            work_group=work_group,
            s3_staging_dir=s3_staging_dir,
            cache_size=cache_size,
            cache_expiration_time=cache_expiration_time,
        )
        query_execution = self._poll(self.query_id)
        if query_execution.state == AthenaQueryExecution.STATE_SUCCEEDED:
            self.result_set = self._result_set_class(
                self._connection,
                self._converter,
                query_execution,
                self.arraysize,
                self._retry_config,
            )
        else:
            raise OperationalError(query_execution.state_change_reason)
        return self

    def executemany(
        self, operation: str, seq_of_parameters: List[Optional[Dict[str, Any]]]
    ):
        for parameters in seq_of_parameters:
            self.execute(operation, parameters)
        # Operations that have result sets are not allowed with executemany.
        self._reset_state()

    @synchronized
    def cancel(self) -> None:
        if not self.query_id:
            raise ProgrammingError("QueryExecutionId is none or empty.")
        self._cancel(self.query_id)

    @synchronized
    def fetchone(self):
        if not self.has_result_set:
            raise ProgrammingError("No result set.")
        result_set = cast(AthenaResultSet, self.result_set)
        return result_set.fetchone()

    @synchronized
    def fetchmany(self, size: int = None):
        if not self.has_result_set:
            raise ProgrammingError("No result set.")
        result_set = cast(AthenaResultSet, self.result_set)
        return result_set.fetchmany(size)

    @synchronized
    def fetchall(self):
        if not self.has_result_set:
            raise ProgrammingError("No result set.")
        result_set = cast(AthenaResultSet, self.result_set)
        return result_set.fetchall()


class DictCursor(Cursor):
    def __init__(self, **kwargs) -> None:
        super(DictCursor, self).__init__(**kwargs)
        self._result_set_class = AthenaDictResultSet
        if "dict_type" in kwargs:
            AthenaDictResultSet.dict_type = kwargs["dict_type"]
