# -*- coding: utf-8 -*-
import logging
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from pyathena.common import BaseCursor, CursorIterator
from pyathena.converter import Converter
from pyathena.error import OperationalError, ProgrammingError
from pyathena.formatter import Formatter
from pyathena.model import AthenaQueryExecution
from pyathena.result_set import AthenaResultSet, WithResultSet
from pyathena.util import RetryConfig, synchronized

if TYPE_CHECKING:
    from pyathena.connection import Connection

_logger = logging.getLogger(__name__)  # type: ignore


class Cursor(BaseCursor, CursorIterator, WithResultSet):
    def __init__(
        self,
        connection: "Connection",
        s3_staging_dir: str,
        schema_name: str,
        work_group: str,
        poll_interval: float,
        encryption_option: str,
        kms_key: str,
        converter: Converter,
        formatter: Formatter,
        retry_config: RetryConfig,
        kill_on_interrupt: bool = True,
        **kwargs
    ) -> None:
        super(Cursor, self).__init__(
            connection=connection,
            s3_staging_dir=s3_staging_dir,
            schema_name=schema_name,
            work_group=work_group,
            poll_interval=poll_interval,
            encryption_option=encryption_option,
            kms_key=kms_key,
            converter=converter,
            formatter=formatter,
            retry_config=retry_config,
            kill_on_interrupt=kill_on_interrupt,
            **kwargs
        )

    @property
    def rownumber(self) -> Optional[int]:
        return self._result_set.rownumber if self._result_set else None

    def close(self) -> None:
        if self._result_set and not self._result_set.is_closed:
            self._result_set.close()

    @synchronized
    def execute(
        self,
        operation: str,
        parameters: Dict[str, Any] = None,
        work_group: Optional[str] = None,
        s3_staging_dir: Optional[str] = None,
        cache_size: int = 0,
    ):
        self._reset_state()
        self._query_id = self._execute(
            operation,
            parameters=parameters,
            work_group=work_group,
            s3_staging_dir=s3_staging_dir,
            cache_size=cache_size,
        )
        query_execution = self._poll(self._query_id)
        if query_execution.state == AthenaQueryExecution.STATE_SUCCEEDED:
            self._result_set = AthenaResultSet(
                self._connection,
                self._converter,
                query_execution,
                self.arraysize,
                self._retry_config,
            )
        else:
            raise OperationalError(query_execution.state_change_reason)
        return self

    def executemany(self, operation: str, seq_of_parameters: List[Dict[str, Any]]):
        for parameters in seq_of_parameters:
            self.execute(operation, parameters)
        # Operations that have result sets are not allowed with executemany.
        self._reset_state()

    @synchronized
    def cancel(self) -> None:
        if not self._query_id:
            raise ProgrammingError("QueryExecutionId is none or empty.")
        self._cancel(self._query_id)

    @synchronized
    def fetchone(self):
        if not self.has_result_set:
            raise ProgrammingError("No result set.")
        return self._result_set.fetchone()

    @synchronized
    def fetchmany(self, size: int = None):
        if not self.has_result_set:
            raise ProgrammingError("No result set.")
        return self._result_set.fetchmany(size)

    @synchronized
    def fetchall(self):
        if not self.has_result_set:
            raise ProgrammingError("No result set.")
        return self._result_set.fetchall()
