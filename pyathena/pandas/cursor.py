# -*- coding: utf-8 -*-
import logging
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional, cast

from pyathena.common import CursorIterator
from pyathena.converter import Converter
from pyathena.cursor import BaseCursor
from pyathena.error import OperationalError, ProgrammingError
from pyathena.formatter import Formatter
from pyathena.model import AthenaQueryExecution
from pyathena.pandas.result_set import AthenaPandasResultSet
from pyathena.result_set import WithResultSet
from pyathena.util import RetryConfig, synchronized

if TYPE_CHECKING:
    from pandas import DataFrame

    from pyathena.connection import Connection

_logger = logging.getLogger(__name__)  # type: ignore


class PandasCursor(BaseCursor, CursorIterator, WithResultSet):
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
        **kwargs,
    ) -> None:
        super(PandasCursor, self).__init__(
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
            **kwargs,
        )
        self._query_id: Optional[str] = None
        self._result_set: Optional[AthenaPandasResultSet] = None

    @property
    def result_set(self) -> Optional[AthenaPandasResultSet]:
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
        keep_default_na: bool = False,
        na_values: Optional[Iterable[str]] = ("",),
        quoting: int = 1,
        **kwargs,
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
            self.result_set = AthenaPandasResultSet(
                connection=self._connection,
                converter=self._converter,
                query_execution=query_execution,
                arraysize=self.arraysize,
                retry_config=self._retry_config,
                keep_default_na=keep_default_na,
                na_values=na_values,
                quoting=quoting,
                **kwargs,
            )
        else:
            raise OperationalError(query_execution.state_change_reason)
        return self

    def executemany(
        self, operation: str, seq_of_parameters: List[Optional[Dict[str, Any]]]
    ) -> None:
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
        result_set = cast(AthenaPandasResultSet, self.result_set)
        return result_set.fetchone()

    @synchronized
    def fetchmany(self, size: Optional[int] = None):
        if not self.has_result_set:
            raise ProgrammingError("No result set.")
        result_set = cast(AthenaPandasResultSet, self.result_set)
        return result_set.fetchmany(size)

    @synchronized
    def fetchall(self):
        if not self.has_result_set:
            raise ProgrammingError("No result set.")
        result_set = cast(AthenaPandasResultSet, self.result_set)
        return result_set.fetchall()

    @synchronized
    def as_pandas(self) -> "DataFrame":
        if not self.has_result_set:
            raise ProgrammingError("No result set.")
        result_set = cast(AthenaPandasResultSet, self.result_set)
        return result_set.as_pandas()
