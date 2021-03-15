# -*- coding: utf-8 -*-
import logging
from concurrent.futures import Future
from concurrent.futures.thread import ThreadPoolExecutor
from multiprocessing import cpu_count
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple, Union

from pyathena.common import CursorIterator
from pyathena.converter import Converter
from pyathena.cursor import BaseCursor
from pyathena.error import NotSupportedError, ProgrammingError
from pyathena.formatter import Formatter
from pyathena.model import AthenaQueryExecution
from pyathena.result_set import AthenaDictResultSet, AthenaResultSet
from pyathena.util import RetryConfig

if TYPE_CHECKING:
    from pyathena.connection import Connection
    from pyathena.pandas.result_set import AthenaPandasResultSet

_logger = logging.getLogger(__name__)  # type: ignore


class AsyncCursor(BaseCursor):
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
        max_workers: int = (cpu_count() or 1) * 5,
        arraysize: int = CursorIterator.DEFAULT_FETCH_SIZE,
        kill_on_interrupt: bool = True,
    ) -> None:
        super(AsyncCursor, self).__init__(
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
        )
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        self._arraysize = arraysize
        self._result_set_class = AthenaResultSet

    @property
    def arraysize(self) -> int:
        return self._arraysize

    @arraysize.setter
    def arraysize(self, value: int) -> None:
        if value <= 0 or value > CursorIterator.DEFAULT_FETCH_SIZE:
            raise ProgrammingError(
                "MaxResults is more than maximum allowed length {0}.".format(
                    CursorIterator.DEFAULT_FETCH_SIZE
                )
            )
        self._arraysize = value

    def close(self, wait: bool = False) -> None:
        self._executor.shutdown(wait=wait)

    def _description(
        self, query_id: str
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
        result_set = self._collect_result_set(query_id)
        return result_set.description

    def description(
        self, query_id: str
    ) -> "Future[\
        Optional[\
            List[\
                Tuple[\
                    Optional[Any],\
                    Optional[Any],\
                    None,\
                    None,\
                    Optional[Any],\
                    Optional[Any],\
                    Optional[Any],\
                ]\
            ]\
        ]\
    ]":
        return self._executor.submit(self._description, query_id)

    def query_execution(self, query_id: str) -> "Future[AthenaQueryExecution]":
        return self._executor.submit(self._get_query_execution, query_id)

    def poll(self, query_id: str) -> "Future[AthenaQueryExecution]":
        return self._executor.submit(self._poll, query_id)

    def _collect_result_set(self, query_id: str) -> AthenaResultSet:
        query_execution = self._poll(query_id)
        return self._result_set_class(
            connection=self._connection,
            converter=self._converter,
            query_execution=query_execution,
            arraysize=self._arraysize,
            retry_config=self._retry_config,
        )

    def execute(
        self,
        operation: str,
        parameters: Optional[Dict[str, Any]] = None,
        work_group: Optional[str] = None,
        s3_staging_dir: Optional[str] = None,
        cache_size: int = 0,
        cache_expiration_time: int = 0,
    ) -> Tuple[str, "Future[Union[AthenaResultSet, AthenaPandasResultSet]]"]:
        query_id = self._execute(
            operation,
            parameters=parameters,
            work_group=work_group,
            s3_staging_dir=s3_staging_dir,
            cache_size=cache_size,
            cache_expiration_time=cache_expiration_time,
        )
        return query_id, self._executor.submit(self._collect_result_set, query_id)

    def executemany(
        self, operation: str, seq_of_parameters: List[Optional[Dict[str, Any]]]
    ):
        raise NotSupportedError

    def cancel(self, query_id: str) -> "Future[None]":
        return self._executor.submit(self._cancel, query_id)


class AsyncDictCursor(AsyncCursor):
    def __init__(self, **kwargs) -> None:
        super(AsyncDictCursor, self).__init__(**kwargs)
        self._result_set_class = AthenaDictResultSet
        if "dict_type" in kwargs:
            AthenaDictResultSet.dict_type = kwargs["dict_type"]
