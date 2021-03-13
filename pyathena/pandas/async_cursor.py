# -*- coding: utf-8 -*-
import logging
from concurrent.futures import Future
from multiprocessing import cpu_count
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple, Union

from pyathena.async_cursor import AsyncCursor
from pyathena.common import CursorIterator
from pyathena.converter import Converter
from pyathena.formatter import Formatter
from pyathena.pandas.result_set import AthenaPandasResultSet
from pyathena.util import RetryConfig

if TYPE_CHECKING:
    from pyathena.connection import Connection
    from pyathena.result_set import AthenaResultSet

_logger = logging.getLogger(__name__)  # type: ignore


class AsyncPandasCursor(AsyncCursor):
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
        schema_name: Optional[str],
        catalog_name: Optional[str],
        work_group: Optional[str],
        max_workers: int = (cpu_count() or 1) * 5,
        arraysize: int = CursorIterator.DEFAULT_FETCH_SIZE,
        kill_on_interrupt: bool = True,
    ) -> None:
        super(AsyncPandasCursor, self).__init__(
            connection=connection,
            s3_staging_dir=s3_staging_dir,
            poll_interval=poll_interval,
            encryption_option=encryption_option,
            kms_key=kms_key,
            converter=converter,
            formatter=formatter,
            retry_config=retry_config,
            max_workers=max_workers,
            arraysize=arraysize,
            schema_name=schema_name,
            catalog_name=catalog_name,
            work_group=work_group,
            kill_on_interrupt=kill_on_interrupt,
        )

    def _collect_result_set(
        self,
        query_id: str,
        keep_default_na: bool = False,
        na_values: List[str] = None,
        quoting: int = 1,
        kwargs: Dict[str, Any] = None,
    ) -> AthenaPandasResultSet:
        if kwargs is None:
            kwargs = dict()
        query_execution = self._poll(query_id)
        return AthenaPandasResultSet(
            connection=self._connection,
            converter=self._converter,
            query_execution=query_execution,
            arraysize=self._arraysize,
            retry_config=self._retry_config,
            keep_default_na=keep_default_na,
            na_values=na_values,
            quoting=quoting,
            **kwargs,
        )

    def execute(
        self,
        operation: str,
        parameters: Optional[Dict[str, Any]] = None,
        work_group: Optional[str] = None,
        s3_staging_dir: Optional[str] = None,
        cache_size: int = 0,
        cache_expiration_time: int = 0,
        keep_default_na: bool = False,
        na_values: List[str] = None,
        quoting: int = 1,
        **kwargs,
    ) -> Tuple[str, "Future[Union[AthenaResultSet, AthenaPandasResultSet]]"]:
        query_id = self._execute(
            operation,
            parameters=parameters,
            work_group=work_group,
            s3_staging_dir=s3_staging_dir,
            cache_size=cache_size,
            cache_expiration_time=cache_expiration_time,
        )
        return (
            query_id,
            self._executor.submit(
                self._collect_result_set,
                query_id,
                keep_default_na,
                na_values,
                quoting,
                kwargs,
            ),
        )
