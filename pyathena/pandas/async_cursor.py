# -*- coding: utf-8 -*-
import logging
from multiprocessing import cpu_count

from pyathena.async_cursor import AsyncCursor
from pyathena.common import CursorIterator
from pyathena.pandas.result_set import AthenaPandasResultSet

_logger = logging.getLogger(__name__)


class AsyncPandasCursor(AsyncCursor):
    def __init__(
        self,
        connection,
        s3_staging_dir,
        schema_name,
        work_group,
        poll_interval,
        encryption_option,
        kms_key,
        converter,
        formatter,
        retry_config,
        max_workers=(cpu_count() or 1) * 5,
        arraysize=CursorIterator.DEFAULT_FETCH_SIZE,
        kill_on_interrupt=True,
    ):
        super(AsyncPandasCursor, self).__init__(
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
            max_workers=max_workers,
            arraysize=arraysize,
            kill_on_interrupt=kill_on_interrupt,
        )

    def _collect_result_set(
        self,
        query_id,
        keep_default_na=False,
        na_values=None,
        quoting=1,
    ):
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
        )

    def execute(
        self,
        operation,
        parameters=None,
        work_group=None,
        s3_staging_dir=None,
        cache_size=0,
        keep_default_na=False,
        na_values=None,
        quoting=1,
    ):
        query_id = self._execute(
            operation,
            parameters=parameters,
            work_group=work_group,
            s3_staging_dir=s3_staging_dir,
            cache_size=cache_size,
        )
        return (
            query_id,
            self._executor.submit(
                self._collect_result_set, query_id, keep_default_na, na_values, quoting
            ),
        )
