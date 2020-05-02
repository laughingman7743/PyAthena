# -*- coding: utf-8 -*-
from __future__ import absolute_import, unicode_literals

import logging
from concurrent.futures.thread import ThreadPoolExecutor

from pyathena import cpu_count
from pyathena.common import CursorIterator
from pyathena.cursor import BaseCursor
from pyathena.error import NotSupportedError, ProgrammingError
from pyathena.result_set import AthenaResultSet

_logger = logging.getLogger(__name__)


class AsyncCursor(BaseCursor):
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
    ):
        super(AsyncCursor, self).__init__(
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
        )
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        self._arraysize = arraysize

    @property
    def arraysize(self):
        return self._arraysize

    @arraysize.setter
    def arraysize(self, value):
        if value <= 0 or value > CursorIterator.DEFAULT_FETCH_SIZE:
            raise ProgrammingError(
                "MaxResults is more than maximum allowed length {0}.".format(
                    CursorIterator.DEFAULT_FETCH_SIZE
                )
            )
        self._arraysize = value

    def close(self, wait=False):
        self._executor.shutdown(wait=wait)

    def _description(self, query_id):
        result_set = self._collect_result_set(query_id)
        return result_set.description

    def description(self, query_id):
        return self._executor.submit(self._description, query_id)

    def query_execution(self, query_id):
        return self._executor.submit(self._get_query_execution, query_id)

    def poll(self, query_id):
        return self._executor.submit(self._poll, query_id)

    def _collect_result_set(self, query_id):
        query_execution = self._poll(query_id)
        return AthenaResultSet(
            connection=self._connection,
            converter=self._converter,
            query_execution=query_execution,
            arraysize=self._arraysize,
            retry_config=self._retry_config,
        )

    def execute(
        self,
        operation,
        parameters=None,
        work_group=None,
        s3_staging_dir=None,
        cache_size=0,
    ):
        query_id = self._execute(
            operation,
            parameters=parameters,
            work_group=work_group,
            s3_staging_dir=s3_staging_dir,
            cache_size=cache_size,
        )
        return query_id, self._executor.submit(self._collect_result_set, query_id)

    def executemany(self, operation, seq_of_parameters):
        raise NotSupportedError

    def cancel(self, query_id):
        return self._executor.submit(self._cancel, query_id)
