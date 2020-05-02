# -*- coding: utf-8 -*-
from __future__ import absolute_import, unicode_literals

import logging

from pyathena import cpu_count
from pyathena.async_cursor import AsyncCursor
from pyathena.common import CursorIterator
from pyathena.result_set import AthenaPandasResultSet

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
        )

    def _collect_result_set(self, query_id):
        query_execution = self._poll(query_id)
        return AthenaPandasResultSet(
            connection=self._connection,
            converter=self._converter,
            query_execution=query_execution,
            arraysize=self._arraysize,
            retry_config=self._retry_config,
        )
