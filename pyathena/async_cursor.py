# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals
import logging
import os

from concurrent.futures.thread import ThreadPoolExecutor

from pyathena.cursor import BaseCursor
from pyathena.model import AthenaResultSet


_logger = logging.getLogger(__name__)


class AsyncCursor(BaseCursor):

    def __init__(self, client, s3_staging_dir, schema_name, poll_interval,
                 encryption_option, kms_key, converter, formatter,
                 retry_exceptions, retry_attempt, retry_multiplier,
                 retry_max_delay, retry_exponential_base,
                 max_workers=(os.cpu_count() or 1) * 5):
        super(AsyncCursor, self).__init__(client, s3_staging_dir, schema_name, poll_interval,
                                          encryption_option, kms_key, converter, formatter,
                                          retry_exceptions, retry_attempt, retry_multiplier,
                                          retry_max_delay, retry_exponential_base)
        self._executor = ThreadPoolExecutor(max_workers=max_workers)

    def close(self, wait=False):
        self._executor.shutdown(wait=wait)

    def _description(self, query_id):
        result_set = self._collect_result_set(query_id)
        return [
            (
                m.get('Name', None),
                m.get('Type', None),
                None,
                None,
                m.get('Precision', None),
                m.get('Scale', None),
                m.get('Nullable', None)
            )
            for m in result_set.meta_data
        ]

    def description(self, query_id):
        return self._executor.submit(self._description, query_id)

    def query_execution(self, query_id):
        return self._executor.submit(self._query_execution, query_id)

    def poll(self, query_id):
        return self._executor.submit(self._poll, query_id)

    def _collect_result_set(self, query_id):
        query_execution = self._poll(query_id)
        return AthenaResultSet(
            self._connection, self._converter, query_execution, self.arraysize,
            self.retry_exceptions, self.retry_attempt, self.retry_multiplier,
            self.retry_max_delay, self.retry_exponential_base)

    def execute(self, operation, parameters=None):
        query_id = self._execute(operation, parameters)
        return query_id, self._executor.submit(self._collect_result_set, query_id)

    def cancel(self, query_id):
        return self._executor.submit(self._cancel, query_id)
