# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import logging
from concurrent.futures.thread import ThreadPoolExecutor

from pyathena.common import CursorIterator
from pyathena.cursor import BaseCursor
from pyathena.error import NotSupportedError, ProgrammingError
from pyathena.result_set import AthenaResultSet

try:
    from multiprocessing import cpu_count
except ImportError:
    def cpu_count():
        return None


_logger = logging.getLogger(__name__)


class AsyncCursor(BaseCursor):

    def __init__(self, connection, s3_staging_dir, schema_name, poll_interval,
                 encryption_option, kms_key, converter, formatter,
                 retry_exceptions, retry_attempt, retry_multiplier,
                 retry_max_delay, retry_exponential_base,
                 max_workers=(cpu_count() or 1) * 5,
                 arraysize=CursorIterator.DEFAULT_FETCH_SIZE):
        super(AsyncCursor, self).__init__(connection, s3_staging_dir, schema_name, poll_interval,
                                          encryption_option, kms_key, converter, formatter,
                                          retry_exceptions, retry_attempt, retry_multiplier,
                                          retry_max_delay, retry_exponential_base)
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        self._arraysize = arraysize

    @property
    def arraysize(self):
        return self._arraysize

    @arraysize.setter
    def arraysize(self, value):
        if value <= 0 or value > CursorIterator.DEFAULT_FETCH_SIZE:
            raise ProgrammingError('MaxResults is more than maximum allowed length {0}.'.format(
                CursorIterator.DEFAULT_FETCH_SIZE))
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
            self._connection, self._converter, query_execution, self._arraysize,
            self.retry_exceptions, self.retry_attempt, self.retry_multiplier,
            self.retry_max_delay, self.retry_exponential_base)

    def execute(self, operation, parameters=None):
        query_id = self._execute(operation, parameters)
        return query_id, self._executor.submit(self._collect_result_set, query_id)

    def executemany(self, operation, seq_of_parameters):
        raise NotSupportedError

    def cancel(self, query_id):
        return self._executor.submit(self._cancel, query_id)
