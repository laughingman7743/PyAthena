# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import logging

from pyathena.common import BaseCursor, CursorIterator
from pyathena.error import NotSupportedError, OperationalError, ProgrammingError
from pyathena.model import AthenaQueryExecution
from pyathena.result_set import AthenaResultSet
from pyathena.util import synchronized

_logger = logging.getLogger(__name__)


class Cursor(BaseCursor, CursorIterator):

    def __init__(self, client, s3_staging_dir, schema_name, poll_interval,
                 encryption_option, kms_key, converter, formatter,
                 retry_exceptions, retry_attempt, retry_multiplier,
                 retry_max_delay, retry_exponential_base):
        super(Cursor, self).__init__(client, s3_staging_dir, schema_name, poll_interval,
                                     encryption_option, kms_key, converter, formatter,
                                     retry_exceptions, retry_attempt, retry_multiplier,
                                     retry_max_delay, retry_exponential_base)
        self._query_id = None
        self._result_set = None

    @property
    def rownumber(self):
        return self._result_set.rownumber if self._result_set else None

    @property
    def has_result_set(self):
        return self._result_set is not None

    @property
    def description(self):
        if not self.has_result_set:
            return None
        return self._result_set.description

    @property
    def query_id(self):
        return self._query_id

    @property
    def query(self):
        if not self.has_result_set:
            return None
        return self._result_set.query

    @property
    def state(self):
        if not self.has_result_set:
            return None
        return self._result_set.state

    @property
    def state_change_reason(self):
        if not self.has_result_set:
            return None
        return self._result_set.state_change_reason

    @property
    def completion_date_time(self):
        if not self.has_result_set:
            return None
        return self._result_set.completion_date_time

    @property
    def submission_date_time(self):
        if not self.has_result_set:
            return None
        return self._result_set.submission_date_time

    @property
    def data_scanned_in_bytes(self):
        if not self.has_result_set:
            return None
        return self._result_set.data_scanned_in_bytes

    @property
    def execution_time_in_millis(self):
        if not self.has_result_set:
            return None
        return self._result_set.execution_time_in_millis

    @property
    def output_location(self):
        if not self.has_result_set:
            return None
        return self._result_set.output_location

    def close(self):
        if self._result_set and not self._result_set.is_closed:
            self._result_set.close()

    def _reset_state(self):
        self._description = None
        self._query_id = None
        if self._result_set and not self._result_set.is_closed:
            self._result_set.close()
        self._result_set = None

    @synchronized
    def execute(self, operation, parameters=None):
        self._reset_state()
        self._query_id = self._execute(operation, parameters)
        query_execution = self._poll(self._query_id)
        if query_execution.state == AthenaQueryExecution.STATE_SUCCEEDED:
            self._result_set = AthenaResultSet(
                self._connection, self._converter, query_execution, self.arraysize,
                self.retry_exceptions, self.retry_attempt, self.retry_multiplier,
                self.retry_max_delay, self.retry_exponential_base)
        else:
            raise OperationalError(query_execution.state_change_reason)

    def executemany(self, operation, seq_of_parameters):
        raise NotSupportedError

    @synchronized
    def cancel(self):
        if not self._query_id:
            raise ProgrammingError('QueryExecutionId is none or empty.')
        self._cancel(self._query_id)

    @synchronized
    def fetchone(self):
        if not self.has_result_set:
            raise ProgrammingError('No result set.')
        return self._result_set.fetchone()

    @synchronized
    def fetchmany(self, size=None):
        if not self.has_result_set:
            raise ProgrammingError('No result set.')
        return self._result_set.fetchmany(size)

    @synchronized
    def fetchall(self):
        if not self.has_result_set:
            raise ProgrammingError('No result set.')
        return self._result_set.fetchall()
