#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import logging

from pyathena.common import CursorIterator
from pyathena.cursor import BaseCursor
from pyathena.error import NotSupportedError, OperationalError, ProgrammingError
from pyathena.model import AthenaQueryExecution
from pyathena.result_set import AthenaPandasResultSet, WithResultSet
from pyathena.util import synchronized

_logger = logging.getLogger(__name__)


class PandasCursor(BaseCursor, CursorIterator, WithResultSet):

    def __init__(self, connection, s3_staging_dir, schema_name, poll_interval,
                 encryption_option, kms_key, converter, formatter,
                 retry_exceptions, retry_attempt, retry_multiplier,
                 retry_max_delay, retry_exponential_base, **kwargs):
        super(PandasCursor, self).__init__(connection, s3_staging_dir, schema_name, poll_interval,
                                           encryption_option, kms_key, converter, formatter,
                                           retry_exceptions, retry_attempt, retry_multiplier,
                                           retry_max_delay, retry_exponential_base, **kwargs)

    @property
    def rownumber(self):
        return self._result_set.rownumber if self._result_set else None

    def close(self):
        if self._result_set and not self._result_set.is_closed:
            self._result_set.close()

    @synchronized
    def execute(self, operation, parameters=None):
        self._reset_state()
        self._query_id = self._execute(operation, parameters)
        query_execution = self._poll(self._query_id)
        if query_execution.state == AthenaQueryExecution.STATE_SUCCEEDED:
            self._result_set = AthenaPandasResultSet(
                self._connection, self._converter, query_execution, self.arraysize,
                self.retry_exceptions, self.retry_attempt, self.retry_multiplier,
                self.retry_max_delay, self.retry_exponential_base)
        else:
            raise OperationalError(query_execution.state_change_reason)
        return self

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

    @synchronized
    def as_pandas(self):
        if not self.has_result_set:
            raise ProgrammingError('No result set.')
        return self._result_set.as_pandas()
