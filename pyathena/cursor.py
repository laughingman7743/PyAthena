# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals
import logging

from pyathena.common import BaseCursor
from pyathena.error import OperationalError, ProgrammingError
from pyathena.model import AthenaResultSet
from pyathena.util import synchronized


_logger = logging.getLogger(__name__)


class Cursor(BaseCursor):

    def __init__(self, client, s3_staging_dir, schema_name, poll_interval,
                 encryption_option, kms_key, converter, formatter,
                 retry_exceptions, retry_attempt, retry_multiplier,
                 retry_max_delay, retry_exponential_base):
        super(Cursor, self).__init__(client, s3_staging_dir, schema_name, poll_interval,
                                     encryption_option, kms_key, converter, formatter,
                                     retry_exceptions, retry_attempt, retry_multiplier,
                                     retry_max_delay, retry_exponential_base)
        self._description = None
        self._query_id = None
        self._meta_data = None
        self._result_set = None

    @property
    def rownumber(self):
        return self._result_set.rownumber if self._result_set else None

    @property
    def rowcount(self):
        """By default, return -1 to indicate that this is not supported."""
        return -1

    @property
    def has_result_set(self):
        return self._result_set and self._meta_data is not None

    @property
    def description(self):
        if self._description or self._description == []:
            return self._description
        if not self.has_result_set:
            return None
        self._description = [
            (
                m.get('Name', None),
                m.get('Type', None),
                None,
                None,
                m.get('Precision', None),
                m.get('Scale', None),
                m.get('Nullable', None)
            )
            for m in self._meta_data
        ]
        return self._description

    @property
    def query_id(self):
        return self._query_id

    @property
    def output_location(self):
        if not self.has_result_set:
            return None
        return self._result_set.query_execution.output_location

    @property
    def completion_date_time(self):
        if not self.has_result_set:
            return None
        return self._result_set.query_execution.completion_date_time

    @property
    def submission_date_time(self):
        if not self.has_result_set:
            return None
        return self._result_set.query_execution.submission_date_time

    @property
    def data_scanned_in_bytes(self):
        if not self.has_result_set:
            return None
        return self._result_set.query_execution.data_scanned_in_bytes

    @property
    def execution_time_in_millis(self):
        if not self.has_result_set:
            return None
        return self._result_set.query_execution.execution_time_in_millis

    def close(self):
        pass

    def _reset_state(self):
        self._description = None
        self._query_id = None
        self._result_set = None
        self._meta_data = None

    @synchronized
    def execute(self, operation, parameters=None):
        self._reset_state()
        self._query_id = self._execute(operation, parameters)
        query_execution = self._poll(self._query_id)
        if query_execution.state == 'SUCCEEDED':
            self._result_set = AthenaResultSet(
                self._connection, self._converter, query_execution, self.arraysize,
                self.retry_exceptions, self.retry_attempt, self.retry_multiplier,
                self.retry_max_delay, self.retry_exponential_base)
            self._meta_data = self._result_set.meta_data
        else:
            raise OperationalError(query_execution.state_change_reason)

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

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __next__(self):
        row = self.fetchone()
        if row is None:
            raise StopIteration
        else:
            return row

    next = __next__

    def __iter__(self):
        return self
