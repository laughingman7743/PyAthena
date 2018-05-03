# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import logging

from pyathena.error import DataError

_logger = logging.getLogger(__name__)


class AthenaQueryExecution(object):

    STATE_QUEUED = 'QUEUED'
    STATE_RUNNING = 'RUNNING'
    STATE_SUCCEEDED = 'SUCCEEDED'
    STATE_FAILED = 'FAILED'
    STATE_CANCELLED = 'CANCELLED'

    def __init__(self, response):
        query_execution = response.get('QueryExecution', None)
        if not query_execution:
            raise DataError('KeyError `QueryExecution`')

        self._query_id = query_execution.get('QueryExecutionId', None)
        if not self._query_id:
            raise DataError('KeyError `QueryExecutionId`')

        self._query = query_execution.get('Query', None)
        if not self._query:
            raise DataError('KeyError `Query`')

        status = query_execution.get('Status', None)
        if not status:
            raise DataError('KeyError `Status`')
        self._state = status.get('State', None)
        self._state_change_reason = status.get('StateChangeReason', None)
        self._completion_date_time = status.get('CompletionDateTime', None)
        self._submission_date_time = status.get('SubmissionDateTime', None)

        statistics = query_execution.get('Statistics', {})
        self._data_scanned_in_bytes = statistics.get('DataScannedInBytes', None)
        self._execution_time_in_millis = statistics.get('EngineExecutionTimeInMillis', None)

        result_conf = query_execution.get('ResultConfiguration', {})
        self._output_location = result_conf.get('OutputLocation', None)

    @property
    def query_id(self):
        return self._query_id

    @property
    def query(self):
        return self._query

    @property
    def state(self):
        return self._state

    @property
    def state_change_reason(self):
        return self._state_change_reason

    @property
    def completion_date_time(self):
        return self._completion_date_time

    @property
    def submission_date_time(self):
        return self._submission_date_time

    @property
    def data_scanned_in_bytes(self):
        return self._data_scanned_in_bytes

    @property
    def execution_time_in_millis(self):
        return self._execution_time_in_millis

    @property
    def output_location(self):
        return self._output_location
