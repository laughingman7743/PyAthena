# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals
import collections
import logging

from future.utils import raise_from
from past.builtins.misc import xrange

from pyathena.error import DataError, OperationalError, ProgrammingError
from pyathena.util import retry_api_call


_logger = logging.getLogger(__name__)


class AthenaQueryExecution(object):

    def __init__(self, response):
        query_execution = response.get('QueryExecution', None)
        if not query_execution:
            raise DataError('KeyError `QueryExecution`')

        self.query_id = query_execution.get('QueryExecutionId', None)
        if not self.query_id:
            raise DataError('KeyError `QueryExecutionId`')

        self.query = query_execution.get('Query', None)
        if not self.query:
            raise DataError('KeyError `Query`')

        status = query_execution.get('Status', None)
        if not status:
            raise DataError('KeyError `Status`')
        self.state = status.get('State', None)
        self.state_change_reason = status.get('StateChangeReason', None)
        self.completion_date_time = status.get('CompletionDateTime', None)
        self.submission_date_time = status.get('SubmissionDateTime', None)

        statistics = query_execution.get('Statistics', {})
        self.data_scanned_in_bytes = statistics.get('DataScannedInBytes', None)
        self.execution_time_in_millis = statistics.get('EngineExecutionTimeInMillis', None)

        result_conf = query_execution.get('ResultConfiguration', {})
        self.output_location = result_conf.get('OutputLocation', None)


class AthenaResultSet(object):

    def __init__(self, connection, converter, query_execution, arraysize,
                 retry_exceptions, retry_attempt, retry_multiplier,
                 retry_max_delay, retry_exponential_base):
        self._connection = connection
        self._converter = converter
        self._query_execution = query_execution
        assert self._query_execution, 'Required argument `query_execution` not found.'
        self._arraysize = arraysize

        self.retry_exceptions = retry_exceptions
        self.retry_attempt = retry_attempt
        self.retry_multiplier = retry_multiplier
        self.retry_max_delay = retry_max_delay
        self.retry_exponential_base = retry_exponential_base

        self._meta_data = None
        self._rows = collections.deque()
        self._next_token = None
        self._rownumber = 0

        if self._query_execution.state == 'SUCCEEDED':
            self._pre_fetch()

    @property
    def meta_data(self):
        return self._meta_data

    @property
    def query_execution(self):
        return self._query_execution

    @property
    def rownumber(self):
        return self._rownumber

    def __fetch(self, next_token=None):
        if self._query_execution.state != 'SUCCEEDED':
            raise ProgrammingError('QueryExecutionState is not SUCCEEDED.')
        if not self._query_execution.query_id:
            raise ProgrammingError('QueryExecutionId is none or empty.')
        request = {
            'QueryExecutionId': self._query_execution.query_id,
            'MaxResults': self._arraysize,
        }
        if next_token:
            request.update({'NextToken': next_token})
        try:
            response = retry_api_call(self._connection.get_query_results,
                                      exceptions=self.retry_exceptions,
                                      attempt=self.retry_attempt,
                                      multiplier=self.retry_multiplier,
                                      max_delay=self.retry_max_delay,
                                      exp_base=self.retry_exponential_base,
                                      logger=_logger,
                                      **request)
        except Exception as e:
            _logger.exception('Failed to fetch result set.')
            raise_from(OperationalError(*e.args), e)
        else:
            return response

    def _fetch(self):
        if not self._next_token:
            raise ProgrammingError('NextToken is none or empty.')
        response = self.__fetch(self._next_token)
        self._process_rows(response)

    def _pre_fetch(self):
        response = self.__fetch(None)
        self._process_meta_data(response)
        self._process_rows(response)

    def fetchone(self):
        if not self._query_execution.query_id:
            raise ProgrammingError('QueryExecutionId is none or empty.')
        if not self._rows and self._next_token:
            self._fetch()
        if not self._rows:
            return None
        else:
            self._rownumber += 1
            return self._rows.popleft()

    def fetchmany(self, size=None):
        if not self._query_execution.query_id:
            raise ProgrammingError('QueryExecutionId is none or empty.')
        if not size or size <= 0:
            size = self._arraysize
        rows = []
        for _ in xrange(size):
            row = self.fetchone()
            if row:
                rows.append(row)
            else:
                break
        return rows

    def fetchall(self):
        if not self._query_execution.query_id:
            raise ProgrammingError('QueryExecutionId is none or empty.')
        rows = []
        while True:
            row = self.fetchone()
            if row:
                rows.append(row)
            else:
                break
        return rows

    def _process_meta_data(self, response):
        result_set = response.get('ResultSet', None)
        if not result_set:
            raise DataError('KeyError `ResultSet`')
        meta_data = result_set.get('ResultSetMetadata', None)
        if not meta_data:
            raise DataError('KeyError `ResultSetMetadata`')
        column_info = meta_data.get('ColumnInfo', None)
        if column_info is None:
            raise DataError('KeyError `ColumnInfo`')
        self._meta_data = tuple(column_info)

    def _process_rows(self, response):
        result_set = response.get('ResultSet', None)
        if not result_set:
            raise DataError('KeyError `ResultSet`')
        rows = result_set.get('Rows', None)
        if rows is None:
            raise DataError('KeyError `Rows`')
        processed_rows = []
        if len(rows) > 0:
            offset = 1 if not self._next_token and self._is_first_row_column_labels(rows) else 0
            processed_rows = [
                tuple([self._converter.convert(meta.get('Type', None),
                                               row.get('VarCharValue', None))
                       for meta, row in zip(self._meta_data, rows[i].get('Data', []))])
                for i in xrange(offset, len(rows))
            ]
        self._rows.extend(processed_rows)
        self._next_token = response.get('NextToken', None)

    def _is_first_row_column_labels(self, rows):
        first_row_data = rows[0].get('Data', [])
        for meta, data in zip(self._meta_data, first_row_data):
            if meta.get('Name', None) != data.get('VarCharValue', None):
                return False
        return True

    @property
    def is_closed(self):
        return self._connection is None

    def close(self):
        self._connection = None
        self._query_execution = None
        self._meta_data = None
        self._rows = None
        self._next_token = None
        self._rownumber = 0

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
