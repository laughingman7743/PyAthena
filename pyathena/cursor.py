# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals
import collections
import logging
import time

from future.utils import raise_from
from past.builtins.misc import xrange

from pyathena.error import (DatabaseError, OperationalError, ProgrammingError,
                            NotSupportedError, DataError)
from pyathena.util import synchronized, retry_api_call


_logger = logging.getLogger(__name__)


class Cursor(object):

    DEFAULT_FETCH_SIZE = 1000

    def __init__(self, client, s3_staging_dir, schema_name, poll_interval,
                 encryption_option, kms_key, converter, formatter,
                 retry_exceptions=('ThrottlingException', 'TooManyRequestsException'),
                 retry_attempt=5, retry_multiplier=1,
                 retry_max_delay=1800, retry_exponential_base=2):
        self._connection = client
        self._s3_staging_dir = s3_staging_dir
        self._schema_name = schema_name
        self._poll_interval = poll_interval
        self._encryption_option = encryption_option
        self._kms_key = kms_key
        self._converter = converter
        self._formatter = formatter

        self.retry_exceptions = retry_exceptions
        self.retry_attempt = retry_attempt
        self.retry_multiplier = retry_multiplier
        self.retry_max_deply = retry_max_delay
        self.retry_exponential_base = retry_exponential_base

        self._rownumber = None
        self._arraysize = self.DEFAULT_FETCH_SIZE

        self._description = None
        self._query_id = None
        self._next_token = None
        self._result_set = collections.deque()
        self._meta_data = None

        self._completion_date_time = None
        self._submission_date_time = None
        self._data_scanned_in_bytes = None
        self._execution_time_in_millis = None

    @property
    def connection(self):
        return self._connection

    @property
    def arraysize(self):
        return self._arraysize

    @arraysize.setter
    def arraysize(self, value):
        if value <= 0 or value > self.DEFAULT_FETCH_SIZE:
            raise ProgrammingError('MaxResults is more than maximum allowed length {0}.'.format(
                self.DEFAULT_FETCH_SIZE))
        self._arraysize = value

    @property
    def rownumber(self):
        return self._rownumber

    @property
    def rowcount(self):
        """By default, return -1 to indicate that this is not supported."""
        return -1

    @property
    def description(self):
        if self._description or self._description == []:
            return self._description
        if self._meta_data is None:
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

    def close(self):
        pass

    def _build_query_execution_request(self, query):
        request = {
            'QueryString': query,
            'QueryExecutionContext': {
                'Database': self._schema_name,
            },
            'ResultConfiguration': {
                'OutputLocation': self._s3_staging_dir,
            },
        }
        if self._encryption_option:
            enc_conf = {
                'EncryptionOption': self._encryption_option,
            }
            if self._kms_key:
                enc_conf.update({
                    'KmsKey': self._kms_key
                })
            request['ResultConfiguration'].update({
                'EncryptionConfiguration': enc_conf,
            })
        return request

    def _poll(self):
        if not self._query_id:
            raise ProgrammingError('QueryExecutionId is none or empty.')
        while True:
            try:
                request = {'QueryExecutionId': self._query_id}
                response = retry_api_call(self._connection.get_query_execution,
                                          exceptions=self.retry_exceptions,
                                          attempt=self.retry_attempt,
                                          multiplier=self.retry_multiplier,
                                          max_delay=self.retry_max_deply,
                                          exp_base=self.retry_exponential_base,
                                          logger=_logger,
                                          **request)
            except Exception as e:
                _logger.exception('Failed to poll query result.')
                raise_from(OperationalError(*e.args), e)
            else:
                query_execution = response.get('QueryExecution', None)
                if not query_execution:
                    raise DataError('KeyError `QueryExecution`')
                status = query_execution.get('Status', None)
                if not status:
                    raise DataError('KeyError `Status`')

                state = status.get('State', None)
                if state == 'SUCCEEDED':
                    self._completion_date_time = status.get('CompletionDateTime', None)
                    self._submission_date_time = status.get('SubmissionDateTime', None)

                    statistics = query_execution.get('Statistics', {})
                    self._data_scanned_in_bytes = statistics.get(
                        'DataScannedInBytes', None)
                    self._execution_time_in_millis = statistics.get(
                        'EngineExecutionTimeInMillis', None)
                    break
                elif state == 'FAILED':
                    raise OperationalError(status.get('StateChangeReason', None))
                elif state == 'CANCELLED':
                    raise OperationalError(status.get('StateChangeReason', None))
                else:
                    time.sleep(self._poll_interval)

    def _reset_state(self):
        self._description = None
        self._query_id = None
        self._next_token = None
        self._result_set.clear()
        self._rownumber = 0

        self._completion_date_time = None
        self._submission_date_time = None
        self._data_scanned_in_bytes = None
        self._execution_time_in_millis = None

    @synchronized
    def execute(self, operation, parameters=None):
        query = self._formatter.format(operation, parameters)
        _logger.debug(query)

        request = self._build_query_execution_request(query)
        try:
            self._reset_state()
            response = retry_api_call(self._connection.start_query_execution,
                                      exceptions=self.retry_exceptions,
                                      attempt=self.retry_attempt,
                                      multiplier=self.retry_multiplier,
                                      max_delay=self.retry_max_deply,
                                      exp_base=self.retry_exponential_base,
                                      logger=_logger,
                                      **request)
        except Exception as e:
            _logger.exception('Failed to execute query.')
            raise_from(DatabaseError(*e.args), e)
        else:
            self._query_id = response.get('QueryExecutionId', None)
            self._poll()
            self._pre_fetch()

    def executemany(self, operation, seq_of_parameters):
        raise NotSupportedError

    @synchronized
    def cancel(self):
        if not self._query_id:
            raise ProgrammingError('QueryExecutionId is none or empty.')
        try:
            request = {'QueryExecutionId': self._query_id}
            retry_api_call(self._connection.stop_query_execution,
                           exceptions=self.retry_exceptions,
                           attempt=self.retry_attempt,
                           multiplier=self.retry_multiplier,
                           max_delay=self.retry_max_deply,
                           exp_base=self.retry_exponential_base,
                           logger=_logger,
                           **request)
        except Exception as e:
            _logger.exception('Failed to cancel query.')
            raise_from(OperationalError(*e.args), e)

    def _is_first_row_column_labels(self, rows):
        first_row_data = rows[0].get('Data', [])
        for meta, data in zip(self._meta_data, first_row_data):
            if meta.get('Name', None) != data.get('VarCharValue', None):
                return False
        return True

    def _process_result_set(self, response):
        if self._meta_data is None:
            raise ProgrammingError('ResultSetMetadata is none.')
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
        self._result_set.extend(processed_rows)
        self._next_token = response.get('NextToken', None)

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
        self._meta_data = column_info

    def _pre_fetch(self):
        if not self._query_id:
            raise ProgrammingError('QueryExecutionId is none or empty.')
        try:
            request = {
                'QueryExecutionId': self._query_id,
                'MaxResults': self._arraysize,
            }
            response = retry_api_call(self._connection.get_query_results,
                                      exceptions=self.retry_exceptions,
                                      attempt=self.retry_attempt,
                                      multiplier=self.retry_multiplier,
                                      max_delay=self.retry_max_deply,
                                      exp_base=self.retry_exponential_base,
                                      logger=_logger,
                                      **request)
        except Exception as e:
            _logger.exception('Failed to fetch result set.')
            raise_from(OperationalError(*e.args), e)
        else:
            self._process_meta_data(response)
            self._process_result_set(response)

    def _fetch(self):
        if not self._query_id or not self._next_token:
            raise ProgrammingError('QueryExecutionId or NextToken is none or empty.')
        try:
            request = {
                'QueryExecutionId': self._query_id,
                'MaxResults': self._arraysize,
                'NextToken': self._next_token,
            }
            response = retry_api_call(self._connection.get_query_results,
                                      exceptions=self.retry_exceptions,
                                      attempt=self.retry_attempt,
                                      multiplier=self.retry_multiplier,
                                      max_delay=self.retry_max_deply,
                                      exp_base=self.retry_exponential_base,
                                      logger=_logger,
                                      **request)
        except Exception as e:
            _logger.exception('Failed to fetch result set.')
            raise_from(OperationalError(*e.args), e)
        else:
            self._process_result_set(response)

    @synchronized
    def fetchone(self):
        if not self._query_id:
            raise ProgrammingError('QueryExecutionId is none or empty.')
        if not self._result_set and self._next_token:
            self._fetch()
        if not self._result_set:
            return None
        else:
            self._rownumber += 1
            return self._result_set.popleft()

    @synchronized
    def fetchmany(self, size=None):
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

    @synchronized
    def fetchall(self):
        rows = []
        while True:
            row = self.fetchone()
            if row:
                rows.append(row)
            else:
                break
        return rows

    def setinputsizes(self, sizes):
        """Does nothing by default"""
        pass

    def setoutputsize(self, size, column=None):
        """Does nothing by default"""
        pass

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
