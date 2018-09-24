# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import logging
import time
from abc import ABCMeta, abstractmethod

from future.utils import raise_from, with_metaclass

from pyathena.error import DatabaseError, OperationalError, ProgrammingError
from pyathena.model import AthenaQueryExecution
from pyathena.util import retry_api_call

_logger = logging.getLogger(__name__)


class CursorIterator(with_metaclass(ABCMeta, object)):

    DEFAULT_FETCH_SIZE = 1000

    def __init__(self, **kwargs):
        super(CursorIterator, self).__init__()
        self.arraysize = kwargs.get('arraysize', self.DEFAULT_FETCH_SIZE)
        self._rownumber = None

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

    @abstractmethod
    def fetchone(self):
        raise NotImplementedError  # pragma: no cover

    @abstractmethod
    def fetchmany(self):
        raise NotImplementedError  # pragma: no cover

    @abstractmethod
    def fetchall(self):
        raise NotImplementedError  # pragma: no cover

    def __next__(self):
        row = self.fetchone()
        if row is None:
            raise StopIteration
        else:
            return row

    next = __next__

    def __iter__(self):
        return self


class BaseCursor(with_metaclass(ABCMeta, object)):

    def __init__(self, connection, s3_staging_dir, schema_name, poll_interval,
                 encryption_option, kms_key, converter, formatter,
                 retry_exceptions, retry_attempt, retry_multiplier,
                 retry_max_delay, retry_exponential_base, **kwargs):
        super(BaseCursor, self).__init__(**kwargs)
        self._connection = connection
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
        self.retry_max_delay = retry_max_delay
        self.retry_exponential_base = retry_exponential_base

        self.retry_attempt = retry_attempt
        self.retry_multiplier = retry_multiplier
        self.retry_max_delay = retry_max_delay
        self.retry_exponential_base = retry_exponential_base

    @property
    def connection(self):
        return self._connection

    def _get_query_execution(self, query_id):
        request = {'QueryExecutionId': query_id}
        try:
            response = retry_api_call(self._connection.client.get_query_execution,
                                      exceptions=self.retry_exceptions,
                                      attempt=self.retry_attempt,
                                      multiplier=self.retry_multiplier,
                                      max_delay=self.retry_max_delay,
                                      exp_base=self.retry_exponential_base,
                                      logger=_logger,
                                      **request)
        except Exception as e:
            _logger.exception('Failed to get query execution.')
            raise_from(OperationalError(*e.args), e)
        else:
            return AthenaQueryExecution(response)

    def _poll(self, query_id):
        while True:
            query_execution = self._get_query_execution(query_id)
            if query_execution.state in [AthenaQueryExecution.STATE_SUCCEEDED,
                                         AthenaQueryExecution.STATE_FAILED,
                                         AthenaQueryExecution.STATE_CANCELLED]:
                return query_execution
            else:
                time.sleep(self._poll_interval)

    def _build_start_query_execution_request(self, query):
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

    def _execute(self, operation, parameters=None):
        query = self._formatter.format(operation, parameters)
        _logger.debug(query)

        request = self._build_start_query_execution_request(query)
        try:
            response = retry_api_call(self._connection.client.start_query_execution,
                                      exceptions=self.retry_exceptions,
                                      attempt=self.retry_attempt,
                                      multiplier=self.retry_multiplier,
                                      max_delay=self.retry_max_delay,
                                      exp_base=self.retry_exponential_base,
                                      logger=_logger,
                                      **request)
        except Exception as e:
            _logger.exception('Failed to execute query.')
            raise_from(DatabaseError(*e.args), e)
        else:
            return response.get('QueryExecutionId', None)

    @abstractmethod
    def execute(self, operation, parameters=None):
        raise NotImplementedError  # pragma: no cover

    @abstractmethod
    def executemany(self, operation, seq_of_parameters):
        raise NotImplementedError  # pragma: no cover

    @abstractmethod
    def close(self):
        raise NotImplementedError  # pragma: no cover

    def _cancel(self, query_id):
        request = {'QueryExecutionId': query_id}
        try:
            retry_api_call(self._connection.client.stop_query_execution,
                           exceptions=self.retry_exceptions,
                           attempt=self.retry_attempt,
                           multiplier=self.retry_multiplier,
                           max_delay=self.retry_max_delay,
                           exp_base=self.retry_exponential_base,
                           logger=_logger,
                           **request)
        except Exception as e:
            _logger.exception('Failed to cancel query.')
            raise_from(OperationalError(*e.args), e)

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
