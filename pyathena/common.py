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

    def __init__(self, connection, s3_staging_dir, schema_name, work_group,
                 poll_interval, encryption_option, kms_key, converter, formatter,
                 retry_config, **kwargs):
        super(BaseCursor, self).__init__(**kwargs)
        self._connection = connection
        self._s3_staging_dir = s3_staging_dir
        self._schema_name = schema_name
        self._work_group = work_group
        self._poll_interval = poll_interval
        self._encryption_option = encryption_option
        self._kms_key = kms_key
        self._converter = converter
        self._formatter = formatter
        self._retry_config = retry_config

    @property
    def connection(self):
        return self._connection

    def _get_query_execution(self, query_id):
        request = {'QueryExecutionId': query_id}
        try:
            response = retry_api_call(self._connection.client.get_query_execution,
                                      config=self._retry_config,
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

    def _build_start_query_execution_request(self, query, work_group=None, s3_staging_dir=None):
        if not s3_staging_dir:
            s3_staging_dir = self._s3_staging_dir
        request = {
            'QueryString': query,
            'QueryExecutionContext': {
                'Database': self._schema_name,
            },
            'ResultConfiguration': {
                'OutputLocation': s3_staging_dir,
            },
        }
        if self._work_group or work_group:
            request.update({
                'WorkGroup': work_group if work_group else self._work_group
            })
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

    def _find_previous_query_id(self, request, work_group, cache_size):
        next_token = None
        while cache_size > 0:
            n = min(cache_size, 50)  # 50 is max allowed by AWS API
            response = self.connection._client.list_query_executions(
                MaxResults=n, NextToken=next_token, WorkGroup=work_group
            )
            cache_size -= n
            query_ids = response['QueryExecutionIds']
            next_token = response['NextToken']
            query_executions = self.connection._client.batch_get_query_execution(
                QueryExecutionIds=query_ids
            )['QueryExecutions']
            for execution in query_executions:
                queries_match = execution['Query'] == request['QueryString']
                succeeded = execution['Status']['State'] == 'SUCCEEDED'
                locations_match = execution['ResultConfiguration']['OutputLocation'] == request['ResultConfiguration']['OutputLocation']
                is_dml = execution['StatementType'] == 'DML'
                if queries_match and succeeded and locations_match and is_dml:
                    return execution['QueryExecutionId']

    def _execute(self, operation, parameters=None, work_group=None, s3_staging_dir=None,
                 cache_size=0):
        query = self._formatter.format(operation, parameters)
        _logger.debug(query)

        request = self._build_start_query_execution_request(query, work_group, s3_staging_dir)
        query_id = self._find_previous_query_id(request, work_group, cache_size)
        if query_id is None:
            try:
                query_id = retry_api_call(self._connection.client.start_query_execution,
                                          config=self._retry_config,
                                          logger=_logger,
                                          **request).get('QueryExecutionId', None)
            except Exception as e:
                _logger.exception('Failed to execute query.')
                raise_from(DatabaseError(*e.args), e)
        return query_id

    @abstractmethod
    def execute(self, operation, parameters=None, work_group=None, s3_staging_dir=None,
                cache_size=0):
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
                           config=self._retry_config,
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
