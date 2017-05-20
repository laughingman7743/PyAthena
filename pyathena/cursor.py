# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals
import collections
import logging
import sys
import time

from future.utils import reraise
from past.builtins.misc import xrange

from pyathena.error import (DatabaseError,
                            OperationalError,
                            ProgrammingError,
                            NotSupportedError)
from pyathena.util import synchronized


_logger = logging.getLogger(__name__)


class Cursor(object):

    DEFAULT_FETCH_SIZE = 1000

    def __init__(self, client, s3_staging_dir, schema_name, poll_interval,
                 encryption_option, kms_key, converter, formatter):
        self._connection = client
        self._s3_staging_dir = s3_staging_dir
        self._schema_name = schema_name
        self._poll_interval = poll_interval
        self._encryption_option = encryption_option
        self._kms_key = kms_key
        self._converter = converter
        self._formatter = formatter

        self._rownumber = None
        self._arraysize = self.DEFAULT_FETCH_SIZE

        self._description = None
        self._query_id = None
        self._next_token = None
        self._result_set = collections.deque()
        self._meta_data = None

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

    def close(self):
        pass

    def _build_query_execution_request(self, query):
        request = {
            'QueryString': query,
            'QueryExecutionContext': {
                'Database': self._schema_name
            },
            'ResultConfiguration': {
                'OutputLocation': self._s3_staging_dir,
            }
        }
        if self._encryption_option:
            enc_conf = {
                'EncryptionOption': self._encryption_option
            }
            if self._kms_key:
                enc_conf.update({
                    'KmsKey': self._kms_key
                })
            request['ResultConfiguration'].update({
                'EncryptionConfiguration': enc_conf
            })
        return request

    def _poll(self):
        if not self._query_id:
            raise ProgrammingError('QueryExecutionId is none or empty.')
        while True:
            response = self._connection.get_query_execution(
                QueryExecutionId=self._query_id
            )
            query_execution = response.get('QueryExecution', None)
            if not query_execution:
                raise DatabaseError('KeyError `QueryExecution`')
            status = query_execution.get('Status', None)
            if not status:
                raise DatabaseError('KeyError `Status`')

            state = status.get('State', None)
            if state == 'SUCCEEDED':
                break
            elif state == 'FAILED':
                raise OperationalError(status.get('StateChangeReason', None))
            elif state == 'CANCELLED':
                raise OperationalError(status.get('StateChangeReason', None))
            else:
                time.sleep(self._poll_interval)

    @synchronized
    def execute(self, operation, parameters=None):
        query = self._formatter.format(operation, parameters)
        _logger.debug(query)

        self._description = None
        self._query_id = None
        self._next_token = None
        self._result_set.clear()
        self._rownumber = 0

        request = self._build_query_execution_request(query)
        try:
            response = self._connection.start_query_execution(**request)
        except Exception:
            _logger.exception('Failed to execute query.')
            exc_info = sys.exc_info()
            reraise(DatabaseError, exc_info[1], exc_info[2])
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
        self._connection.stop_query_execution(
            QueryExecutionId=self._query_id
        )

    def _process_result_set(self, response):
        if self._meta_data is None:
            raise ProgrammingError('ResultSetMetadata is none or empty.')
        result_set = response.get('ResultSet', None)
        if not result_set:
            raise DatabaseError('KeyError `ResultSet`')
        rows = result_set.get('Rows', None)
        if rows is None:
            raise DatabaseError('KeyError `Rows`')
        offset = 1 if not self._next_token else 0
        self._result_set.extend([
            tuple([self._converter.convert(self._meta_data[j].get('Type', None),
                                           rows[i].get('Data', [])[j].get('VarCharValue', None))
                   for j in xrange(len(self._meta_data))])
            for i in xrange(offset, len(rows))
        ])
        self._next_token = response.get('NextToken', None)

    def _process_meta_data(self, response):
        result_set = response.get('ResultSet', None)
        if not result_set:
            raise DatabaseError('KeyError `ResultSet`')
        meta_data = result_set.get('ResultSetMetadata', None)
        if not meta_data:
            raise DatabaseError('KeyError `ResultSetMetadata`')
        column_info = meta_data.get('ColumnInfo', None)
        if column_info is None:
            raise DatabaseError('KeyError `ColumnInfo`')
        self._meta_data = [c for c in column_info]

    def _pre_fetch(self):
        if not self._query_id:
            raise ProgrammingError('QueryExecutionId is none or empty.')
        response = self._connection.get_query_results(
            QueryExecutionId=self._query_id,
            MaxResults=self._arraysize
        )
        self._process_meta_data(response)
        self._process_result_set(response)

    def _fetch(self):
        if not self._query_id or not self._next_token:
            raise ProgrammingError('QueryExecutionId or NextToken is none or empty.')
        response = self._connection.get_query_results(
            QueryExecutionId=self._query_id,
            MaxResults=self._arraysize,
            NextToken=self._next_token
        )
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
