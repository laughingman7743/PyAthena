# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import binascii
import collections
import io
import json
import logging
import re
from decimal import Decimal

from future.utils import raise_from
from past.builtins.misc import xrange

from pyathena.common import CursorIterator
from pyathena.error import DataError, OperationalError, ProgrammingError
from pyathena.model import AthenaQueryExecution
from pyathena.util import retry_api_call

_logger = logging.getLogger(__name__)


class WithResultSet(object):

    def __init__(self):
        super(WithResultSet, self).__init__()
        self._query_id = None
        self._result_set = None

    def _reset_state(self):
        self._query_id = None
        if self._result_set and not self._result_set.is_closed:
            self._result_set.close()
        self._result_set = None

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


class AthenaResultSet(CursorIterator):

    def __init__(self, connection, converter, query_execution, arraysize, retry_config):
        super(AthenaResultSet, self).__init__(arraysize=arraysize)
        self._connection = connection
        self._converter = converter
        self._query_execution = query_execution
        assert self._query_execution, 'Required argument `query_execution` not found.'
        self._retry_config = retry_config

        self._meta_data = None
        self._rows = collections.deque()
        self._next_token = None

        if self._query_execution.state == AthenaQueryExecution.STATE_SUCCEEDED:
            self._rownumber = 0
            self._pre_fetch()

    @property
    def query_id(self):
        return self._query_execution.query_id

    @property
    def query(self):
        return self._query_execution.query

    @property
    def state(self):
        return self._query_execution.state

    @property
    def state_change_reason(self):
        return self._query_execution.state_change_reason

    @property
    def completion_date_time(self):
        return self._query_execution.completion_date_time

    @property
    def submission_date_time(self):
        return self._query_execution.submission_date_time

    @property
    def data_scanned_in_bytes(self):
        return self._query_execution.data_scanned_in_bytes

    @property
    def execution_time_in_millis(self):
        return self._query_execution.execution_time_in_millis

    @property
    def output_location(self):
        return self._query_execution.output_location

    @property
    def description(self):
        if self._meta_data is None:
            return None
        return [
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

    def __fetch(self, next_token=None):
        if not self._query_execution.query_id:
            raise ProgrammingError('QueryExecutionId is none or empty.')
        if self._query_execution.state != 'SUCCEEDED':
            raise ProgrammingError('QueryExecutionState is not SUCCEEDED.')
        request = {
            'QueryExecutionId': self._query_execution.query_id,
            'MaxResults': self._arraysize,
        }
        if next_token:
            request.update({'NextToken': next_token})
        try:
            response = retry_api_call(self._connection.client.get_query_results,
                                      config=self._retry_config,
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
        response = self.__fetch()
        self._process_meta_data(response)
        self._process_rows(response)

    def fetchone(self):
        if not self._rows and self._next_token:
            self._fetch()
        if not self._rows:
            return None
        else:
            self._rownumber += 1
            return self._rows.popleft()

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

    def fetchall(self):
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
        self._rownumber = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class AthenaPandasResultSet(AthenaResultSet):

    _pattern_output_location = re.compile(r'^s3://(?P<bucket>[a-zA-Z0-9.\-_]+)/(?P<key>.+)$')
    _converters = {
        'decimal': Decimal,
        'varbinary': lambda b: binascii.a2b_hex(''.join(b.split(' '))),
        'json': json.loads,
    }
    _parse_dates = [
        'date',
        'time',
        'time with time zone',
        'timestamp',
        'timestamp with time zone',
    ]

    def __init__(self, connection, converter, query_execution, arraysize, retry_config):
        super(AthenaPandasResultSet, self).__init__(
            connection=connection,
            converter=converter,
            query_execution=query_execution,
            arraysize=1,  # Fetch one row to retrieve metadata
            retry_config=retry_config)
        self._arraysize = arraysize
        self._client = self._connection.session.client(
            's3', region_name=self._connection.region_name, **self._connection._client_kwargs)
        if self._query_execution.state == AthenaQueryExecution.STATE_SUCCEEDED:
            self._df = self._as_pandas()
        else:
            import pandas as pd
            self._df = pd.DataFrame()
        self._iterrows = self._df.iterrows()

    @classmethod
    def _parse_output_location(cls, output_location):
        match = cls._pattern_output_location.search(output_location)
        if match:
            return match.group('bucket'), match.group('key')
        else:
            raise DataError('Unknown `output_location` format.')

    @property
    def _dtypes(self):
        if not hasattr(self, '__dtypes'):
            import pandas as pd
            self.__dtypes = {
                'boolean': bool,
                'tinyint': pd.Int64Dtype(),
                'smallint': pd.Int64Dtype(),
                'integer': pd.Int64Dtype(),
                'bigint': pd.Int64Dtype(),
                'float': float,
                'real': float,
                'double': float,
                'char': str,
                'varchar': str,
                'array': str,
                'map': str,
                'row': str,
            }
        return self.__dtypes

    @property
    def dtypes(self):
        return {
            d[0]: self._dtypes[d[1]] for d in self.description if d[1] in self._dtypes
        }

    @property
    def converters(self):
        return {
            d[0]: self._converters[d[1]] for d in self.description if d[1] in self._converters
        }

    @property
    def parse_dates(self):
        return [
            d[0] for d in self.description if d[1] in self._parse_dates
        ]

    def _trunc_date(self, df):
        times = [d[0] for d in self.description if d[1] in ('time', 'time with time zone')]
        if times:
            df.loc[:, times] = df.loc[:, times].apply(lambda r: r.dt.time)
        return df

    def _fetch(self):
        try:
            row = next(self._iterrows)
        except StopIteration:
            return None
        else:
            self._rownumber = row[0] + 1
            return tuple([row[1][d[0]] for d in self.description])

    def fetchone(self):
        return self._fetch()

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

    def fetchall(self):
        rows = []
        while True:
            row = self.fetchone()
            if row:
                rows.append(row)
            else:
                break
        return rows

    def _as_pandas(self):
        import pandas as pd
        if not self.output_location:
            raise ProgrammingError('OutputLocation is none or empty.')
        bucket, key = self._parse_output_location(self.output_location)
        try:
            response = retry_api_call(self._client.get_object,
                                      config=self._retry_config,
                                      logger=_logger,
                                      Bucket=bucket,
                                      Key=key)
        except Exception as e:
            _logger.exception('Failed to download csv.')
            raise_from(OperationalError(*e.args), e)
        else:
            length = response['ContentLength']
            if length:
                df = pd.read_csv(io.BytesIO(response['Body'].read()),
                                 dtype=self.dtypes,
                                 converters=self.converters,
                                 parse_dates=self.parse_dates,
                                 infer_datetime_format=True)
                df = self._trunc_date(df)
            else:  # Allow empty response so DDL can be used
                df = pd.DataFrame()
            return df

    def as_pandas(self):
        return self._df

    def close(self):
        super(AthenaPandasResultSet, self).close()
        self._df = None
        self._iterrows = None
