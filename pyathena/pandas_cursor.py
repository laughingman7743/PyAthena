#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import io
import logging
import re

import pandas as pd
from future.utils import raise_from

from pyathena import DataError
from pyathena.common import CursorIterator
from pyathena.cursor import BaseCursor
from pyathena.error import NotSupportedError, OperationalError, ProgrammingError
from pyathena.model import AthenaQueryExecution
from pyathena.util import retry_api_call

_logger = logging.getLogger(__name__)


class PandasCursor(BaseCursor):

    _pattern_output_location = re.compile(r'^s3://(?P<bucket>[a-zA-Z0-9.\-_]+)/(?P<key>.+)$')

    def __init__(self, connection, s3_staging_dir, schema_name, poll_interval,
                 encryption_option, kms_key, converter, formatter,
                 retry_exceptions, retry_attempt, retry_multiplier,
                 retry_max_delay, retry_exponential_base,
                 arraysize=CursorIterator.DEFAULT_FETCH_SIZE):
        super(PandasCursor, self).__init__(connection, s3_staging_dir, schema_name, poll_interval,
                                           encryption_option, kms_key, converter, formatter,
                                           retry_exceptions, retry_attempt, retry_multiplier,
                                           retry_max_delay, retry_exponential_base)
        self._client = self.connection.session.client(
            's3', region_name=self.connection.region_name, **self.connection._kwargs)
        self._arraysize = arraysize

        self._query_id = None
        self._query_execution = None

    @property
    def has_query_execution(self):
        return self._query_execution is not None

    @property
    def query_id(self):
        return self._query_id

    @property
    def query(self):
        if not self.has_query_execution:
            return None
        return self._query_execution.query

    @property
    def state(self):
        if not self.has_query_execution:
            return None
        return self._query_execution.state

    @property
    def state_change_reason(self):
        if not self.has_query_execution:
            return None
        return self._query_execution.state_change_reason

    @property
    def completion_date_time(self):
        if not self.has_query_execution:
            return None
        return self._query_execution.completion_date_time

    @property
    def submission_date_time(self):
        if not self.has_query_execution:
            return None
        return self._query_execution.submission_date_time

    @property
    def data_scanned_in_bytes(self):
        if not self.has_query_execution:
            return None
        return self._query_execution.data_scanned_in_bytes

    @property
    def execution_time_in_millis(self):
        if not self.has_query_execution:
            return None
        return self._query_execution.execution_time_in_millis

    @property
    def output_location(self):
        if not self.has_query_execution:
            return None
        return self._query_execution.output_location

    def _parse_output_location(self, output_location):
        match = self._pattern_output_location.search(output_location)
        if match:
            return match.group('bucket'), match.group('key')
        else:
            raise DataError('Unknown `output_location` format.')

    def close(self):
        pass

    def _reset_state(self):
        self._query_id = None
        self._query_execution = None

    def execute(self, operation, parameters=None):
        self._reset_state()
        self._query_id = self._execute(operation, parameters)
        self._query_execution = self._poll(self._query_id)
        if self._query_execution.state != AthenaQueryExecution.STATE_SUCCEEDED:
            raise OperationalError(self._query_execution.state_change_reason)
        return self

    def executemany(self, operation, seq_of_parameters):
        raise NotSupportedError

    def cancel(self):
        if not self._query_id:
            raise ProgrammingError('QueryExecutionId is none or empty.')
        self._cancel(self._query_id)

    def as_pandas(self):
        if not self.has_query_execution:
            raise ProgrammingError('No query execution.')
        bucket, key = self._parse_output_location(self.output_location)
        try:
            response = retry_api_call(self._client.get_object,
                                      Bucket=bucket,
                                      Key=key)
        except Exception as e:
            _logger.exception('Failed to download csv.')
            raise_from(OperationalError(*e.args), e)
        else:
            return pd.read_csv(io.BytesIO(response['Body'].read()))
