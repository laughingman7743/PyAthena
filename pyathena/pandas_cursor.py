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
from pyathena.common import WithResultSet
from pyathena.cursor import BaseCursor
from pyathena.error import NotSupportedError, OperationalError, ProgrammingError
from pyathena.model import AthenaQueryExecution
from pyathena.util import retry_api_call

_logger = logging.getLogger(__name__)


class PandasCursor(BaseCursor, WithResultSet):

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

    def _parse_output_location(self, output_location):
        match = self._pattern_output_location.search(output_location)
        if match:
            return match.group('bucket'), match.group('key')
        else:
            raise DataError('Unknown `output_location` format.')

    def close(self):
        pass

    def execute(self, operation, parameters=None):
        self._reset_state()
        self._query_id = self._execute(operation, parameters)
        query_execution = self._poll(self._query_id)
        if query_execution.state == AthenaQueryExecution.STATE_SUCCEEDED:
            self._result_set = AthenaResultSet(
                self._connection, self._converter, query_execution, self._arraysize,
                self.retry_exceptions, self.retry_attempt, self.retry_multiplier,
                self.retry_max_delay, self.retry_exponential_base)
        else:
            raise OperationalError(query_execution.state_change_reason)
        return self

    def executemany(self, operation, seq_of_parameters):
        raise NotSupportedError

    def cancel(self):
        if not self._query_id:
            raise ProgrammingError('QueryExecutionId is none or empty.')
        self._cancel(self._query_id)

    def as_pandas(self):
        if not self.has_result_set:
            raise ProgrammingError('No result set.')
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
