# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import logging
import os
import time

from boto3.session import Session

from pyathena.converter import TypeConverter
from pyathena.cursor import Cursor
from pyathena.error import NotSupportedError
from pyathena.formatter import ParameterFormatter

_logger = logging.getLogger(__name__)


class Connection(object):

    _ENV_S3_STAGING_DIR = 'AWS_ATHENA_S3_STAGING_DIR'

    def __init__(self, s3_staging_dir=None, region_name=None, schema_name='default',
                 poll_interval=1, encryption_option=None, kms_key=None, profile_name=None,
                 role_arn=None, role_session_name='PyAthena-session-{0}'.format(int(time.time())),
                 duration_seconds=3600, converter=None, formatter=None,
                 retry_exceptions=('ThrottlingException', 'TooManyRequestsException'),
                 retry_attempt=5, retry_multiplier=1,
                 retry_max_delay=1800, retry_exponential_base=2,
                 cursor_class=Cursor, **kwargs):
        if s3_staging_dir:
            self.s3_staging_dir = s3_staging_dir
        else:
            self.s3_staging_dir = os.getenv(self._ENV_S3_STAGING_DIR, None)
        assert self.s3_staging_dir, 'Required argument `s3_staging_dir` not found.'
        assert schema_name, 'Required argument `schema_name` not found.'
        self.region_name = region_name
        self.schema_name = schema_name
        self.poll_interval = poll_interval
        self.encryption_option = encryption_option
        self.kms_key = kms_key

        if role_arn:
            creds = self._assume_role(profile_name, region_name, role_arn,
                                      role_session_name, duration_seconds,
                                      **kwargs)
            profile_name = None
            kwargs.update({
                'aws_access_key_id': creds['AccessKeyId'],
                'aws_secret_access_key': creds['SecretAccessKey'],
                'aws_session_token': creds['SessionToken'],
            })
        self._session = Session(profile_name=profile_name, **kwargs)
        self._client = self._session.client('athena', region_name=region_name, **kwargs)

        self._converter = converter if converter else TypeConverter()
        self._formatter = formatter if formatter else ParameterFormatter()

        self.retry_exceptions = retry_exceptions
        self.retry_attempt = retry_attempt
        self.retry_multiplier = retry_multiplier
        self.retry_max_delay = retry_max_delay
        self.retry_exponential_base = retry_exponential_base

        self.cursor_class = cursor_class
        self._kwargs = kwargs

    @staticmethod
    def _assume_role(profile_name, region_name, role_arn,
                     role_session_name, duration_seconds, **kwargs):
        # MFA is not supported. If you want to use MFA, create a configuration file.
        # http://boto3.readthedocs.io/en/latest/guide/configuration.html#assume-role-provider
        session = Session(profile_name=profile_name, **kwargs)
        client = session.client('sts', region_name=region_name, **kwargs)
        response = client.assume_role(
            RoleArn=role_arn,
            RoleSessionName=role_session_name,
            DurationSeconds=duration_seconds,
        )
        return response['Credentials']

    @property
    def session(self):
        return self._session

    @property
    def client(self):
        return self._client

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def cursor(self, cursor=None, **kwargs):
        if not cursor:
            cursor = self.cursor_class
        return cursor(self, self.s3_staging_dir, self.schema_name, self.poll_interval,
                      self.encryption_option, self.kms_key, self._converter, self._formatter,
                      self.retry_exceptions, self.retry_attempt, self.retry_multiplier,
                      self.retry_max_delay, self.retry_exponential_base, **kwargs)

    def close(self):
        pass

    def commit(self):
        pass

    def rollback(self):
        raise NotSupportedError
