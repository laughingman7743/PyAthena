# -*- coding: utf-8 -*-
from __future__ import absolute_import, unicode_literals

import logging
import os
import time

from boto3.session import Session
from future.utils import iteritems

from pyathena.async_pandas_cursor import AsyncPandasCursor
from pyathena.converter import DefaultPandasTypeConverter, DefaultTypeConverter
from pyathena.cursor import Cursor
from pyathena.error import NotSupportedError
from pyathena.formatter import DefaultParameterFormatter
from pyathena.pandas_cursor import PandasCursor
from pyathena.util import RetryConfig

_logger = logging.getLogger(__name__)


class Connection(object):

    _ENV_S3_STAGING_DIR = "AWS_ATHENA_S3_STAGING_DIR"
    _ENV_WORK_GROUP = "AWS_ATHENA_WORK_GROUP"
    _SESSION_PASSING_ARGS = [
        "aws_access_key_id",
        "aws_secret_access_key",
        "aws_session_token",
        "region_name",
        "botocore_session",
        "profile_name",
    ]
    _CLIENT_PASSING_ARGS = [
        "aws_access_key_id",
        "aws_secret_access_key",
        "aws_session_token",
        "config",
        "api_version",
        "use_ssl",
        "verify",
        "endpoint_url",
    ]

    def __init__(
        self,
        s3_staging_dir=None,
        region_name=None,
        schema_name="default",
        work_group=None,
        poll_interval=1,
        encryption_option=None,
        kms_key=None,
        profile_name=None,
        role_arn=None,
        role_session_name="PyAthena-session-{0}".format(int(time.time())),
        duration_seconds=3600,
        converter=None,
        formatter=None,
        retry_config=None,
        cursor_class=Cursor,
        **kwargs
    ):
        self._kwargs = kwargs
        if s3_staging_dir:
            self.s3_staging_dir = s3_staging_dir
        else:
            self.s3_staging_dir = os.getenv(self._ENV_S3_STAGING_DIR, None)
        self.region_name = region_name
        self.schema_name = schema_name
        if work_group:
            self.work_group = work_group
        else:
            self.work_group = os.getenv(self._ENV_WORK_GROUP, None)
        self.poll_interval = poll_interval
        self.encryption_option = encryption_option
        self.kms_key = kms_key
        self.profile_name = profile_name

        assert self.schema_name, "Required argument `schema_name` not found."
        assert (
            self.s3_staging_dir or self.work_group
        ), "Required argument `s3_staging_dir` or `work_group` not found."

        if role_arn:
            creds = self._assume_role(
                self.profile_name,
                self.region_name,
                role_arn,
                role_session_name,
                duration_seconds,
            )
            self.profile_name = None
            self._kwargs.update(
                {
                    "aws_access_key_id": creds["AccessKeyId"],
                    "aws_secret_access_key": creds["SecretAccessKey"],
                    "aws_session_token": creds["SessionToken"],
                }
            )
        self._session = Session(profile_name=self.profile_name, **self._session_kwargs)
        self._client = self._session.client(
            "athena", region_name=self.region_name, **self._client_kwargs
        )
        self._converter = converter
        self._formatter = formatter if formatter else DefaultParameterFormatter()
        self._retry_config = retry_config if retry_config else RetryConfig()
        self.cursor_class = cursor_class

    def _assume_role(
        self, profile_name, region_name, role_arn, role_session_name, duration_seconds
    ):
        # MFA is not supported. If you want to use MFA, create a configuration file.
        # http://boto3.readthedocs.io/en/latest/guide/configuration.html#assume-role-provider
        session = Session(profile_name=profile_name, **self._session_kwargs)
        client = session.client("sts", region_name=region_name, **self._client_kwargs)
        response = client.assume_role(
            RoleArn=role_arn,
            RoleSessionName=role_session_name,
            DurationSeconds=duration_seconds,
        )
        return response["Credentials"]

    @property
    def _session_kwargs(self):
        return {
            k: v for k, v in iteritems(self._kwargs) if k in self._SESSION_PASSING_ARGS
        }

    @property
    def _client_kwargs(self):
        return {
            k: v for k, v in iteritems(self._kwargs) if k in self._CLIENT_PASSING_ARGS
        }

    @property
    def session(self):
        return self._session

    @property
    def client(self):
        return self._client

    @property
    def retry_config(self):
        return self._retry_config

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def cursor(self, cursor=None, **kwargs):
        if not cursor:
            cursor = self.cursor_class
        converter = kwargs.pop("converter", self._converter)
        if not converter:
            if cursor is PandasCursor or cursor is AsyncPandasCursor:
                converter = DefaultPandasTypeConverter()
            else:
                converter = DefaultTypeConverter()
        return cursor(
            connection=self,
            s3_staging_dir=kwargs.pop("s3_staging_dir", self.s3_staging_dir),
            schema_name=kwargs.pop("schema_name", self.schema_name),
            work_group=kwargs.pop("work_group", self.work_group),
            poll_interval=kwargs.pop("poll_interval", self.poll_interval),
            encryption_option=kwargs.pop("encryption_option", self.encryption_option),
            kms_key=kwargs.pop("kms_key", self.kms_key),
            converter=converter,
            formatter=kwargs.pop("formatter", self._formatter),
            retry_config=kwargs.pop("retry_config", self._retry_config),
            **kwargs
        )

    def close(self):
        pass

    def commit(self):
        pass

    def rollback(self):
        raise NotSupportedError
