# -*- coding: utf-8 -*-
import logging
import os
import time
from typing import Any, Dict, List, Optional, Type

from boto3.session import Session

from pyathena.common import BaseCursor
from pyathena.converter import (
    Converter,
    DefaultPandasTypeConverter,
    DefaultTypeConverter,
)
from pyathena.cursor import Cursor
from pyathena.error import NotSupportedError
from pyathena.formatter import DefaultParameterFormatter, Formatter
from pyathena.util import RetryConfig

_logger = logging.getLogger(__name__)  # type: ignore


class Connection(object):

    _ENV_S3_STAGING_DIR: str = "AWS_ATHENA_S3_STAGING_DIR"
    _ENV_WORK_GROUP: str = "AWS_ATHENA_WORK_GROUP"
    _SESSION_PASSING_ARGS: List[str] = [
        "aws_access_key_id",
        "aws_secret_access_key",
        "aws_session_token",
        "region_name",
        "botocore_session",
        "profile_name",
    ]
    _CLIENT_PASSING_ARGS: List[str] = [
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
        s3_staging_dir: Optional[str] = None,
        region_name: Optional[str] = None,
        schema_name: Optional[str] = "default",
        work_group: Optional[str] = None,
        poll_interval: int = 1,
        encryption_option: Optional[str] = None,
        kms_key: Optional[str] = None,
        profile_name: Optional[str] = None,
        role_arn: Optional[str] = None,
        role_session_name: str = "PyAthena-session-{0}".format(int(time.time())),
        duration_seconds: int = 3600,
        converter: Converter = None,
        formatter: Formatter = None,
        retry_config: RetryConfig = None,
        cursor_class: Type[BaseCursor] = Cursor,
        kill_on_interrupt: bool = True,
        **kwargs
    ) -> None:
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
        self.kill_on_interrupt = kill_on_interrupt

    def _assume_role(
        self,
        profile_name: Optional[str],
        region_name: Optional[str],
        role_arn: Optional[str],
        role_session_name: str,
        duration_seconds: int,
    ):
        # MFA is not supported. If you want to use MFA, create a configuration file.
        # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html#assume-role-provider
        session = Session(profile_name=profile_name, **self._session_kwargs)
        client = session.client("sts", region_name=region_name, **self._client_kwargs)
        response = client.assume_role(
            RoleArn=role_arn,
            RoleSessionName=role_session_name,
            DurationSeconds=duration_seconds,
        )
        return response["Credentials"]

    @property
    def _session_kwargs(self) -> Dict[str, Any]:
        return {
            k: v for k, v in self._kwargs.items() if k in self._SESSION_PASSING_ARGS
        }

    @property
    def _client_kwargs(self) -> Dict[str, Any]:
        return {k: v for k, v in self._kwargs.items() if k in self._CLIENT_PASSING_ARGS}

    @property
    def session(self) -> Session:
        return self._session

    @property
    def client(self):
        return self._client

    @property
    def retry_config(self) -> RetryConfig:
        return self._retry_config

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def cursor(self, cursor: Optional[Type[BaseCursor]] = None, **kwargs) -> BaseCursor:
        if not cursor:
            cursor = self.cursor_class
        converter = kwargs.pop("converter", self._converter)
        if not converter:
            from pyathena.async_pandas_cursor import AsyncPandasCursor
            from pyathena.pandas_cursor import PandasCursor

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
            kill_on_interrupt=kwargs.pop("kill_on_interrupt", self.kill_on_interrupt),
            **kwargs
        )

    def close(self) -> None:
        pass

    def commit(self) -> None:
        pass

    def rollback(self) -> None:
        raise NotSupportedError
