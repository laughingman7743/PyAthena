# -*- coding: utf-8 -*-
import logging
import os
import time
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Type

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

if TYPE_CHECKING:
    from botocore.client import BaseClient

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
        catalog_name: Optional[str] = "awsdatacatalog",
        work_group: Optional[str] = None,
        poll_interval: float = 1,
        encryption_option: Optional[str] = None,
        kms_key: Optional[str] = None,
        profile_name: Optional[str] = None,
        role_arn: Optional[str] = None,
        role_session_name: str = "PyAthena-session-{0}".format(int(time.time())),
        serial_number: Optional[str] = None,
        duration_seconds: int = 3600,
        converter: Optional[Converter] = None,
        formatter: Optional[Formatter] = None,
        retry_config: Optional[RetryConfig] = None,
        cursor_class: Type[BaseCursor] = Cursor,
        kill_on_interrupt: bool = True,
        session: Optional[Session] = None,
        **kwargs
    ) -> None:
        self._kwargs = {
            **kwargs,
            "role_arn": role_arn,
            "role_session_name": role_session_name,
            "serial_number": serial_number,
            "duration_seconds": duration_seconds,
        }
        if s3_staging_dir:
            self.s3_staging_dir: Optional[str] = s3_staging_dir
        else:
            self.s3_staging_dir = os.getenv(self._ENV_S3_STAGING_DIR, None)
        self.region_name = region_name
        self.schema_name = schema_name
        self.catalog_name = catalog_name
        if work_group:
            self.work_group: Optional[str] = work_group
        else:
            self.work_group = os.getenv(self._ENV_WORK_GROUP, None)
        self.poll_interval = poll_interval
        self.encryption_option = encryption_option
        self.kms_key = kms_key
        self.profile_name = profile_name

        assert (
            self.s3_staging_dir or self.work_group
        ), "Required argument `s3_staging_dir` or `work_group` not found."

        if session:
            self._session = session
        else:
            if role_arn:
                creds = self._assume_role(
                    profile_name=self.profile_name,
                    region_name=self.region_name,
                    role_arn=role_arn,
                    role_session_name=role_session_name,
                    serial_number=serial_number,
                    duration_seconds=duration_seconds,
                )
                self.profile_name = None
                self._kwargs.update(
                    {
                        "aws_access_key_id": creds["AccessKeyId"],
                        "aws_secret_access_key": creds["SecretAccessKey"],
                        "aws_session_token": creds["SessionToken"],
                    }
                )
            elif serial_number:
                creds = self._get_session_token(
                    profile_name=self.profile_name,
                    region_name=self.region_name,
                    serial_number=serial_number,
                    duration_seconds=duration_seconds,
                )
                self.profile_name = None
                self._kwargs.update(
                    {
                        "aws_access_key_id": creds["AccessKeyId"],
                        "aws_secret_access_key": creds["SecretAccessKey"],
                        "aws_session_token": creds["SessionToken"],
                    }
                )
            self._session = Session(
                profile_name=self.profile_name, **self._session_kwargs
            )
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
        serial_number: Optional[str],
        duration_seconds: int,
    ) -> Dict[str, Any]:
        session = Session(profile_name=profile_name, **self._session_kwargs)
        client = session.client("sts", region_name=region_name, **self._client_kwargs)
        request = {
            "RoleArn": role_arn,
            "RoleSessionName": role_session_name,
            "DurationSeconds": duration_seconds,
        }
        if serial_number:
            token_code = input("Enter the MFA code: ")
            request.update(
                {
                    "SerialNumber": serial_number,
                    "TokenCode": token_code,
                }
            )
        response = client.assume_role(**request)
        creds: Dict[str, Any] = response["Credentials"]
        return creds

    def _get_session_token(
        self,
        profile_name: Optional[str],
        region_name: Optional[str],
        serial_number: Optional[str],
        duration_seconds: int,
    ) -> Dict[str, Any]:
        session = Session(profile_name=profile_name, **self._session_kwargs)
        client = session.client("sts", region_name=region_name, **self._client_kwargs)
        token_code = input("Enter the MFA code: ")
        request = {
            "DurationSeconds": duration_seconds,
            "SerialNumber": serial_number,
            "TokenCode": token_code,
        }
        response = client.get_session_token(**request)
        creds: Dict[str, Any] = response["Credentials"]
        return creds

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
    def client(self) -> "BaseClient":
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
            from pyathena.pandas.async_cursor import AsyncPandasCursor
            from pyathena.pandas.cursor import PandasCursor

            if cursor is PandasCursor or cursor is AsyncPandasCursor:
                converter = DefaultPandasTypeConverter()
            else:
                converter = DefaultTypeConverter()
        return cursor(
            connection=self,
            s3_staging_dir=kwargs.pop("s3_staging_dir", self.s3_staging_dir),
            poll_interval=kwargs.pop("poll_interval", self.poll_interval),
            encryption_option=kwargs.pop("encryption_option", self.encryption_option),
            kms_key=kwargs.pop("kms_key", self.kms_key),
            converter=converter,
            formatter=kwargs.pop("formatter", self._formatter),
            retry_config=kwargs.pop("retry_config", self._retry_config),
            schema_name=kwargs.pop("schema_name", self.schema_name),
            catalog_name=kwargs.pop("catalog_name", self.catalog_name),
            work_group=kwargs.pop("work_group", self.work_group),
            kill_on_interrupt=kwargs.pop("kill_on_interrupt", self.kill_on_interrupt),
            **kwargs
        )

    def close(self) -> None:
        pass

    def commit(self) -> None:
        pass

    def rollback(self) -> None:
        raise NotSupportedError
