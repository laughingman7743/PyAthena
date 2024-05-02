# -*- coding: utf-8 -*-
from __future__ import annotations

import itertools
import logging
import re
from concurrent.futures import Future, as_completed
from concurrent.futures.thread import ThreadPoolExecutor
from copy import deepcopy
from datetime import datetime
from multiprocessing import cpu_count
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Pattern, Tuple, Union, cast

import botocore.exceptions
from boto3 import Session
from botocore import UNSIGNED
from botocore.client import BaseClient, Config
from fsspec import AbstractFileSystem
from fsspec.callbacks import _DEFAULT_CALLBACK
from fsspec.spec import AbstractBufferedFile

import pyathena
from pyathena.filesystem.s3_object import (
    S3CompleteMultipartUpload,
    S3MultipartUpload,
    S3MultipartUploadPart,
    S3Object,
    S3ObjectType,
    S3PutObject,
    S3StorageClass,
)
from pyathena.util import RetryConfig, retry_api_call

if TYPE_CHECKING:
    from pyathena.connection import Connection

_logger = logging.getLogger(__name__)  # type: ignore


class S3FileSystem(AbstractFileSystem):
    # https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html
    # The minimum size of a part in a multipart upload is 5MB.
    MULTIPART_UPLOAD_MINIMUM_SIZE: int = 5 * 2**20  # 5MB
    DEFAULT_BLOCK_SIZE: int = 5 * 2**20  # 5MB
    PATTERN_PATH: Pattern[str] = re.compile(
        r"(^s3://|^s3a://|^)(?P<bucket>[a-zA-Z0-9.\-_]+)(/(?P<key>[^?]+)|/)?"
        r"($|\?version(Id|ID|id|_id)=(?P<version_id>.+)$)"
    )

    protocol = ["s3", "s3a"]
    _extra_tokenize_attributes = ("default_block_size",)

    def __init__(
        self,
        connection: Optional["Connection[Any]"] = None,
        default_block_size: Optional[int] = None,
        default_cache_type: Optional[str] = None,
        max_workers: int = (cpu_count() or 1) * 5,
        s3_additional_kwargs=None,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        if connection:
            self._client = connection.session.client(
                "s3",
                region_name=connection.region_name,
                config=connection.config,
                **connection._client_kwargs,
            )
            self._retry_config = connection.retry_config
        else:
            self._client = self._get_client_compatible_with_s3fs(**kwargs)
            self._retry_config = RetryConfig()
        self.default_block_size = (
            default_block_size if default_block_size else self.DEFAULT_BLOCK_SIZE
        )
        self.default_cache_type = default_cache_type if default_cache_type else "bytes"
        self.max_workers = max_workers
        self.s3_additional_kwargs = s3_additional_kwargs if s3_additional_kwargs else {}

        requester_pays = kwargs.pop("requester_pays", False)
        self.request_kwargs = {"RequestPayer": "requester"} if requester_pays else {}

    def _get_client_compatible_with_s3fs(self, **kwargs) -> BaseClient:
        """
        https://github.com/fsspec/s3fs/blob/2023.4.0/s3fs/core.py#L457-L535
        """
        from pyathena.connection import Connection

        config_kwargs = deepcopy(kwargs.pop("config_kwargs", {}))
        user_agent_extra = config_kwargs.pop("user_agent_extra", None)
        if user_agent_extra:
            if pyathena.user_agent_extra not in user_agent_extra:
                config_kwargs.update(
                    {"user_agent_extra": f"{pyathena.user_agent_extra} {user_agent_extra}"}
                )
            else:
                config_kwargs.update({"user_agent_extra": user_agent_extra})
        else:
            config_kwargs.update({"user_agent_extra": pyathena.user_agent_extra})
        connect_timeout = kwargs.pop("connect_timeout", None)
        if connect_timeout:
            config_kwargs.update({"connect_timeout": connect_timeout})
        read_timeout = kwargs.pop("read_timeout", None)
        if read_timeout:
            config_kwargs.update({"read_timeout": read_timeout})

        client_kwargs = deepcopy(kwargs.pop("client_kwargs", {}))
        use_ssl = kwargs.pop("use_ssl", None)
        if use_ssl:
            client_kwargs.update({"use_ssl": use_ssl})
        endpoint_url = kwargs.pop("endpoint_url", None)
        if endpoint_url:
            client_kwargs.update({"endpoint_url": endpoint_url})
        anon = kwargs.pop("anon", False)
        if anon:
            config_kwargs.update({"signature_version": UNSIGNED})
        else:
            creds = dict(
                aws_access_key_id=kwargs.pop("key", kwargs.pop("username", None)),
                aws_secret_access_key=kwargs.pop("secret", kwargs.pop("password", None)),
                aws_session_token=kwargs.pop("token", None),
            )
            kwargs.update(**creds)
            client_kwargs.update(**creds)

        config = Config(**config_kwargs)
        session = Session(
            **{k: v for k, v in kwargs.items() if k in Connection._SESSION_PASSING_ARGS}
        )
        return session.client(
            "s3",
            config=config,
            **{k: v for k, v in client_kwargs.items() if k in Connection._CLIENT_PASSING_ARGS},
        )

    @staticmethod
    def parse_path(path: str) -> Tuple[str, Optional[str], Optional[str]]:
        match = S3FileSystem.PATTERN_PATH.search(path)
        if match:
            return match.group("bucket"), match.group("key"), match.group("version_id")
        else:
            raise ValueError(f"Invalid S3 path format {path}.")

    def _head_bucket(self, bucket, refresh: bool = False) -> Optional[S3Object]:
        if bucket not in self.dircache or refresh:
            try:
                self._call(
                    self._client.head_bucket,
                    Bucket=bucket,
                )
            except botocore.exceptions.ClientError as e:
                if e.response["Error"]["Code"] in ["NoSuchKey", "NoSuchBucket", "404"]:
                    return None
                raise
            file = S3Object(
                init={
                    "ContentLength": 0,
                    "ContentType": None,
                    "StorageClass": S3StorageClass.S3_STORAGE_CLASS_BUCKET,
                    "ETag": None,
                    "LastModified": None,
                },
                type=S3ObjectType.S3_OBJECT_TYPE_DIRECTORY,
                bucket=bucket,
                key=None,
                version_id=None,
            )
            self.dircache[bucket] = file
        else:
            file = self.dircache[bucket]
        return file

    def _head_object(
        self, path: str, version_id: Optional[str] = None, refresh: bool = False
    ) -> Optional[S3Object]:
        bucket, key, path_version_id = self.parse_path(path)
        version_id = path_version_id if path_version_id else version_id
        if path not in self.dircache or refresh:
            try:
                request = {
                    "Bucket": bucket,
                    "Key": key,
                }
                if version_id:
                    request.update({"VersionId": version_id})
                response = self._call(
                    self._client.head_object,
                    **request,
                )
            except botocore.exceptions.ClientError as e:
                if e.response["Error"]["Code"] in ["NoSuchKey", "NoSuchBucket", "404"]:
                    return None
                raise
            file = S3Object(
                init=response,
                type=S3ObjectType.S3_OBJECT_TYPE_FILE,
                bucket=bucket,
                key=key,
                version_id=version_id,
            )
            self.dircache[path] = file
        else:
            file = self.dircache[path]
        return file

    def _ls_buckets(self, refresh: bool = False) -> List[S3Object]:
        if "" not in self.dircache or refresh:
            try:
                response = self._call(
                    self._client.list_buckets,
                )
            except botocore.exceptions.ClientError as e:
                if e.response["Error"]["Code"] in ["AccessDenied", "403"]:
                    return []
                raise
            buckets = [
                S3Object(
                    init={
                        "ContentLength": 0,
                        "ContentType": None,
                        "StorageClass": S3StorageClass.S3_STORAGE_CLASS_BUCKET,
                        "ETag": None,
                        "LastModified": None,
                    },
                    type=S3ObjectType.S3_OBJECT_TYPE_DIRECTORY,
                    bucket=b["Name"],
                    key=None,
                    version_id=None,
                )
                for b in response["Buckets"]
            ]
            self.dircache[""] = buckets
        else:
            buckets = self.dircache[""]
        return buckets

    def _ls_dirs(
        self,
        path: str,
        prefix: str = "",
        delimiter: str = "/",
        next_token: Optional[str] = None,
        max_keys: Optional[int] = None,
        refresh: bool = False,
    ) -> List[S3Object]:
        bucket, key, version_id = self.parse_path(path)
        if key:
            prefix = f"{key}/{prefix if prefix else ''}"
        if path not in self.dircache or refresh:
            files: List[S3Object] = []
            while True:
                request: Dict[Any, Any] = {
                    "Bucket": bucket,
                    "Prefix": prefix,
                    "Delimiter": delimiter,
                }
                if next_token:
                    request.update({"ContinuationToken": next_token})
                if max_keys:
                    request.update({"MaxKeys": max_keys})
                response = self._call(
                    self._client.list_objects_v2,
                    **request,
                )
                files.extend(
                    S3Object(
                        init={
                            "ContentLength": 0,
                            "ContentType": None,
                            "StorageClass": S3StorageClass.S3_STORAGE_CLASS_DIRECTORY,
                            "ETag": None,
                            "LastModified": None,
                        },
                        type=S3ObjectType.S3_OBJECT_TYPE_DIRECTORY,
                        bucket=bucket,
                        key=c["Prefix"][:-1],
                        version_id=version_id,
                    )
                    for c in response.get("CommonPrefixes", [])
                )
                files.extend(
                    S3Object(
                        init=c,
                        type=S3ObjectType.S3_OBJECT_TYPE_FILE,
                        bucket=bucket,
                        key=c["Key"],
                    )
                    for c in response.get("Contents", [])
                )
                next_token = response.get("NextContinuationToken")
                if not next_token:
                    break
            if files:
                self.dircache[path] = files
        else:
            files = self.dircache[path]
        return files

    def ls(
        self, path: str, detail: bool = False, refresh: bool = False, **kwargs
    ) -> Union[List[S3Object], List[str]]:
        path = self._strip_protocol(path).rstrip("/")
        if path in ["", "/"]:
            files = self._ls_buckets(refresh)
        else:
            files = self._ls_dirs(path, refresh=refresh)
            if not files and "/" in path:
                file = self._head_object(path, refresh=refresh)
                if file:
                    files = [file]
        return [f for f in files] if detail else [f.name for f in files]

    def info(self, path: str, **kwargs) -> S3Object:
        refresh = kwargs.pop("refresh", False)
        path = self._strip_protocol(path)
        bucket, key, path_version_id = self.parse_path(path)
        version_id = path_version_id if path_version_id else kwargs.pop("version_id", None)
        if path in ["/", ""]:
            return S3Object(
                init={
                    "ContentLength": 0,
                    "ContentType": None,
                    "StorageClass": S3StorageClass.S3_STORAGE_CLASS_DIRECTORY,
                    "ETag": None,
                    "LastModified": None,
                },
                type=S3ObjectType.S3_OBJECT_TYPE_DIRECTORY,
                bucket=bucket,
                key=path,
                version_id=version_id,
            )
        if not refresh:
            caches: Union[List[S3Object], S3Object] = self._ls_from_cache(path)
            if caches is not None:
                if isinstance(caches, list):
                    cache = next((c for c in caches if c.name == path), None)
                elif caches.name == path:
                    cache = caches
                else:
                    cache = None

                if cache:
                    return cache
                else:
                    return S3Object(
                        init={
                            "ContentLength": 0,
                            "ContentType": None,
                            "StorageClass": S3StorageClass.S3_STORAGE_CLASS_DIRECTORY,
                            "ETag": None,
                            "LastModified": None,
                        },
                        type=S3ObjectType.S3_OBJECT_TYPE_DIRECTORY,
                        bucket=bucket,
                        key=path,
                        version_id=version_id,
                    )
        if key:
            info = self._head_object(path, refresh=refresh, version_id=version_id)
            if info:
                return info

        response = self._call(
            self._client.list_objects_v2,
            Bucket=bucket,
            Prefix=f"{key.rstrip('/')}/" if key else "",
            Delimiter="/",
            MaxKeys=1,
        )
        if (
            response.get("KeyCount", 0) > 0
            or response.get("Contents", [])
            or response.get("CommonPrefixes", [])
        ):
            return S3Object(
                init={
                    "ContentLength": 0,
                    "ContentType": None,
                    "StorageClass": S3StorageClass.S3_STORAGE_CLASS_DIRECTORY,
                    "ETag": None,
                    "LastModified": None,
                },
                type=S3ObjectType.S3_OBJECT_TYPE_DIRECTORY,
                bucket=bucket,
                key=path,
                version_id=version_id,
            )
        else:
            raise FileNotFoundError(path)

    def find(
        self,
        path: str,
        maxdepth: Optional[int] = None,
        withdirs: Optional[bool] = None,
        detail: bool = False,
        **kwargs,
    ) -> Union[Dict[str, S3Object], List[str]]:
        # TODO: Support maxdepth and withdirs
        path = self._strip_protocol(path)
        if path in ["", "/"]:
            raise ValueError("Cannot traverse all files in S3.")
        bucket, key, _ = self.parse_path(path)
        prefix = kwargs.pop("prefix", "")

        files = self._ls_dirs(path, prefix=prefix, delimiter="")
        if not files and key:
            try:
                files = [self.info(path)]
            except FileNotFoundError:
                files = []
        if detail:
            return {f.name: f for f in files}
        else:
            return [f.name for f in files]

    def exists(self, path: str, **kwargs) -> bool:
        path = self._strip_protocol(path)
        if path in ["", "/"]:
            # The root always exists.
            return True
        bucket, key, _ = self.parse_path(path)
        if key:
            try:
                if self._ls_from_cache(path):
                    return True
                info = self.info(path)
                if info:
                    return True
                else:
                    return False
            except FileNotFoundError:
                return False
        elif self.dircache.get(bucket, False):
            return True
        else:
            try:
                if self._ls_from_cache(bucket):
                    return True
            except FileNotFoundError:
                pass
            file = self._head_bucket(bucket)
            if file:
                return True
            else:
                return False

    def rm_file(self, path: str) -> None:
        # TODO
        raise NotImplementedError  # pragma: no cover

    def rmdir(self, path: str) -> None:
        # TODO
        raise NotImplementedError  # pragma: no cover

    def _rm(self, path: str) -> None:
        # TODO
        raise NotImplementedError  # pragma: no cover

    def touch(self, path: str, truncate: bool = True, **kwargs) -> Dict[str, Any]:
        bucket, key, version_id = self.parse_path(path)
        if version_id:
            raise ValueError("Cannot touch the file with the version specified.")
        if not truncate and self.exists(path):
            raise ValueError("Cannot touch the existing file without specifying truncate.")
        if not key:
            raise ValueError("Cannot touch the bucket.")

        object_ = self._put_object(bucket=bucket, key=key, body=None, **kwargs)
        self.invalidate_cache(path)
        return object_.to_dict()

    def cp_file(self, path1: str, path2: str, **kwargs):
        # TODO
        raise NotImplementedError  # pragma: no cover

    def cat_file(
        self, path: str, start: Optional[int] = None, end: Optional[int] = None, **kwargs
    ) -> bytes:
        bucket, key, version_id = self.parse_path(path)
        if start is not None and end is not None:
            ranges = (start, end)
        else:
            ranges = None

        return self._get_object(
            bucket=bucket,
            key=cast(str, key),
            ranges=ranges,
            version_id=version_id,
            **kwargs,
        )[1]

    def pipe_file(self, path: str, value, **kwargs):
        # TODO
        raise NotImplementedError  # pragma: no cover

    def put_file(self, lpath: str, rpath: str, callback=_DEFAULT_CALLBACK, **kwargs):
        # TODO
        raise NotImplementedError  # pragma: no cover

    def get_file(self, rpath: str, lpath: str, callback=_DEFAULT_CALLBACK, outfile=None, **kwargs):
        # TODO
        raise NotImplementedError  # pragma: no cover

    def checksum(self, path: str):
        # TODO
        raise NotImplementedError  # pragma: no cover

    def sign(self, path: str, expiration: int = 100, **kwargs):
        # TODO
        raise NotImplementedError  # pragma: no cover

    def created(self, path: str) -> datetime:
        return self.modified(path)

    def modified(self, path: str) -> datetime:
        info = self.info(path)
        return cast(datetime, info.get("last_modified"))

    def invalidate_cache(self, path: Optional[str] = None) -> None:
        if path is None:
            self.dircache.clear()
        else:
            path = self._strip_protocol(path)
            while path:
                self.dircache.pop(path, None)
                path = self._parent(path)

    def _open(
        self,
        path: str,
        mode: str = "rb",
        block_size: Optional[int] = None,
        cache_type: Optional[str] = None,
        autocommit: bool = True,
        cache_options: Optional[Dict[Any, Any]] = None,
        **kwargs,
    ) -> S3File:
        if block_size is None:
            block_size = self.default_block_size
        if cache_type is None:
            cache_type = self.default_cache_type

        return S3File(
            self,
            path,
            mode,
            version_id=None,
            max_workers=self.max_workers,
            block_size=block_size,
            cache_type=cache_type,
            autocommit=autocommit,
            cache_options=cache_options,
            s3_additional_kwargs=self.s3_additional_kwargs,
            **kwargs,
        )

    def _get_object(
        self,
        bucket: str,
        key: str,
        ranges: Optional[Tuple[int, int]],
        version_id: Optional[str] = None,
        **kwargs,
    ) -> Tuple[int, bytes]:
        request = {"Bucket": bucket, "Key": key}
        if ranges:
            range_ = f"bytes={ranges[0]}-{ranges[1] - 1}"
            request.update({"Range": range_})
        else:
            ranges = (0, 0)
            range_ = "bytes=0-"
        if version_id:
            request.update({"VersionId": version_id})

        _logger.debug(f"Get object: s3://{bucket}/{key}?versionId={version_id}&range={range_}")
        response = self._call(
            self._client.get_object,
            **request,
            **kwargs,
        )
        return ranges[0], cast(bytes, response["Body"].read())

    def _put_object(self, bucket: str, key: str, body: Optional[bytes], **kwargs) -> S3PutObject:
        request: Dict[str, Any] = {"Bucket": bucket, "Key": key}
        if body:
            request.update({"Body": body})

        _logger.debug(f"Put object: s3://{bucket}/{key}")
        response = self._call(
            self._client.put_object,
            **request,
            **kwargs,
        )
        return S3PutObject(response)

    def _create_multipart_upload(self, bucket: str, key: str, **kwargs) -> S3MultipartUpload:
        request = {
            "Bucket": bucket,
            "Key": key,
        }

        _logger.debug(f"Create multipart upload to s3://{bucket}/{key}.")
        response = self._call(
            self._client.create_multipart_upload,
            **request,
            **kwargs,
        )
        return S3MultipartUpload(response)

    def _upload_part_copy(
        self,
        bucket: str,
        key: str,
        copy_source: str,
        upload_id: str,
        part_number: int,
        **kwargs,
    ) -> S3MultipartUploadPart:
        request = {
            "Bucket": bucket,
            "Key": key,
            "CopySource": copy_source,
            "UploadId": upload_id,
            "PartNumber": part_number,
        }

        _logger.debug(
            f"Upload part copy from {copy_source} to s3://{bucket}/{key} as part {part_number}."
        )
        response = self._call(
            self._client.upload_part_copy,
            **request,
            **kwargs,
        )
        return S3MultipartUploadPart(part_number, response)

    def _upload_part(
        self,
        bucket: str,
        key: str,
        upload_id: str,
        part_number: int,
        body: bytes,
        **kwargs,
    ) -> S3MultipartUploadPart:
        request = {
            "Bucket": bucket,
            "Key": key,
            "UploadId": upload_id,
            "PartNumber": part_number,
            "Body": body,
        }

        _logger.debug(f"Upload part of {upload_id} to s3://{bucket}/{key} as part {part_number}.")
        response = self._call(
            self._client.upload_part,
            **request,
            **kwargs,
        )
        return S3MultipartUploadPart(part_number, response)

    def _complete_multipart_upload(
        self, bucket: str, key: str, upload_id: str, parts: List[Dict[str, Any]], **kwargs
    ) -> S3CompleteMultipartUpload:
        request = {
            "Bucket": bucket,
            "Key": key,
            "UploadId": upload_id,
            "MultipartUpload": {"Parts": parts},
        }

        _logger.debug(f"Complete multipart upload {upload_id} to s3://{bucket}/{key}.")
        response = self._call(
            self._client.complete_multipart_upload,
            **request,
            **kwargs,
        )
        return S3CompleteMultipartUpload(response)

    def _call(self, method: Union[str, Callable[..., Any]], **kwargs) -> Dict[str, Any]:
        if isinstance(method, str):
            func = getattr(self._client, method)
        else:
            func = method
        response = retry_api_call(
            func, config=self._retry_config, logger=_logger, **kwargs, **self.request_kwargs
        )
        return cast(Dict[str, Any], response)


class S3File(AbstractBufferedFile):
    def __init__(
        self,
        fs: S3FileSystem,
        path: str,
        mode: str = "rb",
        version_id: Optional[str] = None,
        max_workers: int = (cpu_count() or 1) * 5,
        block_size: int = S3FileSystem.DEFAULT_BLOCK_SIZE,
        cache_type: str = "bytes",
        autocommit: bool = True,
        cache_options: Optional[Dict[Any, Any]] = None,
        size: Optional[int] = None,
        s3_additional_kwargs: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.max_workers = max_workers
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        self.s3_additional_kwargs = s3_additional_kwargs if s3_additional_kwargs else {}

        super().__init__(
            fs=fs,
            path=path,
            mode=mode,
            block_size=block_size,
            autocommit=autocommit,
            cache_type=cache_type,
            cache_options=cache_options,
            size=size,
        )
        bucket, key, path_version_id = S3FileSystem.parse_path(path)
        self.bucket = bucket
        if not key:
            raise ValueError("The path does not contain a key.")
        self.key = key
        if version_id and path_version_id:
            if version_id != path_version_id:
                raise ValueError(
                    f"The version_id: {version_id} specified in the argument and "
                    f"the version_id: {path_version_id} specified in the path do not match."
                )
            self.version_id: Optional[str] = version_id
        elif path_version_id:
            self.version_id = path_version_id
        else:
            self.version_id = version_id
        if "r" not in mode and block_size < self.fs.MULTIPART_UPLOAD_MINIMUM_SIZE:
            # When writing occurs, the block size should not be smaller
            # than the minimum size of a part in a multipart upload.
            raise ValueError(f"Block size must be >= {self.fs.MULTIPART_UPLOAD_MINIMUM_SIZE}MB.")

        self.append_block = False
        if "r" in mode:
            info = self.fs.info(self.path, version_id=self.version_id)
            if etag := info.get("etag"):
                self.s3_additional_kwargs.update({"IfMatch": etag})
            self._details = info
        elif "a" in mode and self.fs.exists(path):
            self.append_block = True
            info = self.fs.info(self.path, version_id=self.version_id)
            loc = info.get("size", 0)
            if loc < self.fs.MULTIPART_UPLOAD_MINIMUM_SIZE:
                self.write(self.fs.cat(self.path))
            self.loc = loc
            self.s3_additional_kwargs.update(info.to_api_repr())
            self._details = info
        else:
            self._details = {}

        self.multipart_upload: Optional[S3MultipartUpload] = None
        self.multipart_upload_parts: List[Future[S3MultipartUploadPart]] = []

    def close(self) -> None:
        super().close()
        self._executor.shutdown()

    def _initiate_upload(self) -> None:
        if self.tell() < self.blocksize:
            # Files smaller than block size in size cannot be multipart uploaded.
            return

        self.multipart_upload = self.fs._create_multipart_upload(
            bucket=self.bucket,
            key=self.key,
            **self.s3_additional_kwargs,
        )
        if self.append_block:
            self.multipart_upload_parts.append(
                self._executor.submit(
                    self.fs._upload_part_copy,
                    bucket=self.bucket,
                    key=self.key,
                    copy_source=self.path,
                    upload_id=cast(str, cast(S3MultipartUpload, self.multipart_upload).upload_id),
                    part_number=1,
                )
            )

    def _upload_chunk(self, final: bool = False) -> bool:
        if self.tell() < self.blocksize:
            # Files smaller than block size in size cannot be multipart uploaded.
            if self.autocommit and final:
                self.commit()
            return True

        if not self.multipart_upload:
            raise RuntimeError("Multipart upload is not initialized.")

        part_number = len(self.multipart_upload_parts)
        self.buffer.seek(0)
        while data := self.buffer.read(self.blocksize):
            part_number += 1
            self.multipart_upload_parts.append(
                self._executor.submit(
                    self.fs._upload_part,
                    bucket=self.bucket,
                    key=self.key,
                    upload_id=cast(str, self.multipart_upload.upload_id),
                    part_number=part_number,
                    body=data,
                )
            )

        if self.autocommit and final:
            self.commit()
        return True

    def commit(self) -> None:
        if self.tell() == 0:
            if self.buffer is not None:
                self.discard()
                self.fs.touch(self.path, **self.s3_additional_kwargs)
        elif not self.multipart_upload_parts:
            if self.buffer is not None:
                # Upload files smaller than block size.
                self.buffer.seek(0)
                data = self.buffer.read()
                self.fs._put_object(
                    bucket=self.bucket,
                    key=self.key,
                    body=data,
                    **self.s3_additional_kwargs,
                )
        else:
            if not self.multipart_upload:
                raise RuntimeError("Multipart upload is not initialized.")

            parts: List[Dict[str, Any]] = []
            for f in as_completed(self.multipart_upload_parts):
                result = f.result()
                part = {
                    "ETag": result.etag,
                    "PartNumber": result.part_number,
                }
                if result.checksum_sha256:
                    part.update({"ChecksumSHA256": result.checksum_sha256})
                parts.append(part)
            parts.sort(key=lambda x: x["PartNumber"])
            self.fs._complete_multipart_upload(
                bucket=self.bucket,
                key=self.key,
                upload_id=cast(str, self.multipart_upload.upload_id),
                parts=parts,
            )

        self.fs.invalidate_cache(self.path)

    def discard(self) -> None:
        if self.multipart_upload:
            for f in self.multipart_upload_parts:
                f.cancel()
            self.fs._call(
                "abort_multipart_upload",
                Bucket=self.bucket,
                Key=self.key,
                UploadId=self.multipart_upload.upload_id,
                **self.s3_additional_kwargs,
            )

        self.multipart_upload = None
        self.multipart_upload_parts = []

    def _fetch_range(self, start: int, end: int) -> bytes:
        ranges = self._get_ranges(
            start, end, max_workers=self.max_workers, worker_block_size=self.blocksize
        )
        if len(ranges) > 1:
            object_ = self._merge_objects(
                list(
                    self._executor.map(
                        lambda bucket, key, ranges, version_id, kwargs: self.fs._get_object(
                            bucket=bucket,
                            key=key,
                            ranges=ranges,
                            version_id=version_id,
                            **kwargs,
                        ),
                        itertools.repeat(self.bucket),
                        itertools.repeat(self.key),
                        ranges,
                        itertools.repeat(self.version_id),
                        itertools.repeat(self.s3_additional_kwargs),
                    )
                )
            )
        else:
            object_ = self.fs._get_object(
                self.bucket,
                self.key,
                ranges[0],
                self.version_id,
                **self.s3_additional_kwargs,
            )[1]
        return object_

    @staticmethod
    def _get_ranges(
        start: int, end: int, max_workers: int, worker_block_size: int
    ) -> List[Tuple[int, int]]:
        ranges = []
        range_size = end - start
        if max_workers > 1 and range_size > worker_block_size:
            range_start = start
            while True:
                range_end = range_start + worker_block_size
                if range_end > end:
                    ranges.append((range_start, end))
                    break
                else:
                    ranges.append((range_start, range_end))
                    range_start += worker_block_size
        else:
            ranges.append((start, end))
        return ranges

    @staticmethod
    def _merge_objects(objects: List[Tuple[int, bytes]]) -> bytes:
        objects.sort(key=lambda x: x[0])
        return b"".join([obj for start, obj in objects])
