# -*- coding: utf-8 -*-
import itertools
import logging
import re
from concurrent.futures.thread import ThreadPoolExecutor
from multiprocessing import cpu_count
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Pattern, Tuple, cast

import botocore.exceptions
from fsspec import AbstractFileSystem
from fsspec.spec import AbstractBufferedFile

from pyathena.filesystem.s3_object import S3Object, S3ObjectType, S3StorageClass
from pyathena.util import retry_api_call

if TYPE_CHECKING:
    from pyathena.connection import Connection

_logger = logging.getLogger(__name__)  # type: ignore


class S3FileSystem(AbstractFileSystem):
    DEFAULT_BLOCK_SIZE: int = 5 * 2**20  # 5MB
    PATTERN_PATH: Pattern[str] = re.compile(
        r"(^s3://|^s3a://|^)(?P<bucket>[a-zA-Z0-9.\-_]+)(/(?P<key>[^?]+)|/)?"
        r"($|\?version(Id|ID|id|_id)=(?P<version_id>.+)$)"
    )

    protocol = ["s3", "s3a"]
    _extra_tokenize_attributes = ("default_block_size",)

    def __init__(
        self,
        connection: "Connection",
        default_block_size: Optional[int] = None,
        default_cache_type: Optional[str] = None,
        max_workers: int = (cpu_count() or 1) * 5,
        *args,
        **kwargs,
    ) -> None:
        super(S3FileSystem, self).__init__(*args, **kwargs)
        self._client = connection.session.client(
            "s3", region_name=connection.region_name, **connection._client_kwargs
        )
        self._retry_config = connection.retry_config
        self.default_block_size = (
            default_block_size if default_block_size else self.DEFAULT_BLOCK_SIZE
        )
        self.default_cache_type = default_cache_type if default_cache_type else "bytes"
        self.max_workers = max_workers

    @staticmethod
    def parse_path(path: str) -> Tuple[str, Optional[str], Optional[str]]:
        match = S3FileSystem.PATTERN_PATH.search(path)
        if match:
            return match.group("bucket"), match.group("key"), match.group("version_id")
        else:
            raise ValueError(f"Invalid S3 path format {path}.")

    def _head_bucket(self, bucket, refresh: bool = False) -> Optional[Dict[Any, Any]]:
        if bucket not in self.dircache or refresh:
            try:
                retry_api_call(
                    self._client.head_bucket,
                    config=self._retry_config,
                    logger=_logger,
                    Bucket=bucket,
                )
            except botocore.exceptions.ClientError as e:
                if e.response["Error"]["Code"] in ["NoSuchKey", "NoSuchBucket", "404"]:
                    return None
                raise
            file = S3Object(
                bucket=bucket,
                key=None,
                size=0,
                type=S3ObjectType.S3_OBJECT_TYPE_DIRECTORY,
                storage_class=S3StorageClass.S3_STORAGE_CLASS_BUCKET,
                etag=None,
            ).to_dict()
            self.dircache[bucket] = file
        else:
            file = self.dircache[bucket]
        return file

    def _head_object(self, path: str, refresh: bool = False) -> Optional[Dict[Any, Any]]:
        bucket, key, _ = self.parse_path(path)
        if path not in self.dircache or refresh:
            try:
                response = retry_api_call(
                    self._client.head_object,
                    config=self._retry_config,
                    logger=_logger,
                    Bucket=bucket,
                    Key=key,
                )
            except botocore.exceptions.ClientError as e:
                if e.response["Error"]["Code"] in ["NoSuchKey", "NoSuchBucket", "404"]:
                    return None
                raise
            file = S3Object(
                bucket=bucket,
                key=key,
                size=response["ContentLength"],
                type=S3ObjectType.S3_OBJECT_TYPE_FILE,
                storage_class=response.get(
                    "StorageClass", S3StorageClass.S3_STORAGE_CLASS_STANDARD
                ),
                etag=response["ETag"],
            ).to_dict()
            self.dircache[path] = file
        else:
            file = self.dircache[path]
        return file

    def _ls_buckets(self, refresh: bool = False) -> List[Dict[Any, Any]]:
        if "" not in self.dircache or refresh:
            try:
                response = retry_api_call(
                    self._client.list_buckets,
                    config=self._retry_config,
                    logger=_logger,
                )
            except botocore.exceptions.ClientError as e:
                if e.response["Error"]["Code"] in ["AccessDenied", "403"]:
                    return []
                raise
            buckets = [
                S3Object(
                    bucket=b["Name"],
                    key=None,
                    size=0,
                    type=S3ObjectType.S3_OBJECT_TYPE_DIRECTORY,
                    storage_class=S3StorageClass.S3_STORAGE_CLASS_BUCKET,
                    etag=None,
                ).to_dict()
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
        next_token=None,
        max_keys: Optional[int] = None,
        refresh: bool = False,
    ) -> List[Dict[Any, Any]]:
        bucket, key, _ = self.parse_path(path)
        if key:
            prefix = f"{key}/{prefix if prefix else ''}"
        if path not in self.dircache or refresh:
            files: List[Dict[Any, Any]] = []
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
                response = retry_api_call(
                    self._client.list_objects_v2,
                    config=self._retry_config,
                    logger=_logger,
                    **request,
                )
                files.extend(
                    S3Object(
                        bucket=bucket,
                        key=c["Prefix"][:-1],
                        size=0,
                        type=S3ObjectType.S3_OBJECT_TYPE_DIRECTORY,
                        storage_class=S3StorageClass.S3_STORAGE_CLASS_DIRECTORY,
                        etag=None,
                    ).to_dict()
                    for c in response.get("CommonPrefixes", [])
                )
                files.extend(
                    S3Object(
                        bucket=bucket,
                        key=c["Key"],
                        size=c["Size"],
                        type=S3ObjectType.S3_OBJECT_TYPE_FILE,
                        storage_class=c["StorageClass"],
                        etag=c["ETag"],
                    ).to_dict()
                    for c in response.get("Contents", [])
                )
                next_token = response.get("NextContinuationToken", None)
                if not next_token:
                    break
            if files:
                self.dircache[path] = files
        else:
            files = self.dircache[path]
        return files

    def ls(self, path, detail=False, refresh=False, **kwargs):
        path = self._strip_protocol(path).rstrip("/")
        if path in ["", "/"]:
            files = self._ls_buckets(refresh)
        else:
            files = self._ls_dirs(path, refresh=refresh)
            if not files and "/" in path:
                file = self._head_object(path, refresh=refresh)
                if file:
                    files = [file]
        return files if detail else [f["name"] for f in files]

    def info(self, path, **kwargs):
        refresh = kwargs.pop("refresh", False)
        path = self._strip_protocol(path)
        bucket, key, _ = self.parse_path(path)
        if path in ["/", ""]:
            return S3Object(
                bucket=bucket,
                key=path,
                size=0,
                type=S3ObjectType.S3_OBJECT_TYPE_DIRECTORY,
                storage_class=S3StorageClass.S3_STORAGE_CLASS_DIRECTORY,
                etag=None,
            ).to_dict()
        if not refresh:
            caches = self._ls_from_cache(path)
            if caches is not None:
                if isinstance(caches, list):
                    cache = next((c for c in caches if c["name"] == path), None)
                elif ("name" in caches) and (caches["name"] == path):
                    cache = caches
                else:
                    cache = None

                if cache:
                    return cache
                else:
                    return S3Object(
                        bucket=bucket,
                        key=path,
                        size=0,
                        type=S3ObjectType.S3_OBJECT_TYPE_DIRECTORY,
                        storage_class=S3StorageClass.S3_STORAGE_CLASS_DIRECTORY,
                        etag=None,
                    ).to_dict()
        if key:
            info = self._head_object(path, refresh=refresh)
            if info:
                return info

        response = retry_api_call(
            self._client.list_objects_v2,
            config=self._retry_config,
            logger=_logger,
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
                bucket=bucket,
                key=path,
                size=0,
                type=S3ObjectType.S3_OBJECT_TYPE_DIRECTORY,
                storage_class=S3StorageClass.S3_STORAGE_CLASS_DIRECTORY,
                etag=None,
            ).to_dict()
        else:
            raise FileNotFoundError(path)

    def find(self, path, maxdepth=None, withdirs=None, detail=False, **kwargs):
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
            return {f["name"]: f for f in files}
        else:
            return [f["name"] for f in files]

    def exists(self, path, **kwargs):
        path = self._strip_protocol(path)
        if path in ["", "/"]:
            # The root always exists.
            return True
        bucket, key, _ = self.parse_path(path)
        if key:
            try:
                if self._ls_from_cache(path):
                    return True
                file = self.info(path)
                if file:
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

    def cp_file(self, path1, path2, **kwargs):
        raise NotImplementedError  # pragma: no cover

    def _rm(self, path):
        raise NotImplementedError  # pragma: no cover

    def created(self, path):
        raise NotImplementedError  # pragma: no cover

    def modified(self, path):
        raise NotImplementedError  # pragma: no cover

    def sign(self, path, expiration=100, **kwargs):
        raise NotImplementedError  # pragma: no cover

    def _open(
        self,
        path,
        mode="rb",
        block_size=None,
        cache_type=None,
        autocommit=True,
        cache_options=None,
        **kwargs,
    ):
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
            **kwargs,
        )

    def _get_object(
        self,
        bucket: str,
        key: str,
        ranges: Tuple[int, int],
        version_id: Optional[str] = None,
        kwargs: Optional[Dict[Any, Any]] = None,
    ) -> Tuple[int, bytes]:
        range_ = f"bytes={ranges[0]}-{ranges[1] - 1}"
        request = {"Bucket": bucket, "Key": key, "Range": range_}
        if version_id:
            request.update({"versionId": version_id})
        kwargs = kwargs if kwargs else {}

        _logger.debug(f"Get object: s3://{bucket}/{key}?versionId={version_id} {range_}")
        response = retry_api_call(
            self._client.get_object,
            config=self._retry_config,
            logger=_logger,
            **request,
            **kwargs,
        )
        return ranges[0], cast(bytes, response["Body"].read())


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
    ):
        super(S3File, self).__init__(
            fs=fs,
            path=path,
            mode=mode,
            block_size=block_size * max_workers,
            autocommit=autocommit,
            cache_type=cache_type,
            cache_options=cache_options,
            size=size,
        )
        self.fs = fs
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
        self.max_workers = max_workers
        self.worker_block_size = block_size
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        if "r" in mode and "etag" in self.details:
            self.request_kwargs = {"IfMatch": self.details["etag"]}

    def close(self):
        super(S3File, self).close()
        self._executor.shutdown()

    def _initiate_upload(self):
        raise NotImplementedError  # pragma: no cover

    def _fetch_range(self, start, end):
        ranges = self._get_ranges(
            start, end, max_workers=self.max_workers, worker_block_size=self.worker_block_size
        )
        if len(ranges) > 1:
            object_ = self._merge_objects(
                list(
                    self._executor.map(
                        self.fs._get_object,
                        itertools.repeat(self.bucket),
                        itertools.repeat(self.key),
                        ranges,
                        itertools.repeat(self.version_id),
                        itertools.repeat(self.request_kwargs),
                    )
                )
            )
        else:
            object_ = self.fs._get_object(
                self.bucket, self.key, ranges[0], self.version_id, self.request_kwargs
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
