# -*- coding: utf-8 -*-
from io import BytesIO

import boto3
import pytest

from pyathena.filesystem.s3 import S3FileSystem
from tests import ENV
from tests.conftest import connect


class TestS3FileSystem:

    s3_test_file_key = ENV.s3_staging_key + "/S3FileSystem__test_read.dat"

    def test_parse_path(self):
        actual = S3FileSystem.parse_path("s3://bucket")
        assert actual[0] == "bucket"
        assert actual[1] is None
        assert actual[2] is None

        actual = S3FileSystem.parse_path("s3://bucket/")
        assert actual[0] == "bucket"
        assert actual[1] is None
        assert actual[2] is None

        actual = S3FileSystem.parse_path("s3://bucket/path/to/obj")
        assert actual[0] == "bucket"
        assert actual[1] == "path/to/obj"
        assert actual[2] is None

        actual = S3FileSystem.parse_path("s3://bucket/path/to/obj?versionId=12345abcde")
        assert actual[0] == "bucket"
        assert actual[1] == "path/to/obj"
        assert actual[2] == "12345abcde"

        actual = S3FileSystem.parse_path("s3a://bucket")
        assert actual[0] == "bucket"
        assert actual[1] is None
        assert actual[2] is None

        actual = S3FileSystem.parse_path("s3a://bucket/")
        assert actual[0] == "bucket"
        assert actual[1] is None
        assert actual[2] is None

        actual = S3FileSystem.parse_path("s3a://bucket/path/to/obj")
        assert actual[0] == "bucket"
        assert actual[1] == "path/to/obj"
        assert actual[2] is None

        actual = S3FileSystem.parse_path("s3a://bucket/path/to/obj?versionId=12345abcde")
        assert actual[0] == "bucket"
        assert actual[1] == "path/to/obj"
        assert actual[2] == "12345abcde"

        actual = S3FileSystem.parse_path("bucket")
        assert actual[0] == "bucket"
        assert actual[1] is None
        assert actual[2] is None

        actual = S3FileSystem.parse_path("bucket/")
        assert actual[0] == "bucket"
        assert actual[1] is None
        assert actual[2] is None

        actual = S3FileSystem.parse_path("bucket/path/to/obj")
        assert actual[0] == "bucket"
        assert actual[1] == "path/to/obj"
        assert actual[2] is None

        actual = S3FileSystem.parse_path("bucket/path/to/obj?versionId=12345abcde")
        assert actual[0] == "bucket"
        assert actual[1] == "path/to/obj"
        assert actual[2] == "12345abcde"

        actual = S3FileSystem.parse_path("bucket/path/to/obj?versionID=12345abcde")
        assert actual[0] == "bucket"
        assert actual[1] == "path/to/obj"
        assert actual[2] == "12345abcde"

        actual = S3FileSystem.parse_path("bucket/path/to/obj?versionid=12345abcde")
        assert actual[0] == "bucket"
        assert actual[1] == "path/to/obj"
        assert actual[2] == "12345abcde"

        actual = S3FileSystem.parse_path("bucket/path/to/obj?version_id=12345abcde")
        assert actual[0] == "bucket"
        assert actual[1] == "path/to/obj"
        assert actual[2] == "12345abcde"

    def test_parse_path_invalid(self):
        with pytest.raises(ValueError):
            S3FileSystem.parse_path("http://bucket")

        with pytest.raises(ValueError):
            S3FileSystem.parse_path("s3://bucket?")

        with pytest.raises(ValueError):
            S3FileSystem.parse_path("s3://bucket?foo=bar")

        with pytest.raises(ValueError):
            S3FileSystem.parse_path("s3://bucket/path/to/obj?foo=bar")

        with pytest.raises(ValueError):
            S3FileSystem.parse_path("s3a://bucket?")

        with pytest.raises(ValueError):
            S3FileSystem.parse_path("s3a://bucket?foo=bar")

        with pytest.raises(ValueError):
            S3FileSystem.parse_path("s3a://bucket/path/to/obj?foo=bar")

    @pytest.fixture(scope="class")
    def fs(self):
        client = boto3.client("s3")
        client.upload_fileobj(
            BytesIO(b"0123456789"),
            ENV.s3_staging_bucket,
            self.s3_test_file_key,
        )
        fs = S3FileSystem(connect())
        return fs

    @pytest.mark.parametrize(
        ["start", "end", "target_data"],
        [(0, 5, b"01234"), (2, 7, b"23456")],
    )
    def test_read(self, fs, start, end, target_data):
        # lowest level access: use _get_object
        data = fs._get_object(ENV.s3_staging_bucket, self.s3_test_file_key, ranges=(start, end))
        assert data == (start, target_data), data
        with fs.open(f"s3://{ENV.s3_staging_bucket}/{self.s3_test_file_key}", "rb") as file:
            # mid-level access: use _fetch_range
            data = file._fetch_range(start, end)
            assert data == target_data, data
            # high-level: use fileobj seek and read
            file.seek(start)
            data = file.read(end - start)
            assert data == target_data, data
