# -*- coding: utf-8 -*-
import pytest

from pyathena.filesystem.s3 import S3FileSystem


class TestS3FileSystem:
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
