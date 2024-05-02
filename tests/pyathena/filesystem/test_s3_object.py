# -*- coding: utf-8 -*-
from datetime import datetime

from pyathena.filesystem.s3_object import S3Object, S3ObjectType, S3StorageClass


class TestS3Object:
    def test_init(self):
        actual = S3Object(
            init={
                "ContentLength": 0,
                "ContentType": None,
                "StorageClass": S3StorageClass.S3_STORAGE_CLASS_BUCKET,
                "ETag": None,
                "LastModified": None,
            },
            type=S3ObjectType.S3_OBJECT_TYPE_DIRECTORY,
            bucket="test-bucket",
            key=None,
            version_id=None,
        )
        assert actual.type == S3ObjectType.S3_OBJECT_TYPE_DIRECTORY
        assert actual.bucket == "test-bucket"
        assert actual.key is None
        assert actual.name == "test-bucket"
        assert actual.size == 0
        assert actual.content_type is None
        assert actual.storage_class == S3StorageClass.S3_STORAGE_CLASS_BUCKET
        assert actual.etag is None
        assert actual.version_id is None
        assert actual.last_modified is None

        actual = S3Object(
            init={
                "ContentLength": 100,
                "ContentType": "application/json",
                "StorageClass": S3StorageClass.S3_STORAGE_CLASS_STANDARD,
                "ETag": "test-etag",
                "LastModified": datetime(2024, 5, 2, 1, 2, 3),
            },
            type=S3ObjectType.S3_OBJECT_TYPE_FILE,
            bucket="test-bucket",
            key="path/to/object",
            version_id="latest",
        )
        assert actual.type == S3ObjectType.S3_OBJECT_TYPE_FILE
        assert actual.bucket == "test-bucket"
        assert actual.key == "path/to/object"
        assert actual.name == "test-bucket/path/to/object"
        assert actual.size == 100
        assert actual.storage_class == S3StorageClass.S3_STORAGE_CLASS_STANDARD
        assert actual.etag == "test-etag"
        assert actual.version_id == "latest"
        assert actual.last_modified == datetime(2024, 5, 2, 1, 2, 3)

    def test_to_api_repr(self):
        actual = S3Object(
            init={
                "ContentLength": 0,
                "ContentType": None,
                "StorageClass": S3StorageClass.S3_STORAGE_CLASS_BUCKET,
                "ETag": None,
                "LastModified": None,
            },
            type=S3ObjectType.S3_OBJECT_TYPE_DIRECTORY,
            bucket="test-bucket",
            key=None,
            version_id=None,
        )
        assert actual.to_api_repr() == {"ContentLength": 0, "StorageClass": "BUCKET"}

        actual = S3Object(
            init={
                "ContentLength": 100,
                "ContentType": "application/json",
                "StorageClass": S3StorageClass.S3_STORAGE_CLASS_STANDARD,
                "ETag": "test-etag",
                "LastModified": datetime(2024, 5, 2, 1, 2, 3),
            },
            type=S3ObjectType.S3_OBJECT_TYPE_FILE,
            bucket="test-bucket",
            key="path/to/object",
            version_id="latest",
        )
        assert actual.to_api_repr() == {
            "ContentLength": 100,
            "ContentType": "application/json",
            "ETag": "test-etag",
            "StorageClass": "STANDARD",
        }
