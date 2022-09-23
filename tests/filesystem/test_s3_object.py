# -*- coding: utf-8 -*-
from pyathena.filesystem.s3_object import S3Object, S3ObjectType, S3StorageClass


class TestS3Object:
    def test_init(self):
        actual = S3Object(
            bucket="test-bucket",
            key=None,
            size=0,
            type=S3ObjectType.S3_OBJECT_TYPE_DIRECTORY,
            storage_class=S3StorageClass.S3_STORAGE_CLASS_DIRECTORY,
            etag=None,
        )
        assert actual.bucket == "test-bucket"
        assert actual.key is None
        assert actual.name == "test-bucket"
        assert actual.size == 0
        assert actual.type == S3ObjectType.S3_OBJECT_TYPE_DIRECTORY
        assert actual.storage_class == S3StorageClass.S3_STORAGE_CLASS_DIRECTORY
        assert actual.etag is None

        actual = S3Object(
            bucket="test-bucket",
            key="path/to/object",
            size=100,
            type=S3ObjectType.S3_OBJECT_TYPE_FILE,
            storage_class=S3StorageClass.S3_STORAGE_CLASS_STANDARD,
            etag="etag",
        )
        assert actual.bucket == "test-bucket"
        assert actual.key == "path/to/object"
        assert actual.name == "test-bucket/path/to/object"
        assert actual.size == 100
        assert actual.type == S3ObjectType.S3_OBJECT_TYPE_FILE
        assert actual.storage_class == S3StorageClass.S3_STORAGE_CLASS_STANDARD
        assert actual.etag == "etag"
