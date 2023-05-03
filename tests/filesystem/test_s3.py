# -*- coding: utf-8 -*-
from itertools import chain
from typing import Dict

import pytest

from pyathena.filesystem.s3 import S3File, S3FileSystem
from tests import ENV
from tests.conftest import connect


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

    @pytest.fixture(scope="class")
    def fs(self) -> Dict[str, S3FileSystem]:
        fs = {
            "default": S3FileSystem(connect()),
            "small_batches": S3FileSystem(connect(), default_block_size=3),
        }
        return fs

    @pytest.mark.parametrize(
        ["start", "end", "batch_mode", "target_data"],
        list(
            chain(
                *[
                    [
                        (0, 5, x, b"01234"),
                        (2, 7, x, b"23456"),
                        (0, 10, x, b"0123456789"),
                    ]
                    for x in ("default", "small_batches")
                ]
            )
        ),
    )
    def test_read(self, fs, start, end, batch_mode, target_data):
        # lowest level access: use _get_object
        data = fs[batch_mode]._get_object(
            ENV.s3_staging_bucket, ENV.s3_filesystem_test_file_key, ranges=(start, end)
        )
        assert data == (start, target_data), data
        with fs[batch_mode].open(
            f"s3://{ENV.s3_staging_bucket}/{ENV.s3_filesystem_test_file_key}", "rb"
        ) as file:
            # mid-level access: use _fetch_range
            data = file._fetch_range(start, end)
            assert data == target_data, data
            # high-level: use fileobj seek and read
            file.seek(start)
            data = file.read(end - start)
            assert data == target_data, data

    def test_compatibility_with_s3fs(self):
        import pandas

        df = pandas.read_csv(
            f"s3://{ENV.s3_staging_bucket}/{ENV.s3_filesystem_test_file_key}",
            header=None,
            names=["col"],
        )
        assert [(row["col"],) for _, row in df.iterrows()] == [(123456789,)]


class TestS3File:
    @pytest.mark.parametrize(
        ["objects", "target"],
        [
            ([(0, b"")], b""),
            ([(0, b"foo")], b"foo"),
            ([(0, b"foo"), (1, b"bar")], b"foobar"),
            ([(1, b"foo"), (0, b"bar")], b"barfoo"),
            ([(1, b""), (0, b"bar")], b"bar"),
            ([(1, b"foo"), (0, b"")], b"foo"),
            ([(2, b"foo"), (1, b"bar"), (3, b"baz")], b"barfoobaz"),
        ],
    )
    def test_merge_objects(self, objects, target):
        assert S3File._merge_objects(objects) == target

    @pytest.mark.parametrize(
        ["start", "end", "max_workers", "worker_block_size", "ranges"],
        [
            (42, 1337, 1, 999, [(42, 1337)]),  # single worker
            (42, 1337, 2, 999, [(42, 42 + 999), (42 + 999, 1337)]),  # more workers
            (
                42,
                1337,
                2,
                333,
                [
                    (42, 42 + 333),
                    (42 + 333, 42 + 666),
                    (42 + 666, 42 + 999),
                    (42 + 999, 1337),
                ],
            ),
            (42, 1337, 2, 1295, [(42, 1337)]),  # single block
            (42, 1337, 2, 1296, [(42, 1337)]),  # single block
            (42, 1337, 2, 1294, [(42, 1336), (1336, 1337)]),  # single block too small
        ],
    )
    def test_get_ranges(self, start, end, max_workers, worker_block_size, ranges):
        assert (
            S3File._get_ranges(
                start, end, max_workers=max_workers, worker_block_size=worker_block_size
            )
            == ranges
        )
