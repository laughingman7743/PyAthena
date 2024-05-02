# -*- coding: utf-8 -*-
import uuid
from itertools import chain

import fsspec
import pytest

from pyathena.filesystem.s3 import S3File, S3FileSystem
from tests import ENV
from tests.pyathena.conftest import connect


@pytest.fixture(scope="class")
def register_filesystem():
    fsspec.register_implementation("s3", "pyathena.filesystem.s3.S3FileSystem", clobber=True)
    fsspec.register_implementation("s3a", "pyathena.filesystem.s3.S3FileSystem", clobber=True)


@pytest.mark.usefixtures("register_filesystem")
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
    def fs(self, request):
        if not hasattr(request, "param"):
            setattr(request, "param", {})
        return S3FileSystem(connect(), **request.param)

    @pytest.mark.parametrize(
        ["fs", "start", "end", "target_data"],
        list(
            chain(
                *[
                    [
                        ({"default_block_size": x}, 0, 5, b"01234"),
                        ({"default_block_size": x}, 2, 7, b"23456"),
                        ({"default_block_size": x}, 0, 10, b"0123456789"),
                    ]
                    for x in (S3FileSystem.DEFAULT_BLOCK_SIZE, 3)
                ]
            )
        ),
        indirect=["fs"],
    )
    def test_read(self, fs, start, end, target_data):
        # lowest level access: use _get_object
        data = fs._get_object(
            ENV.s3_staging_bucket, ENV.s3_filesystem_test_file_key, ranges=(start, end)
        )
        assert data == (start, target_data), data
        with fs.open(
            f"s3://{ENV.s3_staging_bucket}/{ENV.s3_filesystem_test_file_key}", "rb"
        ) as file:
            # mid-level access: use _fetch_range
            data = file._fetch_range(start, end)
            assert data == target_data, data
            # high-level: use fileobj seek and read
            file.seek(start)
            data = file.read(end - start)
            assert data == target_data, data

    @pytest.mark.parametrize(
        ["base", "exp"],
        [
            (1, 2**10),
            (10, 2**10),
            (100, 2**10),
            (1, 2**20),
            (10, 2**20),
            (100, 2**20),
            (1000, 2**20),
        ],
    )
    def test_write(self, fs, base, exp):
        data = b"a" * (base * exp)
        path = f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/{uuid.uuid4()}.dat"
        with fs.open(path, "wb") as f:
            f.write(data)
        with fs.open(path, "rb") as f:
            actual = f.read()
            assert len(actual) == len(data)
            assert actual == data

    @pytest.mark.parametrize(
        ["base", "exp"],
        [
            (1, 2**10),
            (10, 2**10),
            (100, 2**10),
            (1, 2**20),
            (10, 2**20),
            (100, 2**20),
            (1000, 2**20),
        ],
    )
    def test_append(self, fs, base, exp):
        # TODO: Check the metadata is kept.
        data = b"a" * (base * exp)
        path = f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/{uuid.uuid4()}.dat"
        with fs.open(path, "ab") as f:
            f.write(data)
        extra = b"extra"
        with fs.open(path, "ab") as f:
            f.write(extra)
        with fs.open(path, "rb") as f:
            actual = f.read()
            assert len(actual) == len(data + extra)
            assert actual == data + extra

    def test_exists(self, fs):
        path = f"s3://{ENV.s3_staging_bucket}/{ENV.s3_filesystem_test_file_key}"
        assert fs.exists(path)

        not_exists_path = (
            f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/{uuid.uuid4()}"
        )
        assert not fs.exists(not_exists_path)

    def test_touch(self, fs):
        path = f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/{uuid.uuid4()}"
        assert not fs.exists(path)
        fs.touch(path)
        assert fs.exists(path)
        assert fs.size(path) == 0

        with fs.open(path, "wb") as f:
            f.write(b"data")
        assert fs.size(path) == 4
        fs.touch(path, truncate=True)
        assert fs.size(path) == 0

        with fs.open(path, "wb") as f:
            f.write(b"data")
        assert fs.size(path) == 4
        with pytest.raises(ValueError):
            fs.touch(path, truncate=False)
        assert fs.size(path) == 4

    def test_pandas_read_csv(self):
        import pandas

        df = pandas.read_csv(
            f"s3://{ENV.s3_staging_bucket}/{ENV.s3_filesystem_test_file_key}",
            header=None,
            names=["col"],
        )
        assert [(row["col"],) for _, row in df.iterrows()] == [(123456789,)]

    def test_pandas_write_csv(self):
        import pandas

        df = pandas.DataFrame({"a": [1], "b": [2]})
        path = f"s3://{ENV.s3_staging_bucket}/{ENV.s3_staging_key}{ENV.schema}/{uuid.uuid4()}.csv"
        df.to_csv(path, index=False)

        actual = pandas.read_csv(path)
        pandas.testing.assert_frame_equal(df, actual)


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
