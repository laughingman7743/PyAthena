# -*- coding: utf-8 -*-
import os
import random
import string


class Env:
    def __init__(self):
        self.region_name = os.getenv("AWS_DEFAULT_REGION")
        assert (
            self.region_name
        ), "Required environment variable `AWS_DEFAULT_REGION` not found."
        self.s3_staging_dir = os.getenv("AWS_ATHENA_S3_STAGING_DIR")
        assert (
            self.s3_staging_dir
        ), "Required environment variable `AWS_ATHENA_S3_STAGING_DIR` not found."
        self.s3_staging_bucket, self.s3_staging_key = self.s3_staging_dir.replace(
            "s3://", ""
        ).split("/", 1)
        self.work_group = os.getenv("AWS_ATHENA_WORKGROUP")
        assert (
            self.work_group
        ), "Required environment variable `AWS_ATHENA_WORKGROUP` not found."
        self.default_work_group = os.getenv("AWS_ATHENA_DEFAULT_WORKGROUP", "primary")
        self.schema = "pyathena_test_" + "".join(
            [random.choice(string.ascii_lowercase + string.digits) for _ in range(10)]
        )


ENV = Env()
