# -*- coding: utf-8 -*-
import os
import random
import string

BASE_PATH = os.path.dirname(os.path.abspath(__file__))
S3_PREFIX = "test_pyathena"
SCHEMA = "test_pyathena_" + "".join(
    [random.choice(string.ascii_lowercase + string.digits) for _ in range(10)]
)


class Env(object):
    def __init__(self):
        self.region_name = os.getenv("AWS_DEFAULT_REGION")
        assert (
            self.region_name
        ), "Required environment variable `AWS_DEFAULT_REGION` not found."
        self.s3_staging_dir = os.getenv("AWS_ATHENA_S3_STAGING_DIR")
        assert (
            self.s3_staging_dir
        ), "Required environment variable `AWS_ATHENA_S3_STAGING_DIR` not found."
        self.work_group = os.getenv("AWS_ATHENA_WORKGROUP")
        assert (
            self.s3_staging_dir
        ), "Required environment variable `AWS_ATHENA_WORKGROUP` not found."
        self.default_work_group = os.getenv("AWS_ATHENA_DEFAULT_WORKGROUP", "primary")


ENV = Env()
