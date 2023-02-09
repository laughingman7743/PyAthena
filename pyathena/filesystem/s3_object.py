# -*- coding: utf-8 -*-
import logging
from dataclasses import dataclass
from typing import Any, Dict, Optional

_logger = logging.getLogger(__name__)  # type: ignore


class S3ObjectType:
    S3_OBJECT_TYPE_DIRECTORY: str = "directory"
    S3_OBJECT_TYPE_FILE: str = "file"


class S3StorageClass:
    S3_STORAGE_CLASS_STANDARD = "STANDARD"
    S3_STORAGE_CLASS_REDUCED_REDUNDANCY = "REDUCED_REDUNDANCY"
    S3_STORAGE_CLASS_STANDARD_IA = "STANDARD_IA"
    S3_STORAGE_CLASS_ONEZONE_IA = "ONEZONE_IA"
    S3_STORAGE_CLASS_INTELLIGENT_TIERING = "INTELLIGENT_TIERING"
    S3_STORAGE_CLASS_GLACIER = "GLACIER"
    S3_STORAGE_CLASS_DEEP_ARCHIVE = "DEEP_ARCHIVE"
    S3_STORAGE_CLASS_OUTPOSTS = "OUTPOSTS"
    S3_STORAGE_CLASS_GLACIER_IR = "GLACIER_IR"

    S3_STORAGE_CLASS_BUCKET = "BUCKET"
    S3_STORAGE_CLASS_DIRECTORY = "DIRECTORY"


@dataclass
class S3Object:
    bucket: str
    key: Optional[str]
    size: int
    type: str
    storage_class: str
    etag: Optional[str]

    def __post_init__(self) -> None:
        if self.key is None:
            self.name = self.bucket
        else:
            self.name = f"{self.bucket}/{self.key}"

    def to_dict(self) -> Dict[str, Any]:
        return self.__dict__
