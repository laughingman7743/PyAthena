# -*- coding: utf-8 -*-
from __future__ import annotations

import copy
import logging
from datetime import datetime
from typing import Any, Dict, Iterator, MutableMapping, Optional

_logger = logging.getLogger(__name__)  # type: ignore

_API_FIELD_TO_S3_OBJECT_PROPERTY = {
    "ETag": "etag",
    "CacheControl": "cache_control",
    "ContentDisposition": "content_disposition",
    "ContentEncoding": "content_encoding",
    "ContentLanguage": "content_language",
    "ContentLength": "content_length",
    "ContentType": "content_type",
    "Expires": "expires",
    "WebsiteRedirectLocation": "website_redirect_location",
    "ServerSideEncryption": "server_side_encryption",
    "SSECustomerAlgorithm": "sse_customer_algorithm",
    "SSEKMSKeyId": "sse_kms_key_id",
    "BucketKeyEnabled": "bucket_key_enabled",
    "StorageClass": "storage_class",
    "ObjectLockMode": "object_lock_mode",
    "ObjectLockRetainUntilDate": "object_lock_retain_until_date",
    "ObjectLockLegalHoldStatus": "object_lock_legal_hold_status",
    "Metadata": "metadata",
    "LastModified": "last_modified",
}


class S3ObjectType:
    """Constants for S3 object types in filesystem operations.

    These constants are used to distinguish between directories and files
    when working with S3 paths through the S3FileSystem interface.
    """

    S3_OBJECT_TYPE_DIRECTORY: str = "directory"
    S3_OBJECT_TYPE_FILE: str = "file"


class S3StorageClass:
    """Constants for Amazon S3 storage classes.

    S3 storage classes determine the availability, durability, and cost
    characteristics of stored objects. Each class is optimized for different
    access patterns and use cases.

    Storage classes:
        - STANDARD: Default storage for frequently accessed data
        - REDUCED_REDUNDANCY: Lower cost, reduced durability (deprecated)
        - STANDARD_IA: Infrequently accessed data with rapid retrieval
        - ONEZONE_IA: Lower cost IA storage in single availability zone
        - INTELLIGENT_TIERING: Automatic tiering between frequent/infrequent
        - GLACIER: Archive storage for long-term backup
        - DEEP_ARCHIVE: Lowest cost archive storage
        - GLACIER_IR: Archive with faster retrieval than standard Glacier
        - OUTPOSTS: Storage on AWS Outposts

    See Also:
        AWS S3 storage classes documentation:
        https://docs.aws.amazon.com/s3/latest/userguide/storage-class-intro.html
    """

    S3_STORAGE_CLASS_STANDARD: str = "STANDARD"
    S3_STORAGE_CLASS_REDUCED_REDUNDANCY: str = "REDUCED_REDUNDANCY"
    S3_STORAGE_CLASS_STANDARD_IA: str = "STANDARD_IA"
    S3_STORAGE_CLASS_ONEZONE_IA: str = "ONEZONE_IA"
    S3_STORAGE_CLASS_INTELLIGENT_TIERING: str = "INTELLIGENT_TIERING"
    S3_STORAGE_CLASS_GLACIER: str = "GLACIER"
    S3_STORAGE_CLASS_DEEP_ARCHIVE: str = "DEEP_ARCHIVE"
    S3_STORAGE_CLASS_OUTPOSTS: str = "OUTPOSTS"
    S3_STORAGE_CLASS_GLACIER_IR: str = "GLACIER_IR"

    S3_STORAGE_CLASS_BUCKET: str = "BUCKET"
    S3_STORAGE_CLASS_DIRECTORY: str = "DIRECTORY"


class S3Object(MutableMapping[str, Any]):
    """Represents an S3 object with metadata and filesystem-like properties.

    This class provides a dictionary-like interface to S3 object metadata,
    making it easier to work with S3 objects in filesystem operations.
    It handles the mapping between S3 API field names and more pythonic
    property names.

    The object supports both dictionary-style access and property-style
    access to metadata fields like content type, storage class, encryption
    settings, and object lock configurations.

    Example:
        >>> s3_obj = S3Object({"ContentType": "text/csv", "ContentLength": 1024})
        >>> print(s3_obj.content_type)  # "text/csv"
        >>> print(s3_obj["content_length"])  # 1024
        >>> s3_obj.storage_class = "STANDARD_IA"

    Note:
        This class is primarily used internally by S3FileSystem for
        representing S3 objects in filesystem operations.
    """

    def __init__(
        self,
        init: Dict[str, Any],
        **kwargs,
    ) -> None:
        if init:
            filtered = {}
            for k, v in init.items():
                if k not in _API_FIELD_TO_S3_OBJECT_PROPERTY:
                    continue
                filtered[_API_FIELD_TO_S3_OBJECT_PROPERTY[k]] = v
            if "StorageClass" not in init:
                # https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadObject.html#API_HeadObject_ResponseSyntax
                # Amazon S3 returns this header for all objects except for
                # S3 Standard storage class objects.
                filtered[_API_FIELD_TO_S3_OBJECT_PROPERTY["StorageClass"]] = (
                    S3StorageClass.S3_STORAGE_CLASS_STANDARD
                )
            super().update(filtered)
            if "Size" in init:
                self.content_length = init["Size"]
                self.size = init["Size"]
            elif "ContentLength" in init:
                self.size = init["ContentLength"]
            else:
                self.content_length = 0
                self.size = 0
        super().update({_API_FIELD_TO_S3_OBJECT_PROPERTY.get(k, k): v for k, v in kwargs.items()})
        if self.get("key") is None:
            self.name = self.get("bucket")
        else:
            self.name = f"{self.get('bucket')}/{self.get('key')}"

    def get(self, key: str, default: Any = None) -> Any:
        return super().get(key, default)

    def __getitem__(self, item: str) -> Any:
        return self.__dict__.get(item)

    def __getattr__(self, item: str):
        return self.get(item)

    def __setitem__(self, key: str, value: Any) -> None:
        self.__dict__[key] = value

    def __setattr__(self, attr: str, value: Any) -> None:
        self[attr] = value

    def __delitem__(self, key: str) -> None:
        del self.__dict__[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self.__dict__.keys())

    def __len__(self) -> int:
        return len(self.__dict__)

    def __str__(self):
        return str(self.__dict__)

    def to_dict(self) -> Dict[str, Any]:
        """Convert S3Object to dictionary representation.

        Returns:
            Deep copy of the object's attributes as a dictionary.
        """
        return copy.deepcopy(self.__dict__)

    def to_api_repr(self) -> Dict[str, Any]:
        fields = {}
        for k, v in _API_FIELD_TO_S3_OBJECT_PROPERTY.items():
            if k in ["ETag", "ContentLength", "LastModified"]:
                # Excluded from API representation
                continue
            field = self.get(v)
            if field is not None:
                fields[k] = field
        return fields


class S3PutObject:
    """Represents the response from an S3 PUT object operation.

    This class encapsulates the metadata returned when uploading an object
    to S3, including encryption details, versioning information, and
    integrity checksums.

    Attributes:
        expiration: Object expiration time if lifecycle policy applies.
        version_id: Version ID if bucket versioning is enabled.
        etag: Entity tag for the uploaded object.
        server_side_encryption: Server-side encryption method used.
        Various checksum properties: For data integrity verification.

    Note:
        This class is used internally by S3FileSystem operations and
        typically not instantiated directly by users.
    """

    def __init__(self, response: Dict[str, Any]) -> None:
        self._expiration: Optional[str] = response.get("Expiration")
        self._version_id: Optional[str] = response.get("VersionId")
        self._etag: Optional[str] = response.get("ETag")
        self._checksum_crc32: Optional[str] = response.get("ChecksumCRC32")
        self._checksum_crc32c: Optional[str] = response.get("ChecksumCRC32C")
        self._checksum_sha1: Optional[str] = response.get("ChecksumSHA1")
        self._checksum_sha256: Optional[str] = response.get("ChecksumSHA256")
        self._server_side_encryption = response.get("ServerSideEncryption")
        self._sse_customer_algorithm = response.get("SSECustomerAlgorithm")
        self._sse_customer_key_md5 = response.get("SSECustomerKeyMD5")
        self._sse_kms_key_id = response.get("SSEKMSKeyId")
        self._sse_kms_encryption_context = response.get("SSEKMSEncryptionContext")
        self._bucket_key_enabled = response.get("BucketKeyEnabled")
        self._request_charged = response.get("RequestCharged")

    @property
    def expiration(self) -> Optional[str]:
        return self._expiration

    @property
    def version_id(self) -> Optional[str]:
        return self._version_id

    @property
    def etag(self) -> Optional[str]:
        return self._etag

    @property
    def checksum_crc32(self) -> Optional[str]:
        return self._checksum_crc32

    @property
    def checksum_crc32c(self) -> Optional[str]:
        return self._checksum_crc32c

    @property
    def checksum_sha1(self) -> Optional[str]:
        return self._checksum_sha1

    @property
    def checksum_sha256(self) -> Optional[str]:
        return self._checksum_sha256

    @property
    def server_side_encryption(self) -> Optional[str]:
        return self._server_side_encryption

    @property
    def sse_customer_algorithm(self) -> Optional[str]:
        return self._sse_customer_algorithm

    @property
    def sse_customer_key_md5(self) -> Optional[str]:
        return self._sse_customer_key_md5

    @property
    def sse_kms_key_id(self) -> Optional[str]:
        return self._sse_kms_key_id

    @property
    def sse_kms_encryption_context(self) -> Optional[str]:
        return self._sse_kms_encryption_context

    @property
    def bucket_key_enabled(self) -> Optional[bool]:
        return self._bucket_key_enabled

    @property
    def request_charged(self) -> Optional[str]:
        return self._request_charged

    def to_dict(self) -> Dict[str, Any]:
        return copy.deepcopy(self.__dict__)


class S3MultipartUpload:
    """Represents an S3 multipart upload operation.

    This class manages the metadata for multipart uploads, which allow
    uploading large files in chunks for better reliability and performance.
    It tracks upload identifiers, encryption settings, and lifecycle rules.

    Attributes:
        bucket: S3 bucket name for the upload.
        key: Object key being uploaded.
        upload_id: Unique identifier for the multipart upload.
        server_side_encryption: Encryption method applied to the upload.
        abort_date/abort_rule_id: Lifecycle rule information for upload cleanup.

    Note:
        Used internally by S3FileSystem for large file upload operations.
    """

    def __init__(self, response: Dict[str, Any]) -> None:
        self._abort_date = response.get("AbortDate")
        self._abort_rule_id = response.get("AbortRuleId")
        self._bucket = response.get("Bucket")
        self._key = response.get("Key")
        self._upload_id = response.get("UploadId")
        self._server_side_encryption = response.get("ServerSideEncryption")
        self._sse_customer_algorithm = response.get("SSECustomerAlgorithm")
        self._sse_customer_key_md5 = response.get("SSECustomerKeyMD5")
        self._sse_kms_key_id = response.get("SSEKMSKeyId")
        self._sse_kms_encryption_context = response.get("SSEKMSEncryptionContext")
        self._bucket_key_enabled = response.get("BucketKeyEnabled")
        self._request_charged = response.get("RequestCharged")
        self._checksum_algorithm = response.get("ChecksumAlgorithm")

    @property
    def abort_date(self) -> Optional[datetime]:
        return self._abort_date

    @property
    def abort_rule_id(self) -> Optional[str]:
        return self._abort_rule_id

    @property
    def bucket(self) -> Optional[str]:
        return self._bucket

    @property
    def key(self) -> Optional[str]:
        return self._key

    @property
    def upload_id(self) -> Optional[str]:
        return self._upload_id

    @property
    def server_side_encryption(self) -> Optional[str]:
        return self._server_side_encryption

    @property
    def sse_customer_algorithm(self) -> Optional[str]:
        return self._sse_customer_algorithm

    @property
    def sse_customer_key_md5(self) -> Optional[str]:
        return self._sse_customer_key_md5

    @property
    def sse_kms_key_id(self) -> Optional[str]:
        return self._sse_kms_key_id

    @property
    def sse_kms_encryption_context(self) -> Optional[str]:
        return self._sse_kms_encryption_context

    @property
    def bucket_key_enabled(self) -> Optional[bool]:
        return self._bucket_key_enabled

    @property
    def request_charged(self) -> Optional[str]:
        return self._request_charged

    @property
    def checksum_algorithm(self) -> Optional[str]:
        return self._checksum_algorithm


class S3MultipartUploadPart:
    """Represents a single part in an S3 multipart upload operation.

    Each part in a multipart upload has its own metadata including checksums,
    encryption details, and part identification. This class manages that
    metadata and provides methods to convert it to API-compatible formats.

    Attributes:
        part_number: The sequential part number (1-based).
        etag: Entity tag for this specific part.
        checksum_*: Various integrity checksums for the part data.
        server_side_encryption: Encryption settings for this part.

    Note:
        Parts must be at least 5MB except for the last part. Used internally
        by S3FileSystem for chunked upload operations.
    """

    def __init__(self, part_number: int, response: Dict[str, Any]) -> None:
        self._part_number = part_number
        self._copy_source_version_id: Optional[str] = response.get("CopySourceVersionId")
        copy_part_result = response.get("CopyPartResult")
        if copy_part_result:
            self._last_modified: Optional[datetime] = copy_part_result.get("LastModified")
            self._etag: Optional[str] = copy_part_result.get("ETag")
            self._checksum_crc32: Optional[str] = copy_part_result.get("ChecksumCRC32")
            self._checksum_crc32c: Optional[str] = copy_part_result.get("ChecksumCRC32C")
            self._checksum_sha1: Optional[str] = copy_part_result.get("ChecksumSHA1")
            self._checksum_sha256: Optional[str] = copy_part_result.get("ChecksumSHA256")
        else:
            self._last_modified = None
            self._etag = response.get("ETag")
            self._checksum_crc32 = response.get("ChecksumCRC32")
            self._checksum_crc32c = response.get("ChecksumCRC32C")
            self._checksum_sha1 = response.get("ChecksumSHA1")
            self._checksum_sha256 = response.get("ChecksumSHA256")
        self._server_side_encryption: Optional[str] = response.get("ServerSideEncryption")
        self._sse_customer_algorithm: Optional[str] = response.get("SSECustomerAlgorithm")
        self._sse_customer_key_md5: Optional[str] = response.get("SSECustomerKeyMD5")
        self._sse_kms_key_id: Optional[str] = response.get("SSEKMSKeyId")
        self._bucket_key_enabled: Optional[bool] = response.get("BucketKeyEnabled")
        self._request_charged: Optional[str] = response.get("RequestCharged")

    @property
    def part_number(self) -> int:
        return self._part_number

    @property
    def copy_source_version_id(self) -> Optional[str]:
        return self._copy_source_version_id

    @property
    def last_modified(self) -> Optional[datetime]:
        return self._last_modified

    @property
    def etag(self) -> Optional[str]:
        return self._etag

    @property
    def checksum_crc32(self) -> Optional[str]:
        return self._checksum_crc32

    @property
    def checksum_crc32c(self) -> Optional[str]:
        return self._checksum_crc32c

    @property
    def checksum_sha1(self) -> Optional[str]:
        return self._checksum_sha1

    @property
    def checksum_sha256(self) -> Optional[str]:
        return self._checksum_sha256

    @property
    def server_side_encryption(self) -> Optional[str]:
        return self._server_side_encryption

    @property
    def sse_customer_algorithm(self) -> Optional[str]:
        return self._sse_customer_algorithm

    @property
    def sse_customer_key_md5(self) -> Optional[str]:
        return self._sse_customer_key_md5

    @property
    def sse_kms_key_id(self) -> Optional[str]:
        return self._sse_kms_key_id

    @property
    def bucket_key_enabled(self) -> Optional[bool]:
        return self._bucket_key_enabled

    @property
    def request_charged(self) -> Optional[str]:
        return self._request_charged

    def to_api_repr(self) -> Dict[str, Any]:
        return {
            "ETag": self.etag,
            "ChecksumCRC32": self.checksum_crc32,
            "ChecksumCRC32C": self.checksum_crc32c,
            "ChecksumSHA1": self.checksum_sha1,
            "ChecksumSHA256": self.checksum_sha256,
            "PartNumber": self.part_number,
        }


class S3CompleteMultipartUpload:
    """Represents the completion of an S3 multipart upload operation.

    This class encapsulates the final response when a multipart upload is
    completed, including the final object location, versioning information,
    and consolidated metadata from all parts.

    Attributes:
        location: Final S3 URL of the completed object.
        bucket: S3 bucket containing the object.
        key: Final object key.
        version_id: Version ID if bucket versioning is enabled.
        etag: Final entity tag of the complete object.
        server_side_encryption: Encryption applied to the final object.

    Note:
        This represents the successful completion of a multipart upload.
        Used internally by S3FileSystem operations.
    """

    def __init__(self, response: Dict[str, Any]) -> None:
        self._location: Optional[str] = response.get("Location")
        self._bucket: Optional[str] = response.get("Bucket")
        self._key: Optional[str] = response.get("Key")
        self._expiration: Optional[str] = response.get("Expiration")
        self._version_id: Optional[str] = response.get("VersionId")
        self._etag: Optional[str] = response.get("ETag")
        self._checksum_crc32: Optional[str] = response.get("ChecksumCRC32")
        self._checksum_crc32c: Optional[str] = response.get("ChecksumCRC32C")
        self._checksum_sha1: Optional[str] = response.get("ChecksumSHA1")
        self._checksum_sha256: Optional[str] = response.get("ChecksumSHA256")
        self._server_side_encryption = response.get("ServerSideEncryption")
        self._sse_kms_key_id = response.get("SSEKMSKeyId")
        self._bucket_key_enabled = response.get("BucketKeyEnabled")
        self._request_charged = response.get("RequestCharged")

    @property
    def location(self) -> Optional[str]:
        return self._location

    @property
    def bucket(self) -> Optional[str]:
        return self._bucket

    @property
    def key(self) -> Optional[str]:
        return self._key

    @property
    def expiration(self) -> Optional[str]:
        return self._expiration

    @property
    def version_id(self) -> Optional[str]:
        return self._version_id

    @property
    def etag(self) -> Optional[str]:
        return self._etag

    @property
    def checksum_crc32(self) -> Optional[str]:
        return self._checksum_crc32

    @property
    def checksum_crc32c(self) -> Optional[str]:
        return self._checksum_crc32c

    @property
    def checksum_sha1(self) -> Optional[str]:
        return self._checksum_sha1

    @property
    def checksum_sha256(self) -> Optional[str]:
        return self._checksum_sha256

    @property
    def server_side_encryption(self) -> Optional[str]:
        return self._server_side_encryption

    @property
    def sse_kms_key_id(self) -> Optional[str]:
        return self._sse_kms_key_id

    @property
    def bucket_key_enabled(self) -> Optional[bool]:
        return self._bucket_key_enabled

    @property
    def request_charged(self) -> Optional[str]:
        return self._request_charged

    def to_dict(self):
        return copy.deepcopy(self.__dict__)
