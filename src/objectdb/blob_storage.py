"""Blob storage for Hetzner S3 (S3-compatible) object storage."""

from __future__ import annotations

import hashlib
import logging
from io import BytesIO
from typing import Self

import boto3
import botocore
import botocore.exceptions
import pydantic
from botocore.client import Config
from PIL import Image as PILImage, ImageOps
from streamlit.runtime.uploaded_file_manager import UploadedFile

logger = logging.getLogger(__name__)


class Media(pydantic.BaseModel):
    """Generic media container."""

    identifier: str
    data: bytes

    @classmethod
    def from_bytes(cls, data: bytes) -> Self:
        """Create media instance from raw bytes. Identifier is sha256 digest."""
        return cls(identifier=hashlib.sha256(data).hexdigest(), data=data)

    def __eq__(self, other: object) -> bool:
        """Equality by identifier, but only against same concrete type."""
        if not isinstance(other, type(self)):
            raise NotImplementedError
        return self.identifier == other.identifier

    def __hash__(self) -> int:
        """Uniqueness by identifier."""
        return int.from_bytes(self.identifier.encode("utf-8"), "big")


class Image(Media):
    """Uploaded image."""

    @classmethod
    def from_streamlit_uploader(cls, image: UploadedFile) -> Image:
        """Create Image from Streamlit uploader."""
        return cls.from_bytes(data=image.getvalue())

    def resize(self, width: int, height: int) -> Image:
        """Resize image to fit within target while padding with black bars."""
        with PILImage.open(BytesIO(self.data)) as pil_image:
            format_name = pil_image.format or "JPEG"
            resized = ImageOps.pad(
                pil_image, (width, height), method=PILImage.Resampling.LANCZOS, color=(0, 0, 0), centering=(0.5, 0.5)
            )
            if format_name.upper() in ("JPEG", "JPG") and resized.mode not in ("RGB", "L"):
                resized = resized.convert("RGB")
            buffer = BytesIO()
            resized.save(buffer, format=format_name)
            return Image.from_bytes(buffer.getvalue())


class Video(Media):
    """Uploaded video."""


class MediaHandler:
    """Generic S3-compatible media handler used by ImageHandler and VideoHandler."""

    def __init__(  # noqa: PLR0913
        self, endpoint: str, access_key: str, secret_key: str, region: str, bucket: str, path: str, content_type: str
    ) -> None:
        config = Config(signature_version="s3v4", s3={"payload_signing_enabled": False, "addressing_style": "virtual"})
        self.s3 = boto3.client(
            "s3",
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            endpoint_url=endpoint,
            region_name=region,
            config=config,
        )
        self.bucket = bucket
        self.path = path
        self.content_type = content_type

        try:
            self.s3.head_bucket(Bucket=self.bucket)
        except botocore.exceptions.ClientError:
            logger.exception("Bucket %s does not exist.", self.bucket)

    def upload(self, media: Media) -> None:
        """Upload media to S3-compatible storage if not already existing."""
        if not self.exists(media.identifier):
            self.s3.put_object(
                Bucket=self.bucket,
                Key=self.path + media.identifier,
                Body=media.data,
                IfNoneMatch="*",
                ContentType=self.content_type,
            )

    def _download(self, identifier: str) -> bytes:
        response = self.s3.get_object(Bucket=self.bucket, Key=self.path + identifier)
        return response["Body"].read()

    def get_url(self, identifier: str, expiry_minutes: int = 30) -> str:
        """Generate presigned URL for object identified by key."""
        expires_in_seconds = int(expiry_minutes * 60)
        return self.s3.generate_presigned_url(
            ClientMethod="get_object",
            Params={"Bucket": self.bucket, "Key": self.path + identifier},
            ExpiresIn=expires_in_seconds,
        )

    def exists(self, key: str) -> bool:
        """Check if object exists by key."""
        key = self.path + key
        try:
            self.s3.head_object(Bucket=self.bucket, Key=key)
        except botocore.exceptions.ClientError as exc:
            code = exc.response.get("Error", {}).get("Code")
            if code in ("404", "NotFound", "NoSuchKey"):
                return False
            raise
        return True


class ImageHandler(MediaHandler):
    """Image-specific handler (thin wrapper around MediaHandler)."""

    def __init__(self, endpoint: str, access_key: str, secret_key: str, region: str, bucket: str) -> None:
        super().__init__(endpoint, access_key, secret_key, region, bucket, path="pictures/", content_type="image/jpeg")

    def download(self, identifier: str) -> Image:
        """Download image from S3-compatible storage by identifier."""
        data = self._download(identifier)
        return Image(identifier=identifier, data=data)


class VideoHandler(MediaHandler):
    """Video-specific handler (thin wrapper around MediaHandler)."""

    def __init__(self, endpoint: str, access_key: str, secret_key: str, region: str, bucket: str) -> None:
        super().__init__(endpoint, access_key, secret_key, region, bucket, path="videos/", content_type="video/mp4")

    def download(self, identifier: str) -> Video:
        """Download video from S3-compatible storage by identifier."""
        data = self._download(identifier)
        return Video(identifier=identifier, data=data)
