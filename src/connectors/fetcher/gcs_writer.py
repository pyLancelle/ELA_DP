"""
GCS Writer for the generic fetcher.
Handles direct upload of fetch results to Google Cloud Storage.
"""

import json
import logging
from pathlib import Path
from typing import Optional, Tuple

from google.cloud import storage

from .base import FetchResult

logger = logging.getLogger(__name__)


class GCSWriter:
    """Handles writing fetch results to GCS."""

    def __init__(
        self,
        destination: str,
        keep_local: bool = False,
        local_dir: Optional[Path] = None,
    ):
        """
        Initialize GCS writer.

        Args:
            destination: GCS path like gs://bucket/path/
            keep_local: If True, also save a local copy
            local_dir: Directory for local copies (required if keep_local is True)
        """
        self.destination = destination
        self.keep_local = keep_local
        self.local_dir = local_dir
        self._bucket_name, self._prefix = self._parse_gcs_path(destination)
        self._client = storage.Client()
        self._bucket = self._client.bucket(self._bucket_name)

    @staticmethod
    def _parse_gcs_path(path: str) -> Tuple[str, str]:
        """
        Parse gs://bucket/prefix into (bucket, prefix).

        Args:
            path: GCS path like gs://bucket/path/

        Returns:
            Tuple of (bucket_name, prefix)

        Raises:
            ValueError: If path is not a valid GCS path.
        """
        if not path.startswith("gs://"):
            raise ValueError(f"Invalid GCS path: {path}. Must start with gs://")

        path_without_scheme = path[5:]
        parts = path_without_scheme.split("/", 1)
        bucket = parts[0]

        if not bucket:
            raise ValueError(f"Invalid GCS path: {path}. Bucket name is empty.")

        prefix = ""
        if len(parts) > 1 and parts[1]:
            prefix = parts[1].rstrip("/") + "/"

        return bucket, prefix

    def _to_jsonl(self, data: list) -> str:
        """Convert list of dicts to JSONL string."""
        lines = []
        for item in data:
            lines.append(json.dumps(item, default=str, ensure_ascii=False))
        return "\n".join(lines) + "\n" if lines else ""

    def write(self, result: FetchResult) -> Optional[str]:
        """
        Write fetch result to GCS.

        Args:
            result: FetchResult to write.

        Returns:
            GCS URI of uploaded file, or None if no data.
        """
        if not result.data:
            logger.warning(f"No data to write for {result.service}/{result.data_type}")
            return None

        blob_name = f"{self._prefix}{result.filename}"
        blob = self._bucket.blob(blob_name)

        content = self._to_jsonl(result.data)
        blob.upload_from_string(content, content_type="application/x-ndjson")

        gcs_uri = f"gs://{self._bucket_name}/{blob_name}"
        logger.info(
            f"Uploaded {result.filename} to {gcs_uri} ({result.item_count} items)"
        )

        if self.keep_local and self.local_dir:
            local_path = self.local_dir / result.filename
            self.local_dir.mkdir(parents=True, exist_ok=True)
            local_path.write_text(content, encoding="utf-8")
            logger.info(f"Saved local copy to {local_path}")

        return gcs_uri


class LocalWriter:
    """Handles writing fetch results to local filesystem."""

    def __init__(self, output_dir: Path):
        """
        Initialize local writer.

        Args:
            output_dir: Directory to write files to.
        """
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def _to_jsonl(self, data: list) -> str:
        """Convert list of dicts to JSONL string."""
        lines = []
        for item in data:
            lines.append(json.dumps(item, default=str, ensure_ascii=False))
        return "\n".join(lines) + "\n" if lines else ""

    def write(self, result: FetchResult) -> Optional[str]:
        """
        Write fetch result to local filesystem.

        Args:
            result: FetchResult to write.

        Returns:
            Path to written file, or None if no data.
        """
        if not result.data:
            logger.warning(f"No data to write for {result.service}/{result.data_type}")
            return None

        output_path = self.output_dir / result.filename
        content = self._to_jsonl(result.data)
        output_path.write_text(content, encoding="utf-8")

        logger.info(f"Saved {result.filename} ({result.item_count} items)")
        return str(output_path)
