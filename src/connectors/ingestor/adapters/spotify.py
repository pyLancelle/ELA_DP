"""
Spotify Ingestor Adapter
------------------------
Scans GCS landing folder, detects file types, and ingests to BigQuery.
"""

import logging
import re
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from google.cloud import storage
from google.api_core import exceptions as gcp_exceptions

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent.parent))

from src.connectors.ingestor.base import IngestorAdapter, IngestResult

logger = logging.getLogger(__name__)

# Spotify data types available
SPOTIFY_DATA_TYPES = [
    "recently_played",
    "saved_tracks",
    "saved_albums",
    "playlists",
    "top_artists",
    "top_tracks",
    "followed_artists",
    "artist_enrichment",
    "album_enrichment",
]

# File pattern → config name mapping
FILE_PATTERNS = {
    r".*_artist_enrichment\.jsonl$": "artist_enrichment",
    r".*_album_enrichment\.jsonl$": "album_enrichment",
    r".*_recently_played\.jsonl$": "recently_played",
    r".*_saved_tracks\.jsonl$": "saved_tracks",
    r".*_saved_albums\.jsonl$": "saved_albums",
}

# Configs that have YAML definitions
SUPPORTED_CONFIGS = {
    "artist_enrichment",
    "album_enrichment",
    "recently_played",
    "saved_tracks",
    "saved_albums",
}


class SpotifyIngestorAdapter(IngestorAdapter):
    """Adapter that scans GCS and ingests Spotify data to BigQuery."""

    def __init__(self):
        self._configs_path = Path(__file__).parent.parent.parent / "spotify" / "configs"

    @property
    def service_name(self) -> str:
        return "spotify"

    @property
    def available_data_types(self) -> List[str]:
        return SPOTIFY_DATA_TYPES

    def _detect_data_type(self, filename: str) -> Optional[str]:
        """Detect data type from filename pattern."""
        for pattern, data_type in FILE_PATTERNS.items():
            if re.match(pattern, filename, re.IGNORECASE):
                return data_type
        return None

    def _scan_landing_folder(
        self, bucket_name: str, landing_path: str = "spotify/landing"
    ) -> Dict[str, List[str]]:
        """Scan GCS landing folder and group files by data type."""
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blobs = bucket.list_blobs(prefix=landing_path)

        files_by_type = defaultdict(list)
        unsupported_count = 0

        for blob in blobs:
            if not blob.name.endswith(".jsonl"):
                continue

            filename = Path(blob.name).name
            data_type = self._detect_data_type(filename)

            if data_type is None:
                logger.warning(f"Unknown file pattern: {filename}")
                unsupported_count += 1
                continue

            if data_type not in SUPPORTED_CONFIGS:
                logger.info(f"Config not available for '{data_type}': {filename}")
                unsupported_count += 1
                continue

            files_by_type[data_type].append(f"gs://{bucket_name}/{blob.name}")

        # Log summary
        logger.info(f"Scanned gs://{bucket_name}/{landing_path}")
        logger.info(f"Found {len(files_by_type)} data type(s) to process")
        for data_type, files in sorted(files_by_type.items()):
            logger.info(f"  - {data_type}: {len(files)} file(s)")
        if unsupported_count:
            logger.info(f"  - unsupported: {unsupported_count} file(s)")

        return dict(files_by_type)

    def _run_single_ingestion(
        self, data_type: str, env: str, dry_run: bool
    ) -> Tuple[bool, str]:
        """Run ingestion for a single data type."""
        from src.connectors.spotify.spotify_ingest import SpotifyIngestor

        config_path = self._configs_path / f"{data_type}.yaml"

        if not config_path.exists():
            return False, f"Config not found: {config_path}"

        try:
            logger.info(f"Ingesting {data_type}...")
            ingestor = SpotifyIngestor(config_path, env, dry_run)
            exit_code = ingestor.run()

            if exit_code == 0:
                logger.info(f"Successfully ingested {data_type}")
                return True, ""
            else:
                return False, f"Exit code {exit_code}"

        except Exception as e:
            logger.error(f"Error ingesting {data_type}: {e}")
            return False, str(e)

    def ingest(
        self,
        env: str,
        data_types: Optional[List[str]] = None,
        dry_run: bool = False,
    ) -> IngestResult:
        """
        Ingest Spotify data from GCS to BigQuery.

        Args:
            env: Environment (dev/prd)
            data_types: List of data types to ingest (None = auto-detect from GCS)
            dry_run: If True, validate but don't write to BigQuery

        Returns:
            IngestResult with ingestion status
        """
        timestamp = datetime.now()
        bucket_name = f"ela-dp-{env}"

        logger.info(f"Starting Spotify ingestion (env={env}, dry_run={dry_run})")

        try:
            # Scan GCS for files
            files_by_type = self._scan_landing_folder(bucket_name)

            if not files_by_type:
                logger.info("No files found in landing folder")
                return IngestResult(
                    service="spotify",
                    environment=env,
                    data_types=data_types or [],
                    timestamp=timestamp,
                    success=True,
                    files_ingested=0,
                    files_failed=0,
                )

            # Filter by requested data_types if specified
            if data_types:
                files_by_type = {
                    k: v for k, v in files_by_type.items() if k in data_types
                }

            # Run ingestion for each data type
            success_count = 0
            fail_count = 0
            errors = []

            for data_type in sorted(files_by_type.keys()):
                success, error_msg = self._run_single_ingestion(data_type, env, dry_run)
                if success:
                    success_count += 1
                else:
                    fail_count += 1
                    errors.append(f"{data_type}: {error_msg}")

            # Summary
            logger.info(
                f"Ingestion complete: {success_count} succeeded, {fail_count} failed"
            )

            return IngestResult(
                service="spotify",
                environment=env,
                data_types=list(files_by_type.keys()),
                timestamp=timestamp,
                success=fail_count == 0,
                files_ingested=success_count,
                files_failed=fail_count,
                error="; ".join(errors) if errors else None,
            )

        except gcp_exceptions.NotFound:
            error_msg = f"GCS bucket not found: gs://{bucket_name}"
            logger.error(error_msg)
            return IngestResult(
                service="spotify",
                environment=env,
                data_types=data_types or [],
                timestamp=timestamp,
                success=False,
                error=error_msg,
            )

        except gcp_exceptions.Forbidden:
            error_msg = f"Access denied to bucket: gs://{bucket_name}"
            logger.error(error_msg)
            return IngestResult(
                service="spotify",
                environment=env,
                data_types=data_types or [],
                timestamp=timestamp,
                success=False,
                error=error_msg,
            )

        except Exception as e:
            error_msg = f"Fatal error: {e}"
            logger.error(error_msg, exc_info=True)
            return IngestResult(
                service="spotify",
                environment=env,
                data_types=data_types or [],
                timestamp=timestamp,
                success=False,
                error=error_msg,
            )
