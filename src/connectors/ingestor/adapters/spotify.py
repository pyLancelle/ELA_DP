"""
Spotify Ingestor Adapter
------------------------
Delegates to spotify_ingest_auto for unified ingestion.
"""

import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Optional

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent.parent))

from src.connectors.ingestor.base import IngestorAdapter, IngestResult

logger = logging.getLogger(__name__)

# Spotify data types
SPOTIFY_DATA_TYPES = [
    "recently_played",
    "saved_tracks",
    "saved_albums",
    "playlists",
    "top_artists",
    "top_tracks",
    "followed_artists",
]


class SpotifyIngestorAdapter(IngestorAdapter):
    """Adapter that delegates to spotify_ingest_auto for ingestion."""

    @property
    def service_name(self) -> str:
        return "spotify"

    @property
    def available_data_types(self) -> List[str]:
        return SPOTIFY_DATA_TYPES

    def ingest(
        self,
        env: str,
        data_types: Optional[List[str]] = None,
        dry_run: bool = False,
    ) -> IngestResult:
        """
        Ingest Spotify data from GCS to BigQuery using spotify_ingest_auto.

        Args:
            env: Environment (dev/prd)
            data_types: List of data types to ingest (None = all)
            dry_run: If True, validate but don't write to BigQuery

        Returns:
            IngestResult with ingestion status
        """
        import subprocess

        timestamp = datetime.now()

        # Build command to run spotify_ingest_auto
        cmd = [
            sys.executable,
            "-m",
            "src.connectors.spotify.spotify_ingest_auto",
            "--env",
            env,
        ]

        if dry_run:
            cmd.append("--dry-run")

        logger.info(f"Running Spotify ingestion via spotify_ingest_auto")
        logger.info(f"Command: {' '.join(cmd)}")

        try:
            result = subprocess.run(
                cmd, capture_output=True, text=True, timeout=1800  # 30 minutes timeout
            )

            # Forward stdout/stderr to logger
            if result.stdout:
                for line in result.stdout.splitlines():
                    logger.info(line)
            if result.stderr:
                for line in result.stderr.splitlines():
                    logger.error(line)

            if result.returncode == 0:
                logger.info("Spotify ingestion completed successfully")
                return IngestResult(
                    service="spotify",
                    environment=env,
                    data_types=data_types or SPOTIFY_DATA_TYPES,
                    timestamp=timestamp,
                    success=True,
                    files_ingested=0,  # spotify_ingest_auto doesn't report counts
                    files_failed=0,
                )
            else:
                error_msg = (
                    f"spotify_ingest_auto failed with exit code {result.returncode}"
                )
                logger.error(error_msg)
                return IngestResult(
                    service="spotify",
                    environment=env,
                    data_types=data_types or SPOTIFY_DATA_TYPES,
                    timestamp=timestamp,
                    success=False,
                    files_ingested=0,
                    files_failed=0,
                    error=error_msg,
                )

        except subprocess.TimeoutExpired:
            error_msg = "spotify_ingest_auto timed out after 30 minutes"
            logger.error(error_msg)
            return IngestResult(
                service="spotify",
                environment=env,
                data_types=data_types or SPOTIFY_DATA_TYPES,
                timestamp=timestamp,
                success=False,
                files_ingested=0,
                files_failed=0,
                error=error_msg,
            )
        except Exception as e:
            error_msg = f"Error running spotify_ingest_auto: {e}"
            logger.error(error_msg, exc_info=True)
            return IngestResult(
                service="spotify",
                environment=env,
                data_types=data_types or SPOTIFY_DATA_TYPES,
                timestamp=timestamp,
                success=False,
                files_ingested=0,
                files_failed=0,
                error=error_msg,
            )
