#!/usr/bin/env python3
"""
Spotify Album Enrichment Connector
-----------------------------------
Enriches album data by fetching detailed metadata from Spotify API.
Features:
 - Batch fetching (20 albums per API call)
 - Backfill mode: enrich all existing albums
 - Incremental mode: enrich only new albums
 - BigQuery integration to identify albums needing enrichment
 - Automatic token refresh
 - GCS upload support
"""
import os
import sys
import argparse
import logging
from datetime import datetime
from zoneinfo import ZoneInfo
from pathlib import Path
from typing import Optional, List, Dict, Any
from dataclasses import dataclass
from enum import Enum

import spotipy
from spotipy.oauth2 import SpotifyOAuth
from google.cloud import bigquery
from google.cloud import storage

sys.path.append(str(Path(__file__).parent.parent))
from utils import to_jsonl

# Constants
BATCH_SIZE = 20  # Spotify API limit for album batch requests
DEFAULT_TIMEZONE = "Europe/Paris"
REQUIRED_ENV_VARS = [
    "SPOTIFY_CLIENT_ID",
    "SPOTIFY_CLIENT_SECRET",
    "SPOTIFY_REDIRECT_URI",
    "SPOTIFY_REFRESH_TOKEN",
]


class EnrichmentMode(Enum):
    """Enrichment execution modes."""

    BACKFILL = "backfill"  # Enrich all albums
    INCREMENTAL = "incremental"  # Enrich only new albums


@dataclass
class SpotifyConfig:
    """Configuration for Spotify connector."""

    client_id: str
    client_secret: str
    redirect_uri: str
    refresh_token: str
    cache_path: Path
    timezone: str = DEFAULT_TIMEZONE


@dataclass
class BigQueryConfig:
    """Configuration for BigQuery."""

    project_id: str
    dataset: str  # Lake dataset (e.g., dp_lake_dev)

    @property
    def hub_dataset(self) -> str:
        """Derive hub dataset from lake dataset (dp_lake_dev -> dp_hub_dev)."""
        return self.dataset.replace("dp_lake_", "dp_hub_")


class AlbumEnrichmentError(Exception):
    """Custom exception for album enrichment errors."""

    pass


class AlbumEnrichmentConnector:
    """Album enrichment connector with BigQuery integration."""

    # Scope required for album data access
    REQUIRED_SCOPE = ""  # Album data is public, no special scope needed

    def __init__(
        self, spotify_config: SpotifyConfig, bigquery_config: BigQueryConfig
    ):
        """Initialize the album enrichment connector."""
        self.spotify_config = spotify_config
        self.bigquery_config = bigquery_config
        self._spotify_client: Optional[spotipy.Spotify] = None
        self._bq_client: Optional[bigquery.Client] = None

    def authenticate_spotify(self) -> None:
        """Authenticate with Spotify API."""
        logging.info("Authenticating with Spotify...")

        try:
            auth_manager = SpotifyOAuth(
                client_id=self.spotify_config.client_id,
                client_secret=self.spotify_config.client_secret,
                redirect_uri=self.spotify_config.redirect_uri,
                scope=self.REQUIRED_SCOPE,
                cache_path=str(self.spotify_config.cache_path),
            )

            # Clear cache to force refresh
            if self.spotify_config.cache_path.exists():
                self.spotify_config.cache_path.unlink()
                logging.debug("Cleared existing token cache")

            # Get token using refresh token
            try:
                token_info = auth_manager.refresh_access_token(
                    self.spotify_config.refresh_token
                )
                access_token = token_info.get("access_token")
            except Exception as refresh_error:
                raise AlbumEnrichmentError(
                    f"Refresh token authentication failed: {refresh_error}"
                )

            if not access_token:
                raise AlbumEnrichmentError("Failed to get access token")

            self._spotify_client = spotipy.Spotify(auth=access_token)

            # Test authentication
            self._spotify_client.current_user()
            logging.info("✅ Spotify authenticated successfully")

        except Exception as e:
            raise AlbumEnrichmentError(
                f"Spotify authentication failed: {e}"
            ) from e

    def initialize_bigquery(self) -> None:
        """Initialize BigQuery client."""
        try:
            self._bq_client = bigquery.Client(project=self.bigquery_config.project_id)
            logging.info("✅ BigQuery client initialized")
        except Exception as e:
            raise AlbumEnrichmentError(
                f"BigQuery initialization failed: {e}"
            ) from e

    @property
    def spotify(self) -> spotipy.Spotify:
        """Get the authenticated Spotify client."""
        if self._spotify_client is None:
            raise AlbumEnrichmentError(
                "Not authenticated. Call authenticate_spotify() first."
            )
        return self._spotify_client

    @property
    def bigquery(self) -> bigquery.Client:
        """Get the BigQuery client."""
        if self._bq_client is None:
            raise AlbumEnrichmentError(
                "BigQuery not initialized. Call initialize_bigquery() first."
            )
        return self._bq_client

    def get_albums_to_enrich(self, mode: EnrichmentMode) -> List[Dict[str, str]]:
        """
        Query BigQuery to get list of albums needing enrichment.

        Returns:
            List of dicts with albumId and albumName
        """
        logging.info(f"Querying BigQuery for albums to enrich (mode: {mode.value})...")

        # Check if enrichment table exists
        enrichment_table_exists = self._check_table_exists(
            self.bigquery_config.dataset,
            "lake_spotify__normalized_album_enrichment"
        )

        if mode == EnrichmentMode.BACKFILL:
            if not enrichment_table_exists:
                # First run: get all albums from played tracks
                logging.info("Enrichment table doesn't exist yet - enriching all albums from played tracks")
                query = f"""
                WITH track_albums AS (
                    SELECT DISTINCT
                        rp.albumId,
                        rp.albumName,
                        MIN(rp.playedAt) AS first_played_at
                    FROM `{self.bigquery_config.project_id}.{self.bigquery_config.dataset}.lake_spotify__svc_recently_played` AS rp
                    WHERE rp.albumId IS NOT NULL
                    GROUP BY rp.albumId, rp.albumName
                )
                SELECT
                    albumId,
                    albumName
                FROM track_albums
                ORDER BY first_played_at DESC
                """
            else:
                # Get all albums from played tracks that don't have enrichment data yet
                query = f"""
                WITH track_albums AS (
                    SELECT DISTINCT
                        rp.albumId,
                        rp.albumName,
                        MIN(rp.playedAt) AS first_played_at
                    FROM `{self.bigquery_config.project_id}.{self.bigquery_config.dataset}.lake_spotify__svc_recently_played` AS rp
                    WHERE rp.albumId IS NOT NULL
                    GROUP BY rp.albumId, rp.albumName
                )
                SELECT
                    ta.albumId,
                    ta.albumName
                FROM track_albums AS ta
                LEFT JOIN `{self.bigquery_config.project_id}.{self.bigquery_config.dataset}.lake_spotify__normalized_album_enrichment` AS enrichment
                    ON ta.albumId = enrichment.albumId
                WHERE enrichment.albumId IS NULL
                ORDER BY ta.first_played_at DESC
                """
        else:  # INCREMENTAL
            if not enrichment_table_exists:
                # First run: get albums from last 7 days of played tracks
                logging.info("Enrichment table doesn't exist yet - enriching recent albums from played tracks")
                query = f"""
                WITH track_albums AS (
                    SELECT DISTINCT
                        rp.albumId,
                        rp.albumName,
                        MIN(rp.playedAt) AS first_played_at
                    FROM `{self.bigquery_config.project_id}.{self.bigquery_config.dataset}.lake_spotify__svc_recently_played` AS rp
                    WHERE rp.albumId IS NOT NULL
                        AND rp.playedAt >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
                    GROUP BY rp.albumId, rp.albumName
                )
                SELECT
                    albumId,
                    albumName
                FROM track_albums
                ORDER BY first_played_at DESC
                """
            else:
                # Get only albums from tracks played in the last 7 days that need enrichment
                query = f"""
                WITH track_albums AS (
                    SELECT DISTINCT
                        rp.albumId,
                        rp.albumName,
                        MIN(rp.playedAt) AS first_played_at
                    FROM `{self.bigquery_config.project_id}.{self.bigquery_config.dataset}.lake_spotify__svc_recently_played` AS rp
                    WHERE rp.albumId IS NOT NULL
                        AND rp.playedAt >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
                    GROUP BY rp.albumId, rp.albumName
                )
                SELECT
                    ta.albumId,
                    ta.albumName
                FROM track_albums AS ta
                LEFT JOIN `{self.bigquery_config.project_id}.{self.bigquery_config.dataset}.lake_spotify__normalized_album_enrichment` AS enrichment
                    ON ta.albumId = enrichment.albumId
                WHERE enrichment.albumId IS NULL
                ORDER BY ta.first_played_at DESC
                """

        try:
            query_job = self.bigquery.query(query)
            results = query_job.result()

            albums = [
                {"albumId": row.albumId, "albumName": row.albumName}
                for row in results
            ]

            logging.info(f"Found {len(albums)} albums to enrich")
            return albums

        except Exception as e:
            raise AlbumEnrichmentError(
                f"Failed to query albums from BigQuery: {e}"
            ) from e

    def _check_table_exists(self, dataset: str, table_name: str) -> bool:
        """
        Check if a BigQuery table exists.

        Args:
            dataset: Dataset name (e.g., "dp_lake_dev")
            table_name: Table name (e.g., "lake_spotify__normalized_album_enrichment")

        Returns:
            True if table exists, False otherwise
        """
        table_id = f"{self.bigquery_config.project_id}.{dataset}.{table_name}"
        try:
            self.bigquery.get_table(table_id)
            return True
        except Exception:
            return False

    def fetch_album_details(self, album_ids: List[str]) -> List[Dict[str, Any]]:
        """
        Fetch detailed album metadata from Spotify API.

        Args:
            album_ids: List of Spotify album IDs (max 20)

        Returns:
            List of album detail dicts with full metadata
        """
        if len(album_ids) > BATCH_SIZE:
            raise AlbumEnrichmentError(
                f"Cannot fetch more than {BATCH_SIZE} albums at once"
            )

        try:
            results = self.spotify.albums(album_ids)
            albums = results.get("albums", [])

            # Add enrichment timestamp
            enrichment_timestamp = datetime.now(
                ZoneInfo(self.spotify_config.timezone)
            ).isoformat()

            for album in albums:
                if album:  # Can be None if album not found
                    album["enriched_at"] = enrichment_timestamp

            # Filter out None values (albums not found)
            albums = [a for a in albums if a is not None]

            return albums

        except Exception as e:
            logging.error(f"Error fetching album details: {e}")
            return []

    def enrich_albums_batch(
        self, albums: List[Dict[str, str]]
    ) -> List[Dict[str, Any]]:
        """
        Enrich a batch of albums by fetching their details.

        Args:
            albums: List of dicts with albumId and albumName

        Returns:
            List of enriched album data
        """
        all_enriched = []
        total = len(albums)

        # Process in batches of 20 (Spotify API limit)
        for i in range(0, total, BATCH_SIZE):
            batch = albums[i : i + BATCH_SIZE]
            album_ids = [a["albumId"] for a in batch]

            logging.info(
                f"Enriching batch {i // BATCH_SIZE + 1}/{(total + BATCH_SIZE - 1) // BATCH_SIZE} "
                f"({len(album_ids)} albums)..."
            )

            enriched = self.fetch_album_details(album_ids)
            all_enriched.extend(enriched)

            # Log any albums that couldn't be enriched
            enriched_ids = {a["id"] for a in enriched}
            missing_ids = set(album_ids) - enriched_ids
            if missing_ids:
                logging.warning(
                    f"Could not enrich {len(missing_ids)} albums: {missing_ids}"
                )

        logging.info(
            f"Successfully enriched {len(all_enriched)}/{total} albums"
        )
        return all_enriched

    def run_enrichment(self, mode: EnrichmentMode) -> List[Dict[str, Any]]:
        """
        Run the complete enrichment process.

        Args:
            mode: Enrichment mode (backfill or incremental)

        Returns:
            List of enriched album data
        """
        logging.info(f"Starting album enrichment in {mode.value} mode...")

        # Get albums to enrich
        albums_to_enrich = self.get_albums_to_enrich(mode)

        if not albums_to_enrich:
            logging.info("No albums to enrich. Exiting.")
            return []

        # Enrich albums
        enriched_albums = self.enrich_albums_batch(albums_to_enrich)

        return enriched_albums


def setup_logging(level: str = "INFO") -> None:
    """Configure logging format and level."""
    fmt = "%(asctime)s %(levelname)s: %(message)s"
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format=fmt,
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def load_env(dotenv_path: Path) -> None:
    """Load environment variables from .env file."""
    try:
        from dotenv import load_dotenv

        if dotenv_path.exists():
            load_dotenv(dotenv_path)
            logging.debug(f"Loaded .env from {dotenv_path}")
        else:
            logging.warning(f".env file not found at {dotenv_path}")
    except ImportError:
        logging.warning("python-dotenv not installed, skipping .env file loading")


def validate_env_vars() -> Dict[str, str]:
    """Validate and return required environment variables."""
    missing_vars = []
    env_vars = {}

    for var in REQUIRED_ENV_VARS:
        value = os.getenv(var)
        if not value:
            missing_vars.append(var)
        else:
            env_vars[var] = value

    if missing_vars:
        raise AlbumEnrichmentError(
            f"Missing required environment variables: {', '.join(missing_vars)}"
        )

    return env_vars


def generate_output_filename(
    output_dir: Path, timezone: str = DEFAULT_TIMEZONE
) -> Path:
    """Generate timestamped output filename."""
    tz = ZoneInfo(timezone)
    timestamp = datetime.now(tz=tz).strftime("%Y_%m_%d_%H_%M")
    return output_dir / f"{timestamp}_album_enrichment.jsonl"


def upload_to_gcs(local_file: Path, bucket_name: str, gcs_path: str) -> None:
    """
    Upload file to Google Cloud Storage.

    Args:
        local_file: Path to local file
        bucket_name: GCS bucket name (e.g., "ela-dp-dev")
        gcs_path: Destination path in GCS (e.g., "spotify/landing/file.jsonl")
    """
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(gcs_path)

        logging.info(f"Uploading {local_file.name} to gs://{bucket_name}/{gcs_path}...")
        blob.upload_from_filename(str(local_file))
        logging.info(f"✅ Successfully uploaded to GCS")

    except Exception as e:
        raise AlbumEnrichmentError(f"Failed to upload to GCS: {e}") from e


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Enrich Spotify album data from BigQuery and Spotify API.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "-m",
        "--mode",
        choices=[mode.value for mode in EnrichmentMode],
        default=EnrichmentMode.INCREMENTAL.value,
        help="Enrichment mode: backfill (all albums) or incremental (new albums only)",
    )
    parser.add_argument(
        "-o",
        "--output-dir",
        type=Path,
        default=Path("."),
        help="Directory to save JSONL output",
    )
    parser.add_argument(
        "-e",
        "--env",
        type=Path,
        default=Path(__file__).parent.parent.parent.parent / ".env",
        help="Path to the .env file",
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Logging level",
    )
    parser.add_argument(
        "--timezone", default=DEFAULT_TIMEZONE, help="Timezone for timestamps"
    )
    parser.add_argument(
        "--project-id",
        help="GCP Project ID (overrides env var GCP_PROJECT_ID)",
    )
    parser.add_argument(
        "--dataset",
        default="dp_lake_dev",
        help="BigQuery LAKE dataset name (hub dataset will be auto-derived, e.g., dp_lake_dev -> dp_hub_dev)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Query albums but don't call Spotify API",
    )
    parser.add_argument(
        "--gcs-bucket",
        help="GCS bucket name for upload (e.g., 'ela-dp-dev'). If provided, file will be uploaded to spotify/landing/",
    )

    return parser.parse_args()


def main() -> None:
    """Main entry point."""
    try:
        args = parse_args()
        setup_logging(args.log_level)

        # Load environment variables
        load_env(args.env)
        env_vars = validate_env_vars()

        # Get project ID from args or env
        project_id = args.project_id or os.getenv("GCP_PROJECT_ID")
        if not project_id:
            raise AlbumEnrichmentError(
                "GCP_PROJECT_ID must be set via --project-id or environment variable"
            )

        # Create configs
        cache_path = Path(__file__).parent.parent / ".spotify-cache"
        spotify_config = SpotifyConfig(
            client_id=env_vars["SPOTIFY_CLIENT_ID"],
            client_secret=env_vars["SPOTIFY_CLIENT_SECRET"],
            redirect_uri=env_vars["SPOTIFY_REDIRECT_URI"],
            refresh_token=env_vars["SPOTIFY_REFRESH_TOKEN"],
            cache_path=cache_path,
            timezone=args.timezone,
        )

        bigquery_config = BigQueryConfig(project_id=project_id, dataset=args.dataset)

        # Initialize connector
        connector = AlbumEnrichmentConnector(spotify_config, bigquery_config)

        # Authenticate
        connector.authenticate_spotify()
        connector.initialize_bigquery()

        # Run enrichment
        mode = EnrichmentMode(args.mode)

        if args.dry_run:
            logging.info("DRY RUN mode - only querying albums, not enriching")
            albums = connector.get_albums_to_enrich(mode)
            logging.info(f"Would enrich {len(albums)} albums:")
            for album in albums[:10]:  # Show first 10
                logging.info(f"  - {album['albumName']} ({album['albumId']})")
            if len(albums) > 10:
                logging.info(f"  ... and {len(albums) - 10} more")
            return

        enriched_data = connector.run_enrichment(mode)

        if not enriched_data:
            logging.info("No data to save. Exiting.")
            return

        # Write output
        output_file = generate_output_filename(args.output_dir, args.timezone)
        output_file.parent.mkdir(parents=True, exist_ok=True)
        to_jsonl(enriched_data, jsonl_output_path=str(output_file))
        logging.info(f"📁 Enriched data saved to: {output_file} ({len(enriched_data)} albums)")

        # Upload to GCS if bucket specified
        if args.gcs_bucket:
            gcs_path = f"spotify/landing/{output_file.name}"
            upload_to_gcs(output_file, args.gcs_bucket, gcs_path)

        logging.info("✅ Enrichment completed successfully")

    except AlbumEnrichmentError as e:
        logging.error(f"Album enrichment error: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        logging.info("Script interrupted by user")
        sys.exit(130)
    except Exception as e:
        logging.error(f"Unexpected error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
