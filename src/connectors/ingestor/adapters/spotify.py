"""
Spotify Ingestor Adapter
------------------------
Wraps the existing spotify_ingest.py to provide a unified interface.
"""

import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional
import json

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent.parent))

from src.connectors.ingestor.base import IngestorAdapter, IngestResult
from google.cloud import bigquery, storage

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
    """Adapter wrapping existing Spotify ingestion logic."""

    @property
    def service_name(self) -> str:
        return "spotify"

    @property
    def available_data_types(self) -> List[str]:
        return SPOTIFY_DATA_TYPES

    def _get_universal_schema(self):
        """Universal schema for all Spotify data types."""
        return [
            bigquery.SchemaField(
                "raw_data",
                "JSON",
                "NULLABLE",
                description="Complete original record as JSON",
            ),
            bigquery.SchemaField(
                "data_type",
                "STRING",
                "NULLABLE",
                description="Type of Spotify data (recently_played, saved_tracks, etc.)",
            ),
            bigquery.SchemaField(
                "dp_inserted_at",
                "TIMESTAMP",
                "NULLABLE",
                description="Timestamp when the record was inserted into the data platform",
            ),
            bigquery.SchemaField(
                "source_file",
                "STRING",
                "NULLABLE",
                description="Original filename that contained this record",
            ),
        ]

    def _detect_file_type(self, filename: str) -> str:
        """Detect the type of Spotify data file."""
        for data_type in SPOTIFY_DATA_TYPES:
            if data_type in filename:
                return data_type
        return "recently_played"  # Default

    def _list_gcs_files(
        self, bucket_name: str, prefix: str = "spotify/landing/"
    ) -> list:
        """List all JSONL files in the GCS landing directory."""
        client = storage.Client()
        blobs = client.list_blobs(bucket_name, prefix=prefix)
        return [
            f"gs://{bucket_name}/{blob.name}"
            for blob in blobs
            if blob.name.endswith(".jsonl")
        ]

    def _move_gcs_file(self, bucket_name: str, source_path: str, dest_prefix: str):
        """Move a file from one GCS location to another."""
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        source_blob = bucket.blob(source_path)
        filename = source_path.split("/")[-1]
        dest_path = f"spotify/{dest_prefix}/{filename}"
        bucket.copy_blob(source_blob, bucket, dest_path)
        source_blob.delete()
        logger.info(f"Moved {source_path} to {dest_path}")

    def _load_jsonl_with_universal_schema(
        self, uri: str, table_id: str, inserted_at: str, data_type: str
    ):
        """Load JSONL file using universal JSON schema."""
        parts = uri.split("/")
        bucket_name = parts[2]
        blob_path = "/".join(parts[3:])
        filename = parts[-1]

        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_path)
        content = blob.download_as_text().splitlines()

        rows = []
        for line in content:
            try:
                original_data = json.loads(line)

                # Create the universal record structure
                universal_record = {
                    "raw_data": original_data,
                    "data_type": data_type,
                    "dp_inserted_at": inserted_at,
                    "source_file": filename,
                }

                rows.append(universal_record)

            except json.JSONDecodeError:
                logger.warning(f"Invalid JSON line ignored in {filename}")
                continue
            except Exception as e:
                logger.warning(f"Error processing line in {filename}: {e}")
                continue

        if not rows:
            raise ValueError(f"Empty or invalid file: {filename}")

        # Use universal schema for all data types
        schema = self._get_universal_schema()

        bq_client = bigquery.Client()
        job = bq_client.load_table_from_json(
            rows,
            table_id,
            job_config=bigquery.LoadJobConfig(
                schema=schema,
                write_disposition="WRITE_APPEND",
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            ),
        )
        job.result()
        logger.info(f"Loaded {filename} with {len(rows)} records ({data_type})")

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
            data_types: List of data types to ingest (None = all)
            dry_run: If True, validate but don't write to BigQuery

        Returns:
            IngestResult with ingestion status
        """
        timestamp = datetime.now()

        # Get project ID from environment variable or auto-detect
        project_id = os.getenv("GCP_PROJECT_ID")
        if not project_id:
            try:
                # Try to auto-detect from GCP client
                storage_client = storage.Client()
                project_id = storage_client.project
                logger.info(f"Auto-detected GCP project ID: {project_id}")
            except Exception as e:
                logger.debug(f"Could not auto-detect project ID: {e}")
                return IngestResult(
                    service="spotify",
                    environment=env,
                    data_types=data_types or SPOTIFY_DATA_TYPES,
                    timestamp=timestamp,
                    success=False,
                    error="GCP_PROJECT_ID environment variable is required or GCP credentials must be configured for auto-detection",
                )

        bucket = f"ela-dp-{env}"
        dataset = f"dp_lake_{env}"
        inserted_at = datetime.utcnow().isoformat()

        # Single universal table for all Spotify data types
        table_id = f"{project_id}.{dataset}.lake_spotify__stg_spotify_raw"

        uris = self._list_gcs_files(bucket)
        logger.info(f"Found {len(uris)} files to process")

        if not uris:
            logger.info("No files found in spotify/landing/")
            return IngestResult(
                service="spotify",
                environment=env,
                data_types=data_types or SPOTIFY_DATA_TYPES,
                timestamp=timestamp,
                success=True,
                files_ingested=0,
                files_failed=0,
            )

        success_count = 0
        fail_count = 0

        for uri in uris:
            try:
                filename = uri.split("/")[-1]
                detected_type = self._detect_file_type(filename)

                # Filter by data_types if specified
                if data_types and detected_type not in data_types:
                    logger.debug(f"Skipping {detected_type} file: {filename}")
                    continue

                logger.info(f"Processing {detected_type} file: {filename}")

                if not dry_run:
                    self._load_jsonl_with_universal_schema(
                        uri, table_id, inserted_at, detected_type
                    )

                    # Move to archive after successful processing
                    source_path = "/".join(uri.split("/")[3:])
                    self._move_gcs_file(bucket, source_path, "archive")

                success_count += 1

            except Exception as e:
                logger.error(f"Error processing {uri}: {e}")
                fail_count += 1

                # Move to rejected folder on error
                if not dry_run:
                    try:
                        source_path = "/".join(uri.split("/")[3:])
                        self._move_gcs_file(bucket, source_path, "rejected")
                    except Exception as move_error:
                        logger.error(f"Failed to move rejected file: {move_error}")

        logger.info("Spotify ingestion completed")
        logger.info(f"   Files Processed: {success_count}")
        logger.info(f"   Files Failed:    {fail_count}")

        return IngestResult(
            service="spotify",
            environment=env,
            data_types=data_types or SPOTIFY_DATA_TYPES,
            timestamp=timestamp,
            success=fail_count == 0,
            files_ingested=success_count,
            files_failed=fail_count,
            error=f"{fail_count} files failed" if fail_count > 0 else None,
        )
