"""
Chess Ingestor Adapter
----------------------
Wraps the existing chess_ingest.py to provide a unified interface.
"""

import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Optional
import json

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent.parent))

from src.connectors.ingestor.base import IngestorAdapter, IngestResult
from google.cloud import bigquery, storage

logger = logging.getLogger(__name__)

# Chess data types
CHESS_DATA_TYPES = [
    "player_profile",
    "player_stats",
    "games",
    "clubs",
    "tournaments",
]


class ChessIngestorAdapter(IngestorAdapter):
    """Adapter wrapping existing Chess ingestion logic."""

    @property
    def service_name(self) -> str:
        return "chess"

    @property
    def available_data_types(self) -> List[str]:
        return CHESS_DATA_TYPES

    def _get_universal_schema(self):
        """Universal schema for all Chess.com data types."""
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
                description="Type of Chess.com data (player_profile, player_stats, games, etc.)",
            ),
            bigquery.SchemaField(
                "username",
                "STRING",
                "NULLABLE",
                description="Chess.com username this data belongs to",
            ),
            bigquery.SchemaField(
                "dp_inserted_at",
                "TIMESTAMP",
                "NULLABLE",
                description="Ingestion timestamp",
            ),
            bigquery.SchemaField(
                "source_file",
                "STRING",
                "NULLABLE",
                description="Source JSONL filename",
            ),
        ]

    def _detect_file_type_and_username(self, filename: str) -> tuple:
        """Detect the type of Chess.com data file and extract username."""
        filename_lower = filename.lower()

        # Extract username and data type from filename pattern
        try:
            # Split filename and remove extension
            name_parts = filename.replace(".jsonl", "").split("_")

            # Find 'chess' identifier
            chess_index = -1
            for i, part in enumerate(name_parts):
                if part == "chess":
                    chess_index = i
                    break

            if chess_index >= 0 and len(name_parts) > chess_index + 2:
                username = name_parts[chess_index + 1]
                data_type = name_parts[chess_index + 2]
                return data_type, username

        except Exception:
            pass

        # Fallback detection based on keywords
        type_mapping = {
            "player_profile": ["profile"],
            "player_stats": ["stats"],
            "games": ["games"],
            "clubs": ["clubs"],
            "tournaments": ["tournaments"],
        }

        for data_type, keywords in type_mapping.items():
            if any(keyword in filename_lower for keyword in keywords):
                # Try to extract username if possible
                parts = filename_lower.split("_")
                username = "unknown"
                for i, part in enumerate(parts):
                    if part == "chess" and i + 1 < len(parts):
                        username = parts[i + 1]
                        break
                return data_type, username

        return "unknown", "unknown"

    def _list_gcs_files(self, bucket_name: str, prefix: str = "chess/landing/") -> list:
        """List JSONL files in GCS bucket with given prefix."""
        client = storage.Client()
        blobs = client.list_blobs(bucket_name, prefix=prefix)
        return [
            f"gs://{bucket_name}/{blob.name}"
            for blob in blobs
            if blob.name.endswith(".jsonl")
        ]

    def _move_gcs_file(self, bucket_name: str, source_path: str, dest_prefix: str):
        """Move GCS file from source to destination path."""
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        source_blob = bucket.blob(source_path)
        filename = source_path.split("/")[-1]
        dest_path = f"chess/{dest_prefix}/{filename}"
        bucket.copy_blob(source_blob, bucket, dest_path)
        source_blob.delete()
        logger.info(f"Moved {source_path} to {dest_path}")

    def _load_jsonl_as_raw_json(
        self, uri: str, table_id: str, inserted_at: str, file_type: str, username: str
    ):
        """Load JSONL file from GCS to BigQuery with zero transformation."""
        # Parse GCS URI
        parts = uri.split("/")
        bucket_name = parts[2]
        blob_path = "/".join(parts[3:])
        filename = parts[-1]

        # Download from GCS
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_path)
        content = blob.download_as_text().splitlines()

        rows = []
        for line_num, line in enumerate(content, 1):
            try:
                # Parse original data (validation only)
                original_data = json.loads(line)

                # Create row with minimal structure
                row = {
                    "raw_data": original_data,
                    "data_type": file_type,
                    "username": username,
                    "dp_inserted_at": inserted_at,
                    "source_file": filename,
                }

                rows.append(row)

            except json.JSONDecodeError as e:
                logger.warning(f"Invalid JSON on line {line_num} in {filename}: {e}")
                continue
            except Exception as e:
                logger.warning(
                    f"Processing error on line {line_num} in {filename}: {e}"
                )
                continue

        if not rows:
            raise ValueError(f"No valid records found in {filename}")

        # Load to BigQuery with universal schema
        bq_client = bigquery.Client()
        job = bq_client.load_table_from_json(
            rows,
            table_id,
            job_config=bigquery.LoadJobConfig(
                schema=self._get_universal_schema(),
                write_disposition="WRITE_APPEND",
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                autodetect=False,
            ),
        )

        job.result()
        logger.info(f"Loaded {filename} with {len(rows)} rows to {table_id}")

    def ingest(
        self,
        env: str,
        data_types: Optional[List[str]] = None,
        dry_run: bool = False,
    ) -> IngestResult:
        """
        Ingest Chess data from GCS to BigQuery.

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
                    service="chess",
                    environment=env,
                    data_types=data_types or CHESS_DATA_TYPES,
                    timestamp=timestamp,
                    success=False,
                    error="GCP_PROJECT_ID environment variable is required or GCP credentials must be configured for auto-detection",
                )

        bucket = f"ela-dp-{env}"
        dataset = f"dp_lake_{env}"
        inserted_at = datetime.utcnow().isoformat()

        logger.info(f"Starting Chess.com ingestion for {env} environment")
        logger.info(f"Searching for Chess.com files in gs://{bucket}/chess/landing/")

        uris = self._list_gcs_files(bucket)
        logger.info(f"Found {len(uris)} files to process")

        if not uris:
            logger.info("No files found in chess/landing/")
            return IngestResult(
                service="chess",
                environment=env,
                data_types=data_types or CHESS_DATA_TYPES,
                timestamp=timestamp,
                success=True,
                files_ingested=0,
                files_failed=0,
            )

        success_count = 0
        error_count = 0

        for uri in uris:
            try:
                filename = uri.split("/")[-1]
                file_type, username = self._detect_file_type_and_username(filename)

                # Filter by data_types if specified
                if data_types and file_type not in data_types:
                    logger.debug(f"Skipping {file_type} file: {filename}")
                    continue

                # Single table approach - all Chess.com data types in one raw table
                table_id = f"{project_id}.{dataset}.lake_chess__stg_chess_raw"

                logger.info(f"Processing {file_type} file for {username}: {filename}")

                if not dry_run:
                    self._load_jsonl_as_raw_json(
                        uri, table_id, inserted_at, file_type, username
                    )

                    # Move to archive on success
                    source_path = "/".join(uri.split("/")[3:])
                    self._move_gcs_file(bucket, source_path, "archive")

                success_count += 1

            except Exception as e:
                logger.error(f"Ingestion error for {uri}: {e}")
                error_count += 1

                # Move to rejected on error
                if not dry_run:
                    try:
                        source_path = "/".join(uri.split("/")[3:])
                        self._move_gcs_file(bucket, source_path, "rejected")
                    except Exception as move_error:
                        logger.error(f"Failed to move rejected file: {move_error}")

        logger.info("\nIngestion Summary:")
        logger.info(f"   Successfully processed: {success_count} files")
        logger.info(f"   Failed: {error_count} files")
        logger.info(f"Chess.com ingestion completed for {env} environment")

        return IngestResult(
            service="chess",
            environment=env,
            data_types=data_types or CHESS_DATA_TYPES,
            timestamp=timestamp,
            success=error_count == 0,
            files_ingested=success_count,
            files_failed=error_count,
            error=f"{error_count} files failed" if error_count > 0 else None,
        )
