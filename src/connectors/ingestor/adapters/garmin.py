"""
Garmin Ingestor Adapter
-----------------------
Wraps the existing garmin_ingest.py to provide a unified interface.
"""

import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Optional

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent.parent))

from src.connectors.ingestor.base import IngestorAdapter, IngestResult
from src.utils.bq_auto_ingest import BigQueryAutoIngestor
from src.utils.gcs import move_file_in_gcs

logger = logging.getLogger(__name__)

# List of supported Garmin metrics
GARMIN_METRICS = [
    "activities",
    "activity_details",
    "activity_splits",
    "activity_weather",
    "activity_hr_zones",
    "activity_exercise_sets",
    "sleep",
    "steps",
    "heart_rate",
    "body_battery",
    "stress",
    "user_summary",
    "stats_and_body",
    "training_readiness",
    "rhr_daily",
    "spo2",
    "respiration",
    "intensity_minutes",
    "max_metrics",
    "all_day_events",
    "device_info",
    "training_status",
    "hrv",
    "race_predictions",
    "floors",
    "weight",
    "body_composition",
    "endurance_score",
    "hill_score",
]


class GarminIngestorAdapter(IngestorAdapter):
    """Adapter wrapping existing Garmin ingestion logic."""

    @property
    def service_name(self) -> str:
        return "garmin"

    @property
    def available_data_types(self) -> List[str]:
        return GARMIN_METRICS

    def ingest(
        self,
        env: str,
        data_types: Optional[List[str]] = None,
        dry_run: bool = False,
    ) -> IngestResult:
        """
        Ingest Garmin data from GCS to BigQuery.

        Args:
            env: Environment (dev/prd)
            data_types: List of data types to ingest (None = all)
            dry_run: If True, validate but don't write to BigQuery

        Returns:
            IngestResult with ingestion status
        """
        timestamp = datetime.now()
        project_id = os.getenv("GCP_PROJECT_ID", "polar-scene-465223-f7")
        bucket_name = f"ela-dp-{env}"
        dataset_name = f"dp_normalized_{env}"

        logger.info(f"Starting Garmin Ingestion for {env}")
        logger.info(f"   Project: {project_id}")
        logger.info(f"   Bucket: {bucket_name}")
        logger.info(f"   Dataset: {dataset_name}")

        # Initialize ingestor
        ingestor = BigQueryAutoIngestor(project_id=project_id, dry_run=dry_run)

        # Determine metrics to process
        metrics_to_process = data_types if data_types else GARMIN_METRICS

        success_count = 0
        fail_count = 0

        for metric in metrics_to_process:
            logger.info(f"\nProcessing metric: {metric}")

            # Construct source pattern
            prefix = "garmin/landing/"
            table_name = f"normalized_garmin__{metric}"

            try:
                # List files for this metric
                all_files = ingestor.list_gcs_files(f"gs://{bucket_name}/{prefix}")

                # Filter files for this metric
                metric_files = [
                    f
                    for f in all_files
                    if f"garmin_{metric}.jsonl" in f or f"garmin_{metric}_" in f
                ]

                if not metric_files:
                    logger.warning(f"No files found for {metric}")
                    continue

                logger.info(f"   Found {len(metric_files)} files for {metric}")

                # Ingest each file
                for file_uri in metric_files:
                    logger.info(f"   Importing {os.path.basename(file_uri)}...")
                    try:
                        ingestor.ingest_file(
                            source=file_uri,
                            dataset=dataset_name,
                            table=table_name,
                        )
                        success_count += 1

                        # Move ingested file to archive
                        if not dry_run:
                            source_blob_name = file_uri.replace(
                                f"gs://{bucket_name}/", ""
                            )
                            destination_blob_name = source_blob_name.replace(
                                "/landing/", "/archive/"
                            )
                            move_file_in_gcs(
                                bucket_name=bucket_name,
                                source_blob_name=source_blob_name,
                                destination_blob_name=destination_blob_name,
                            )

                    except Exception as e:
                        logger.error(f"Failed to ingest {file_uri}: {e}")
                        fail_count += 1

            except Exception as e:
                logger.error(f"Error processing metric {metric}: {e}")
                fail_count += 1

        logger.info(f"\n{'='*40}")
        logger.info(f"Garmin Ingestion Complete")
        logger.info(f"   Files Processed: {success_count}")
        logger.info(f"   Files Failed:    {fail_count}")
        logger.info(f"{'='*40}")

        return IngestResult(
            service="garmin",
            environment=env,
            data_types=metrics_to_process,
            timestamp=timestamp,
            success=fail_count == 0,
            files_ingested=success_count,
            files_failed=fail_count,
            error=f"{fail_count} files failed" if fail_count > 0 else None,
        )
