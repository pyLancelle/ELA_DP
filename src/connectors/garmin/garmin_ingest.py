#!/usr/bin/env python3
"""
Garmin Data Ingestion Script
Orchestrates the ingestion of Garmin data from GCS to BigQuery.
"""

import argparse
import logging
import os
import sys
from typing import List

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from src.utils.bq_auto_ingest import BigQueryAutoIngestor
from src.utils.gcs import move_file_in_gcs

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# List of supported metrics
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
    "hill_score"
]

def main():
    parser = argparse.ArgumentParser(description='Ingest Garmin data from GCS to BigQuery')
    parser.add_argument('--env', choices=['dev', 'prod'], required=True, help='Environment (dev/prod)')
    parser.add_argument('--dry-run', action='store_true', help='Validate without writing to BigQuery')
    parser.add_argument('--metrics', help='Comma-separated list of metrics to ingest (default: all)')
    
    args = parser.parse_args()
    
    # Configuration based on environment
    project_id = os.getenv('GCP_PROJECT_ID', 'polar-scene-465223-f7')
    bucket_name = f"ela-dp-{args.env}"
    dataset_name = f"dp_normalized_{args.env}"
    
    logging.info(f"🚀 Starting Garmin Ingestion for {args.env}")
    logging.info(f"   Project: {project_id}")
    logging.info(f"   Bucket: {bucket_name}")
    logging.info(f"   Dataset: {dataset_name}")
    
    # Initialize ingestor
    ingestor = BigQueryAutoIngestor(
        project_id=project_id,
        dry_run=args.dry_run
    )
    
    # Determine metrics to process
    metrics_to_process = GARMIN_METRICS
    if args.metrics:
        metrics_to_process = [m.strip() for m in args.metrics.split(',')]
        
    success_count = 0
    fail_count = 0
    
    for metric in metrics_to_process:
        logging.info(f"\n📦 Processing metric: {metric}")
        
        # Construct paths
        # Source: gs://bucket/garmin/landing/*garmin_{metric}.jsonl
        # We use a wildcard to match all files for this metric
        # Note: BigQueryAutoIngestor.ingest_file supports wildcards for GCS
        source_pattern = f"gs://{bucket_name}/garmin/landing/*garmin_{metric}.jsonl"
        
        # Target table
        table_name = f"normalized_garmin__{metric}"
        
        try:
            # Check if any files exist matching the pattern
            # We can use list_gcs_files to check
            # Note: The pattern logic in ingest_file might handle this, but let's be explicit
            # For now, we'll pass the pattern directly to ingest_file which supports GCS wildcards
            # via the underlying BigQuery load job, OR we rely on the ingestor to expand it.
            # Looking at bq_auto_ingest.py, ingest_file takes a single source.
            # If we want to ingest multiple files, we should list them first.
            
            # Let's list files first to be safe and have better logging
            # The pattern needs to be handled carefully. list_gcs_files expects a prefix.
            prefix = f"garmin/landing/"
            all_files = ingestor.list_gcs_files(f"gs://{bucket_name}/{prefix}")
            
            # Filter files for this metric
            # Pattern: ...garmin_{metric}.jsonl or ...garmin_{metric}_*.jsonl
            metric_files = [
                f for f in all_files 
                if f"garmin_{metric}.jsonl" in f or f"garmin_{metric}_" in f
            ]
            
            if not metric_files:
                logging.warning(f"⚠️  No files found for {metric} in {source_pattern}")
                continue
                
            logging.info(f"   Found {len(metric_files)} files for {metric}")
            
            # Ingest each file
            # In a real batch scenario, we might want to load them all at once using wildcard,
            # but bq_auto_ingest.py is designed for file-by-file or directory processing.
            # Let's process them one by one for now to ensure schema detection works per file
            # (or we could merge them, but file-by-file is safer for now)
            
            for file_uri in metric_files:
                logging.info(f"   Importing {os.path.basename(file_uri)}...")
                try:
                    ingestor.ingest_file(
                        source=file_uri,
                        dataset=dataset_name,
                        table=table_name,
                    )
                    success_count += 1

                    # Move ingested file to archive
                    source_blob_name = file_uri.replace(f"gs://{bucket_name}/", "")
                    destination_blob_name = source_blob_name.replace(
                        "/landing/", "/archive/"
                    )
                    move_file_in_gcs(
                        bucket_name=bucket_name,
                        source_blob_name=source_blob_name,
                        destination_blob_name=destination_blob_name,
                    )

                except Exception as e:
                    logging.error(f"❌ Failed to ingest {file_uri}: {e}")
                    fail_count += 1
                    
        except Exception as e:
            logging.error(f"❌ Error processing metric {metric}: {e}")
            fail_count += 1

    logging.info(f"\n{'='*40}")
    logging.info(f"✅ Ingestion Complete")
    logging.info(f"   Files Processed: {success_count}")
    logging.info(f"   Files Failed:    {fail_count}")
    logging.info(f"{'='*40}")

    if fail_count > 0:
        sys.exit(1)

if __name__ == "__main__":
    main()
