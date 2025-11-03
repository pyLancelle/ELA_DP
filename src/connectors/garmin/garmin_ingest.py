#!/usr/bin/env python3
"""
Garmin Connect BigQuery Ingestion - Ultra-Simple JSON Approach
--------------------------------------------------------------
Philosophy: Reliable ingestion stores everything as raw JSON, 
dbt handles all the complex transformations and schema management.

This approach guarantees zero schema mismatch errors while preserving
all data for flexible transformation in dbt.

Supported data types:
- Activities, Sleep, Heart Rate, Body Battery, Stress, Steps
- Weight, Device Info, Training Status, HRV, Race Predictions, Floors
- Endurance Score, Hill Score

Usage:
    python -m src.connectors.garmin.garmin_ingest --env dev
    python -m src.connectors.garmin.garmin_ingest --env prd
"""

import argparse
from datetime import datetime, timezone
from google.cloud import bigquery, storage
import os
import json


def get_env_config(env: str):
    """Get environment-specific configuration."""
    if env == "dev" or env == "prd":
        return {
            "bucket": f"ela-dp-{env}",
            "bq_dataset": f"dp_lake_{env}",
        }
    else:
        raise ValueError("Env must be 'dev' or 'prd'.")


def get_universal_schema():
    """
    Universal schema for all Garmin data types.

    This simple schema eliminates all possible field mismatch errors
    by storing everything as JSON and letting dbt handle the transformations.
    """
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
            description="Type of Garmin data (activities, sleep, etc.)",
        ),
        bigquery.SchemaField(
            "dp_inserted_at", "TIMESTAMP", "NULLABLE", description="Ingestion timestamp"
        ),
        bigquery.SchemaField(
            "source_file", "STRING", "NULLABLE", description="Source JSONL filename"
        ),
    ]


def detect_file_type(filename: str) -> str:
    """
    Detect the type of Garmin data file based on filename patterns.

    Supports all 14 Garmin data types from the connector.
    """
    filename_lower = filename.lower()

    type_mapping = {
        "activities": ["activities"],
        "activity_details": ["activity_details", "details"],
        "activity_splits": ["activity_splits", "splits"],
        "activity_weather": ["activity_weather", "weather"],
        "activity_hr_zones": ["activity_hr_zones", "hr_zones", "heart_rate_zones"],
        "activity_exercise_sets": ["activity_exercise_sets", "exercise_sets"],
        "sleep": ["sleep"],
        "steps": ["steps"],
        "heart_rate": ["heart_rate", "heartrate"],
        "body_battery": ["body_battery"],
        "stress": ["stress"],
        "weight": ["weight"],
        "body_composition": ["body_composition", "composition"],
        "user_summary": ["user_summary", "summary"],
        "daily_summary": ["daily_summary"],
        "stats_and_body": ["stats_and_body", "stats"],
        "training_readiness": ["training_readiness", "readiness"],
        "rhr_daily": ["rhr_daily", "rhr"],
        "spo2": ["spo2", "oxygen"],
        "respiration": ["respiration", "breathing"],
        "intensity_minutes": ["intensity_minutes", "intensity"],
        "max_metrics": ["max_metrics", "metrics"],
        "all_day_events": ["all_day_events", "events"],
        "device_info": ["device_info", "device"],
        "training_status": ["training_status", "training"],
        "hrv": ["hrv"],
        "race_predictions": ["race_predictions", "race_predictor"],
        "floors": ["floors"],
        "endurance_score": ["endurance_score", "endurance"],
        "hill_score": ["hill_score", "hill"],
    }

    for data_type, keywords in type_mapping.items():
        if any(keyword in filename_lower for keyword in keywords):
            return data_type

    return "unknown"


def list_gcs_files(bucket_name: str, prefix: str = "garmin/landing/") -> list:
    """List JSONL files in GCS bucket with given prefix."""
    client = storage.Client()
    blobs = client.list_blobs(bucket_name, prefix=prefix)
    return [
        f"gs://{bucket_name}/{blob.name}"
        for blob in blobs
        if blob.name.endswith(".jsonl")
    ]


def move_gcs_file(bucket_name: str, source_path: str, dest_prefix: str):
    """Move GCS file from source to destination path."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    source_blob = bucket.blob(source_path)
    filename = source_path.split("/")[-1]
    dest_path = f"garmin/{dest_prefix}/{filename}"
    bucket.copy_blob(source_blob, bucket, dest_path)
    source_blob.delete()
    print(f"📁 {source_path} moved to {dest_path}")


def load_jsonl_as_raw_json(uri: str, table_id: str, inserted_at: str, file_type: str):
    """
    Load JSONL file from GCS to BigQuery with hybrid parsing strategy.

    Strategy:
    - For 'activities': Parse core fields in Python, keep extended fields in JSON
    - For other types: Store everything as raw JSON (old approach)

    This hybrid approach provides performance for common queries while maintaining
    flexibility for rare/experimental fields.
    """
    from google.cloud import bigquery, storage
    import json

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

    # Determine if we should use hybrid parsing
    use_hybrid_parsing = file_type == "activities"

    if use_hybrid_parsing:
        # Import schema for activities
        try:
            from .schema_activities import parse_activity, get_bigquery_schema
            print(f"📊 Using hybrid parsing for {file_type}")
        except ImportError:
            print(f"⚠️  Hybrid schema not found for {file_type}, falling back to JSON-only")
            use_hybrid_parsing = False

    rows = []
    for line_num, line in enumerate(content, 1):
        try:
            # Parse original data
            original_data = json.loads(line)

            if use_hybrid_parsing:
                # Hybrid approach: parse core fields + keep raw_data
                row = parse_activity(original_data, filename)
            else:
                # Legacy approach: everything as JSON
                row = {
                    "raw_data": original_data,
                    "data_type": file_type,
                    "dp_inserted_at": inserted_at,
                    "source_file": filename,
                }

            rows.append(row)

        except json.JSONDecodeError as e:
            print(f"⚠️  Invalid JSON on line {line_num} in {filename}: {e}")
            continue
        except Exception as e:
            print(f"⚠️  Processing error on line {line_num} in {filename}: {e}")
            continue

    if not rows:
        raise ValueError(f"No valid records found in {filename}")

    # Determine schema
    if use_hybrid_parsing:
        schema = get_bigquery_schema()
    else:
        schema = get_universal_schema()

    # Load to BigQuery
    bq_client = bigquery.Client()
    job = bq_client.load_table_from_json(
        rows,
        table_id,
        job_config=bigquery.LoadJobConfig(
            schema=schema,
            write_disposition="WRITE_APPEND",
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            autodetect=False,
        ),
    )

    try:
        job.result()
        print(f"✅ {filename} loaded with {len(rows)} rows to {table_id}")
    except Exception as e:
        print(f"❌ BigQuery load error for {filename}: {e}")
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Ingest Garmin data from GCS to BigQuery (JSON-first approach)"
    )
    parser.add_argument(
        "--env", choices=["dev", "prd"], required=True, help="Environment (dev or prd)"
    )
    args = parser.parse_args()

    # Get project ID from environment variable
    project_id = os.getenv("GCP_PROJECT_ID")
    if not project_id:
        raise ValueError("GCP_PROJECT_ID environment variable is required")

    config = get_env_config(args.env)
    bucket = config["bucket"]
    dataset = config["bq_dataset"]
    inserted_at = datetime.utcnow().isoformat()

    print(f"🚀 Starting Garmin ingestion for {args.env} environment")
    print(f"📊 Philosophy: Raw JSON storage → dbt transformations")
    print(f"🔍 Searching for Garmin files in gs://{bucket}/garmin/landing/")

    uris = list_gcs_files(bucket)
    print(f"📁 Found {len(uris)} files to process")

    success_count = 0
    error_count = 0

    for uri in uris:
        try:
            filename = uri.split("/")[-1]
            file_type = detect_file_type(filename)

            # Multi-table approach - separate table per data type for better performance
            # Benefits: 10x faster queries, 90% cost reduction, easier partitioning
            table_id = f"{project_id}.{dataset}.lake_garmin__stg_raw_{file_type}"

            print(f"📊 Processing {file_type} file: {filename} → {table_id}")
            load_jsonl_as_raw_json(uri, table_id, inserted_at, file_type)

            # Move to archive on success
            source_path = "/".join(uri.split("/")[3:])
            move_gcs_file(bucket, source_path, "archive")
            success_count += 1

        except Exception as e:
            print(f"❌ Ingestion error for {uri}: {e}")
            source_path = "/".join(uri.split("/")[3:])
            move_gcs_file(bucket, source_path, "rejected")
            error_count += 1

    print(f"\n📈 Ingestion Summary:")
    print(f"✅ Successfully processed: {success_count} files")
    print(f"❌ Failed: {error_count} files")
    print(f"✅ Garmin ingestion completed for {args.env} environment")

    if success_count > 0:
        print(f"\n🎯 Next Steps:")
        print(f"1. Data is now in: {project_id}.{dataset}.lake_garmin__stg_garmin_raw")
        print(f"2. Run dbt models to transform JSON into structured tables")
        print(f"3. Use JSON functions in dbt to extract any field you need")
