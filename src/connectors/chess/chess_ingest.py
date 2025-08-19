#!/usr/bin/env python3
"""
Chess.com BigQuery Ingestion - Ultra-Simple JSON Approach
---------------------------------------------------------
Philosophy: Reliable ingestion stores everything as raw JSON, 
dbt handles all the complex transformations and schema management.

This approach guarantees zero schema mismatch errors while preserving
all data for flexible transformation in dbt.

Supported data types:
- Player Profile, Player Stats, Games, Clubs, Tournaments

Usage:
    python -m src.connectors.chess.chess_ingest --env dev
    python -m src.connectors.chess.chess_ingest --env prd
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
    Universal schema for all Chess.com data types.

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
            description="Type of Chess.com data (player_profile, player_stats, games, etc.)",
        ),
        bigquery.SchemaField(
            "username",
            "STRING",
            "NULLABLE",
            description="Chess.com username this data belongs to",
        ),
        bigquery.SchemaField(
            "dp_inserted_at", "TIMESTAMP", "NULLABLE", description="Ingestion timestamp"
        ),
        bigquery.SchemaField(
            "source_file", "STRING", "NULLABLE", description="Source JSONL filename"
        ),
    ]


def detect_file_type_and_username(filename: str) -> tuple:
    """
    Detect the type of Chess.com data file based on filename patterns.
    Also extracts username from filename.

    Expected format: YYYY_MM_DD_HH_MM_chess_{username}_{data_type}.jsonl

    Returns: (data_type, username)
    """
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


def list_gcs_files(bucket_name: str, prefix: str = "chess/landing/") -> list:
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
    dest_path = f"chess/{dest_prefix}/{filename}"
    bucket.copy_blob(source_blob, bucket, dest_path)
    source_blob.delete()
    print(f"📁 {source_path} moved to {dest_path}")


def load_jsonl_as_raw_json(
    uri: str, table_id: str, inserted_at: str, file_type: str, username: str
):
    """
    Load JSONL file from GCS to BigQuery with zero transformation.

    Philosophy: Store everything as raw JSON, let dbt handle the rest.
    This approach guarantees no schema mismatch errors.
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

    rows = []
    for line_num, line in enumerate(content, 1):
        try:
            # Parse original data (validation only)
            original_data = json.loads(line)

            # Create row with minimal structure - everything preserved as JSON
            row = {
                "raw_data": original_data,  # Complete original record
                "data_type": file_type,  # For easy filtering in dbt
                "username": username,  # Chess.com username
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

    # Load to BigQuery with universal schema
    bq_client = bigquery.Client()
    job = bq_client.load_table_from_json(
        rows,
        table_id,
        job_config=bigquery.LoadJobConfig(
            schema=get_universal_schema(),
            write_disposition="WRITE_APPEND",
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            # Enable auto-detection as fallback (though our schema should handle everything)
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
        description="Ingest Chess.com data from GCS to BigQuery (JSON-first approach)"
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

    print(f"🚀 Starting Chess.com ingestion for {args.env} environment")
    print(f"📊 Philosophy: Raw JSON storage → dbt transformations")
    print(f"🔍 Searching for Chess.com files in gs://{bucket}/chess/landing/")

    uris = list_gcs_files(bucket)
    print(f"📁 Found {len(uris)} files to process")

    success_count = 0
    error_count = 0

    for uri in uris:
        try:
            filename = uri.split("/")[-1]
            file_type, username = detect_file_type_and_username(filename)

            # Single table approach - all Chess.com data types in one raw table
            table_id = f"{project_id}.{dataset}.lake_chess__stg_chess_raw"

            print(f"📊 Processing {file_type} file for {username}: {filename}")
            load_jsonl_as_raw_json(uri, table_id, inserted_at, file_type, username)

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
    print(f"✅ Chess.com ingestion completed for {args.env} environment")

    if success_count > 0:
        print(f"\n🎯 Next Steps:")
        print(f"1. Data is now in: {project_id}.{dataset}.lake_chess__stg_chess_raw")
        print(f"2. Run dbt models to transform JSON into structured tables")
        print(f"3. Use JSON functions in dbt to extract any field you need")
