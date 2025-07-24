#!/usr/bin/env python3
"""
Spotify BigQuery Ingestion - Universal JSON Approach
---------------------------------------------------
Philosophy: Reliable ingestion stores everything as raw JSON, 
dbt handles all the complex transformations and schema management.

This approach guarantees zero schema mismatch errors while preserving
all data for flexible transformation in dbt.

Supported data types:
- Recently played, Saved tracks, Saved albums, Playlists
- Top artists, Top tracks, Followed artists

Usage:
    python -m src.connectors.spotify.spotify_ingest --env dev
    python -m src.connectors.spotify.spotify_ingest --env prd
"""

import argparse
from datetime import datetime, timezone
from google.cloud import bigquery, storage
import os
import json

dev = False
if dev:
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "gcs_key.json"


def get_universal_schema():
    """
    Universal schema for all Spotify data types.

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


def detect_file_type(filename: str) -> str:
    """Detect the type of Spotify data file."""
    if "saved_tracks" in filename:
        return "saved_tracks"
    elif "saved_albums" in filename:
        return "saved_albums"
    elif "playlists" in filename:
        return "playlists"
    elif "top_artists" in filename:
        return "top_artists"
    elif "top_tracks" in filename:
        return "top_tracks"
    elif "followed_artists" in filename:
        return "followed_artists"
    else:
        return "recently_played"


def get_env_config(env: str):
    """Get environment-specific configuration."""
    if env == "dev" or env == "prd":
        return {
            "bucket": f"ela-dp-{env}",
            "bq_dataset": f"dp_lake_{env}",
        }
    else:
        raise ValueError("Env must be 'dev' or 'prd'.")


def list_gcs_files(bucket_name: str, prefix: str = "spotify/landing/") -> list:
    """List all JSONL files in the GCS landing directory."""
    client = storage.Client()
    blobs = client.list_blobs(bucket_name, prefix=prefix)
    return [
        f"gs://{bucket_name}/{blob.name}"
        for blob in blobs
        if blob.name.endswith(".jsonl")
    ]


def move_gcs_file(bucket_name: str, source_path: str, dest_prefix: str):
    """Move a file from one GCS location to another."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    source_blob = bucket.blob(source_path)
    filename = source_path.split("/")[-1]
    dest_path = f"spotify/{dest_prefix}/{filename}"
    bucket.copy_blob(source_blob, bucket, dest_path)
    source_blob.delete()
    print(f"📁 {source_path} moved to {dest_path}")


def load_jsonl_with_universal_schema(
    uri: str, table_id: str, inserted_at: str, data_type: str
):
    """
    Load JSONL file using universal JSON schema.

    This approach stores the complete original record as JSON,
    eliminating all schema mismatch errors during ingestion.
    """
    from google.cloud import bigquery, storage
    import json

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
                "raw_data": original_data,  # Store complete original record as JSON
                "data_type": data_type,  # Add data type for filtering in dbt
                "dp_inserted_at": inserted_at,
                "source_file": filename,
            }

            rows.append(universal_record)

        except json.JSONDecodeError:
            print(f"❌ Invalid JSON line ignored in {filename}")
            continue
        except Exception as e:
            print(f"⚠️ Error processing line in {filename}: {e}")
            continue

    if not rows:
        raise ValueError(f"Empty or invalid file: {filename}")

    # Use universal schema for all data types
    schema = get_universal_schema()

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
    print(f"✅ {filename} loaded with {len(rows)} records ({data_type})")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Spotify data ingestion with universal JSON schema"
    )
    parser.add_argument("--env", choices=["dev", "prd"], required=True)
    args = parser.parse_args()

    # Get project ID from environment variable
    project_id = os.getenv("GCP_PROJECT_ID")
    if not project_id:
        raise ValueError("GCP_PROJECT_ID environment variable is required")

    config = get_env_config(args.env)
    bucket = config["bucket"]
    dataset = config["bq_dataset"]
    inserted_at = datetime.utcnow().isoformat()

    # Single universal table for all Spotify data types (consistent with Garmin naming)
    table_id = f"{project_id}.{dataset}.lake_spotify__stg_spotify_raw"

    uris = list_gcs_files(bucket)
    print(f"🔍 Found {len(uris)} files to process")

    if not uris:
        print("ℹ️ No files found in spotify/landing/")
        exit(0)

    for uri in uris:
        try:
            filename = uri.split("/")[-1]
            data_type = detect_file_type(filename)

            print(f"📊 Processing {data_type} file: {filename}")
            load_jsonl_with_universal_schema(uri, table_id, inserted_at, data_type)

            # Move to archive after successful processing
            source_path = "/".join(uri.split("/")[3:])
            move_gcs_file(bucket, source_path, "archive")

        except Exception as e:
            print(f"❌ Error processing {uri}: {e}")
            # Move to rejected folder on error
            source_path = "/".join(uri.split("/")[3:])
            move_gcs_file(bucket, source_path, "rejected")

    print("✅ Spotify ingestion completed")
