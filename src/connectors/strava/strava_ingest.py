#!/usr/bin/env python3
"""
Strava BigQuery Ingestion
-------------------------
Ingests Strava fitness data from GCS to BigQuery.
Follows the same pattern as spotify_ingest.py for consistency.

Supported data types:
- Activities (workouts, GPS tracks, performance metrics)
- Athlete (profile information)
- Kudos, Comments (social interaction data)
- Laps (detailed activity segments)
- Streams (GPS and sensor time-series data)
- Gears (equipment information)
- Clubs (community data)

Usage:
    python -m src.connectors.strava.strava_ingest --env dev --project YOUR_PROJECT_ID
    python -m src.connectors.strava.strava_ingest --env prd --project YOUR_PROJECT_ID
"""

import argparse
from datetime import datetime, timezone
from google.cloud import bigquery, storage
import os
import json


# Environment configuration
def get_env_config(env: str):
    """Get environment-specific configuration."""
    if env == "dev" or env == "prd":
        return {
            "bucket": f"ela-dp-{env}",
            "bq_dataset": f"dp_lake_{env}",
        }
    else:
        raise ValueError("Env must be 'dev' or 'prd'.")


# Strava Activities Schema
strava_activities_schema = [
    bigquery.SchemaField("resource_state", "INTEGER", "NULLABLE"),
    bigquery.SchemaField(
        "athlete",
        "RECORD",
        "NULLABLE",
        None,
        None,
        (
            bigquery.SchemaField("id", "INTEGER", "NULLABLE"),
            bigquery.SchemaField("resource_state", "INTEGER", "NULLABLE"),
        ),
    ),
    bigquery.SchemaField("name", "STRING", "NULLABLE"),
    bigquery.SchemaField("distance", "FLOAT", "NULLABLE"),
    bigquery.SchemaField("moving_time", "INTEGER", "NULLABLE"),
    bigquery.SchemaField("elapsed_time", "INTEGER", "NULLABLE"),
    bigquery.SchemaField("total_elevation_gain", "FLOAT", "NULLABLE"),
    bigquery.SchemaField("type", "STRING", "NULLABLE"),
    bigquery.SchemaField("sport_type", "STRING", "NULLABLE"),
    bigquery.SchemaField("workout_type", "INTEGER", "NULLABLE"),
    bigquery.SchemaField("id", "INTEGER", "NULLABLE"),
    bigquery.SchemaField("start_date", "STRING", "NULLABLE"),
    bigquery.SchemaField("start_date_local", "STRING", "NULLABLE"),
    bigquery.SchemaField("timezone", "STRING", "NULLABLE"),
    bigquery.SchemaField("utc_offset", "FLOAT", "NULLABLE"),
    bigquery.SchemaField("location_city", "STRING", "NULLABLE"),
    bigquery.SchemaField("location_state", "STRING", "NULLABLE"),
    bigquery.SchemaField("location_country", "STRING", "NULLABLE"),
    bigquery.SchemaField("achievement_count", "INTEGER", "NULLABLE"),
    bigquery.SchemaField("kudos_count", "INTEGER", "NULLABLE"),
    bigquery.SchemaField("comment_count", "INTEGER", "NULLABLE"),
    bigquery.SchemaField("athlete_count", "INTEGER", "NULLABLE"),
    bigquery.SchemaField("photo_count", "INTEGER", "NULLABLE"),
    bigquery.SchemaField("trainer", "BOOLEAN", "NULLABLE"),
    bigquery.SchemaField("commute", "BOOLEAN", "NULLABLE"),
    bigquery.SchemaField("manual", "BOOLEAN", "NULLABLE"),
    bigquery.SchemaField("private", "BOOLEAN", "NULLABLE"),
    bigquery.SchemaField("visibility", "STRING", "NULLABLE"),
    bigquery.SchemaField("flagged", "BOOLEAN", "NULLABLE"),
    bigquery.SchemaField("gear_id", "STRING", "NULLABLE"),
    bigquery.SchemaField("start_latlng", "FLOAT", "REPEATED"),
    bigquery.SchemaField("end_latlng", "FLOAT", "REPEATED"),
    bigquery.SchemaField("average_speed", "FLOAT", "NULLABLE"),
    bigquery.SchemaField("max_speed", "FLOAT", "NULLABLE"),
    bigquery.SchemaField("average_cadence", "FLOAT", "NULLABLE"),
    bigquery.SchemaField("average_watts", "FLOAT", "NULLABLE"),
    bigquery.SchemaField("weighted_average_watts", "INTEGER", "NULLABLE"),
    bigquery.SchemaField("kilojoules", "FLOAT", "NULLABLE"),
    bigquery.SchemaField("device_watts", "BOOLEAN", "NULLABLE"),
    bigquery.SchemaField("has_heartrate", "BOOLEAN", "NULLABLE"),
    bigquery.SchemaField("average_heartrate", "FLOAT", "NULLABLE"),
    bigquery.SchemaField("max_heartrate", "FLOAT", "NULLABLE"),
    bigquery.SchemaField("heartrate_opt_out", "BOOLEAN", "NULLABLE"),
    bigquery.SchemaField("display_hide_heartrate_option", "BOOLEAN", "NULLABLE"),
    bigquery.SchemaField("elev_high", "FLOAT", "NULLABLE"),
    bigquery.SchemaField("elev_low", "FLOAT", "NULLABLE"),
    bigquery.SchemaField("upload_id", "INTEGER", "NULLABLE"),
    bigquery.SchemaField("upload_id_str", "STRING", "NULLABLE"),
    bigquery.SchemaField("external_id", "STRING", "NULLABLE"),
    bigquery.SchemaField("from_accepted_tag", "BOOLEAN", "NULLABLE"),
    bigquery.SchemaField("pr_count", "INTEGER", "NULLABLE"),
    bigquery.SchemaField("total_photo_count", "INTEGER", "NULLABLE"),
    bigquery.SchemaField("has_kudoed", "BOOLEAN", "NULLABLE"),
    bigquery.SchemaField("suffer_score", "INTEGER", "NULLABLE"),
    bigquery.SchemaField(
        "map",
        "RECORD",
        "NULLABLE",
        None,
        None,
        (
            bigquery.SchemaField("id", "STRING", "NULLABLE"),
            bigquery.SchemaField("polyline", "STRING", "NULLABLE"),
            bigquery.SchemaField("resource_state", "INTEGER", "NULLABLE"),
            bigquery.SchemaField("summary_polyline", "STRING", "NULLABLE"),
        ),
    ),
    # Splits data
    bigquery.SchemaField(
        "splits_metric",
        "RECORD",
        "REPEATED",
        None,
        None,
        (
            bigquery.SchemaField("distance", "FLOAT", "NULLABLE"),
            bigquery.SchemaField("elapsed_time", "INTEGER", "NULLABLE"),
            bigquery.SchemaField("elevation_difference", "FLOAT", "NULLABLE"),
            bigquery.SchemaField("moving_time", "INTEGER", "NULLABLE"),
            bigquery.SchemaField("split", "INTEGER", "NULLABLE"),
            bigquery.SchemaField("average_speed", "FLOAT", "NULLABLE"),
            bigquery.SchemaField("pace_zone", "INTEGER", "NULLABLE"),
        ),
    ),
    bigquery.SchemaField(
        "splits_standard",
        "RECORD",
        "REPEATED",
        None,
        None,
        (
            bigquery.SchemaField("distance", "FLOAT", "NULLABLE"),
            bigquery.SchemaField("elapsed_time", "INTEGER", "NULLABLE"),
            bigquery.SchemaField("elevation_difference", "FLOAT", "NULLABLE"),
            bigquery.SchemaField("moving_time", "INTEGER", "NULLABLE"),
            bigquery.SchemaField("split", "INTEGER", "NULLABLE"),
            bigquery.SchemaField("average_speed", "FLOAT", "NULLABLE"),
            bigquery.SchemaField("pace_zone", "INTEGER", "NULLABLE"),
        ),
    ),
    # Laps data
    bigquery.SchemaField(
        "laps",
        "RECORD",
        "REPEATED",
        None,
        None,
        (
            bigquery.SchemaField("id", "INTEGER", "NULLABLE"),
            bigquery.SchemaField("resource_state", "INTEGER", "NULLABLE"),
            bigquery.SchemaField("name", "STRING", "NULLABLE"),
            bigquery.SchemaField(
                "activity",
                "RECORD",
                "NULLABLE",
                None,
                None,
                (
                    bigquery.SchemaField("id", "INTEGER", "NULLABLE"),
                    bigquery.SchemaField("resource_state", "INTEGER", "NULLABLE"),
                ),
            ),
            bigquery.SchemaField(
                "athlete",
                "RECORD",
                "NULLABLE",
                None,
                None,
                (
                    bigquery.SchemaField("id", "INTEGER", "NULLABLE"),
                    bigquery.SchemaField("resource_state", "INTEGER", "NULLABLE"),
                ),
            ),
            bigquery.SchemaField("elapsed_time", "INTEGER", "NULLABLE"),
            bigquery.SchemaField("moving_time", "INTEGER", "NULLABLE"),
            bigquery.SchemaField("start_date", "STRING", "NULLABLE"),
            bigquery.SchemaField("start_date_local", "STRING", "NULLABLE"),
            bigquery.SchemaField("distance", "FLOAT", "NULLABLE"),
            bigquery.SchemaField("start_index", "INTEGER", "NULLABLE"),
            bigquery.SchemaField("end_index", "INTEGER", "NULLABLE"),
            bigquery.SchemaField("total_elevation_gain", "FLOAT", "NULLABLE"),
            bigquery.SchemaField("average_speed", "FLOAT", "NULLABLE"),
            bigquery.SchemaField("max_speed", "FLOAT", "NULLABLE"),
            bigquery.SchemaField("average_cadence", "FLOAT", "NULLABLE"),
            bigquery.SchemaField("device_watts", "BOOLEAN", "NULLABLE"),
            bigquery.SchemaField("average_watts", "FLOAT", "NULLABLE"),
            bigquery.SchemaField("lap_index", "INTEGER", "NULLABLE"),
            bigquery.SchemaField("split", "INTEGER", "NULLABLE"),
        ),
    ),
    # Gear information
    bigquery.SchemaField(
        "gear",
        "RECORD",
        "NULLABLE",
        None,
        None,
        (
            bigquery.SchemaField("id", "STRING", "NULLABLE"),
            bigquery.SchemaField("primary", "BOOLEAN", "NULLABLE"),
            bigquery.SchemaField("name", "STRING", "NULLABLE"),
            bigquery.SchemaField("resource_state", "INTEGER", "NULLABLE"),
            bigquery.SchemaField("distance", "FLOAT", "NULLABLE"),
        ),
    ),
    # Metadata fields
    bigquery.SchemaField("dp_inserted_at", "TIMESTAMP", "NULLABLE"),
    bigquery.SchemaField("source_file", "STRING", "NULLABLE"),
]

# Strava Athlete Schema
strava_athlete_schema = [
    bigquery.SchemaField("id", "INTEGER", "NULLABLE"),
    bigquery.SchemaField("username", "STRING", "NULLABLE"),
    bigquery.SchemaField("resource_state", "INTEGER", "NULLABLE"),
    bigquery.SchemaField("firstname", "STRING", "NULLABLE"),
    bigquery.SchemaField("lastname", "STRING", "NULLABLE"),
    bigquery.SchemaField("bio", "STRING", "NULLABLE"),
    bigquery.SchemaField("city", "STRING", "NULLABLE"),
    bigquery.SchemaField("state", "STRING", "NULLABLE"),
    bigquery.SchemaField("country", "STRING", "NULLABLE"),
    bigquery.SchemaField("sex", "STRING", "NULLABLE"),
    bigquery.SchemaField("premium", "BOOLEAN", "NULLABLE"),
    bigquery.SchemaField("summit", "BOOLEAN", "NULLABLE"),
    bigquery.SchemaField("created_at", "STRING", "NULLABLE"),
    bigquery.SchemaField("updated_at", "STRING", "NULLABLE"),
    bigquery.SchemaField("badge_type_id", "INTEGER", "NULLABLE"),
    bigquery.SchemaField("weight", "FLOAT", "NULLABLE"),
    bigquery.SchemaField("profile_medium", "STRING", "NULLABLE"),
    bigquery.SchemaField("profile", "STRING", "NULLABLE"),
    bigquery.SchemaField("friend", "STRING", "NULLABLE"),
    bigquery.SchemaField("follower", "STRING", "NULLABLE"),
    bigquery.SchemaField("follower_count", "INTEGER", "NULLABLE"),
    bigquery.SchemaField("friend_count", "INTEGER", "NULLABLE"),
    bigquery.SchemaField("mutual_friend_count", "INTEGER", "NULLABLE"),
    bigquery.SchemaField("athlete_type", "INTEGER", "NULLABLE"),
    bigquery.SchemaField("date_preference", "STRING", "NULLABLE"),
    bigquery.SchemaField("measurement_preference", "STRING", "NULLABLE"),
    bigquery.SchemaField(
        "clubs",
        "RECORD",
        "REPEATED",
        None,
        None,
        (
            bigquery.SchemaField("id", "INTEGER", "NULLABLE"),
            bigquery.SchemaField("resource_state", "INTEGER", "NULLABLE"),
            bigquery.SchemaField("name", "STRING", "NULLABLE"),
            bigquery.SchemaField("profile_medium", "STRING", "NULLABLE"),
            bigquery.SchemaField("profile", "STRING", "NULLABLE"),
            bigquery.SchemaField("cover_photo", "STRING", "NULLABLE"),
            bigquery.SchemaField("cover_photo_small", "STRING", "NULLABLE"),
            bigquery.SchemaField("sport_type", "STRING", "NULLABLE"),
            bigquery.SchemaField("city", "STRING", "NULLABLE"),
            bigquery.SchemaField("state", "STRING", "NULLABLE"),
            bigquery.SchemaField("country", "STRING", "NULLABLE"),
            bigquery.SchemaField("private", "BOOLEAN", "NULLABLE"),
            bigquery.SchemaField("member_count", "INTEGER", "NULLABLE"),
            bigquery.SchemaField("featured", "BOOLEAN", "NULLABLE"),
            bigquery.SchemaField("verified", "BOOLEAN", "NULLABLE"),
            bigquery.SchemaField("url", "STRING", "NULLABLE"),
        ),
    ),
    bigquery.SchemaField(
        "bikes",
        "RECORD",
        "REPEATED",
        None,
        None,
        (
            bigquery.SchemaField("id", "STRING", "NULLABLE"),
            bigquery.SchemaField("primary", "BOOLEAN", "NULLABLE"),
            bigquery.SchemaField("name", "STRING", "NULLABLE"),
            bigquery.SchemaField("resource_state", "INTEGER", "NULLABLE"),
            bigquery.SchemaField("distance", "FLOAT", "NULLABLE"),
        ),
    ),
    bigquery.SchemaField(
        "shoes",
        "RECORD",
        "REPEATED",
        None,
        None,
        (
            bigquery.SchemaField("id", "STRING", "NULLABLE"),
            bigquery.SchemaField("primary", "BOOLEAN", "NULLABLE"),
            bigquery.SchemaField("name", "STRING", "NULLABLE"),
            bigquery.SchemaField("resource_state", "INTEGER", "NULLABLE"),
            bigquery.SchemaField("distance", "FLOAT", "NULLABLE"),
        ),
    ),
    # Metadata fields
    bigquery.SchemaField("dp_inserted_at", "TIMESTAMP", "NULLABLE"),
    bigquery.SchemaField("source_file", "STRING", "NULLABLE"),
]

# Strava Streams Schema (GPS and sensor data)
strava_streams_schema = [
    bigquery.SchemaField("activity_id", "INTEGER", "NULLABLE"),
    bigquery.SchemaField(
        "type", "STRING", "NULLABLE"
    ),  # latlng, distance, time, altitude, velocity_smooth, etc.
    bigquery.SchemaField("data", "STRING", "REPEATED"),  # Time series data points
    bigquery.SchemaField("series_type", "STRING", "NULLABLE"),
    bigquery.SchemaField("original_size", "INTEGER", "NULLABLE"),
    bigquery.SchemaField("resolution", "STRING", "NULLABLE"),
    # Metadata fields
    bigquery.SchemaField("dp_inserted_at", "TIMESTAMP", "NULLABLE"),
    bigquery.SchemaField("source_file", "STRING", "NULLABLE"),
]

# Simple schemas for social/reference data
strava_kudos_schema = [
    bigquery.SchemaField("activity_id", "INTEGER", "NULLABLE"),
    bigquery.SchemaField("firstname", "STRING", "NULLABLE"),
    bigquery.SchemaField("lastname", "STRING", "NULLABLE"),
    bigquery.SchemaField("athlete_id", "INTEGER", "NULLABLE"),
    bigquery.SchemaField("profile", "STRING", "NULLABLE"),
    bigquery.SchemaField("profile_medium", "STRING", "NULLABLE"),
    bigquery.SchemaField("created_at", "STRING", "NULLABLE"),
    # Metadata fields
    bigquery.SchemaField("dp_inserted_at", "TIMESTAMP", "NULLABLE"),
    bigquery.SchemaField("source_file", "STRING", "NULLABLE"),
]

strava_comments_schema = [
    bigquery.SchemaField("activity_id", "INTEGER", "NULLABLE"),
    bigquery.SchemaField("id", "INTEGER", "NULLABLE"),
    bigquery.SchemaField("text", "STRING", "NULLABLE"),
    bigquery.SchemaField("created_at", "STRING", "NULLABLE"),
    bigquery.SchemaField(
        "athlete",
        "RECORD",
        "NULLABLE",
        None,
        None,
        (
            bigquery.SchemaField("id", "INTEGER", "NULLABLE"),
            bigquery.SchemaField("firstname", "STRING", "NULLABLE"),
            bigquery.SchemaField("lastname", "STRING", "NULLABLE"),
            bigquery.SchemaField("profile", "STRING", "NULLABLE"),
            bigquery.SchemaField("profile_medium", "STRING", "NULLABLE"),
        ),
    ),
    # Metadata fields
    bigquery.SchemaField("dp_inserted_at", "TIMESTAMP", "NULLABLE"),
    bigquery.SchemaField("source_file", "STRING", "NULLABLE"),
]

strava_laps_schema = [
    bigquery.SchemaField("activity_id", "INTEGER", "NULLABLE"),
    bigquery.SchemaField("id", "INTEGER", "NULLABLE"),
    bigquery.SchemaField("resource_state", "INTEGER", "NULLABLE"),
    bigquery.SchemaField("name", "STRING", "NULLABLE"),
    bigquery.SchemaField("elapsed_time", "INTEGER", "NULLABLE"),
    bigquery.SchemaField("moving_time", "INTEGER", "NULLABLE"),
    bigquery.SchemaField("start_date", "STRING", "NULLABLE"),
    bigquery.SchemaField("start_date_local", "STRING", "NULLABLE"),
    bigquery.SchemaField("distance", "FLOAT", "NULLABLE"),
    bigquery.SchemaField("start_index", "INTEGER", "NULLABLE"),
    bigquery.SchemaField("end_index", "INTEGER", "NULLABLE"),
    bigquery.SchemaField("total_elevation_gain", "FLOAT", "NULLABLE"),
    bigquery.SchemaField("average_speed", "FLOAT", "NULLABLE"),
    bigquery.SchemaField("max_speed", "FLOAT", "NULLABLE"),
    bigquery.SchemaField("average_cadence", "FLOAT", "NULLABLE"),
    bigquery.SchemaField("device_watts", "BOOLEAN", "NULLABLE"),
    bigquery.SchemaField("average_watts", "FLOAT", "NULLABLE"),
    bigquery.SchemaField("lap_index", "INTEGER", "NULLABLE"),
    bigquery.SchemaField("split", "INTEGER", "NULLABLE"),
    # Metadata fields
    bigquery.SchemaField("dp_inserted_at", "TIMESTAMP", "NULLABLE"),
    bigquery.SchemaField("source_file", "STRING", "NULLABLE"),
]

strava_gears_schema = [
    bigquery.SchemaField("id", "STRING", "NULLABLE"),
    bigquery.SchemaField("primary", "BOOLEAN", "NULLABLE"),
    bigquery.SchemaField("name", "STRING", "NULLABLE"),
    bigquery.SchemaField("resource_state", "INTEGER", "NULLABLE"),
    bigquery.SchemaField("distance", "FLOAT", "NULLABLE"),
    bigquery.SchemaField("brand_name", "STRING", "NULLABLE"),
    bigquery.SchemaField("model_name", "STRING", "NULLABLE"),
    bigquery.SchemaField("frame_type", "STRING", "NULLABLE"),
    bigquery.SchemaField("description", "STRING", "NULLABLE"),
    # Metadata fields
    bigquery.SchemaField("dp_inserted_at", "TIMESTAMP", "NULLABLE"),
    bigquery.SchemaField("source_file", "STRING", "NULLABLE"),
]

strava_clubs_schema = [
    bigquery.SchemaField("id", "INTEGER", "NULLABLE"),
    bigquery.SchemaField("resource_state", "INTEGER", "NULLABLE"),
    bigquery.SchemaField("name", "STRING", "NULLABLE"),
    bigquery.SchemaField("profile_medium", "STRING", "NULLABLE"),
    bigquery.SchemaField("profile", "STRING", "NULLABLE"),
    bigquery.SchemaField("cover_photo", "STRING", "NULLABLE"),
    bigquery.SchemaField("cover_photo_small", "STRING", "NULLABLE"),
    bigquery.SchemaField("sport_type", "STRING", "NULLABLE"),
    bigquery.SchemaField("city", "STRING", "NULLABLE"),
    bigquery.SchemaField("state", "STRING", "NULLABLE"),
    bigquery.SchemaField("country", "STRING", "NULLABLE"),
    bigquery.SchemaField("private", "BOOLEAN", "NULLABLE"),
    bigquery.SchemaField("member_count", "INTEGER", "NULLABLE"),
    bigquery.SchemaField("featured", "BOOLEAN", "NULLABLE"),
    bigquery.SchemaField("verified", "BOOLEAN", "NULLABLE"),
    bigquery.SchemaField("url", "STRING", "NULLABLE"),
    # Metadata fields
    bigquery.SchemaField("dp_inserted_at", "TIMESTAMP", "NULLABLE"),
    bigquery.SchemaField("source_file", "STRING", "NULLABLE"),
]


def detect_file_type(filename: str) -> str:
    """Detect the type of Strava data file based on filename patterns."""
    filename_lower = filename.lower()
    if "activities" in filename_lower:
        return "activities"
    elif "athlete" in filename_lower:
        return "athlete"
    elif "kudos" in filename_lower:
        return "kudos"
    elif "comments" in filename_lower:
        return "comments"
    elif "laps" in filename_lower:
        return "laps"
    elif "streams" in filename_lower:
        return "streams"
    elif "gears" in filename_lower:
        return "gears"
    elif "clubs" in filename_lower:
        return "clubs"
    else:
        return "activities"  # Default fallback


def get_schema_for_type(file_type: str):
    """Get the appropriate schema for a Strava file type."""
    schemas = {
        "activities": strava_activities_schema,
        "athlete": strava_athlete_schema,
        "streams": strava_streams_schema,
        "kudos": strava_kudos_schema,
        "comments": strava_comments_schema,
        "laps": strava_laps_schema,
        "gears": strava_gears_schema,
        "clubs": strava_clubs_schema,
    }
    return schemas.get(file_type, strava_activities_schema)  # Default fallback


def list_gcs_files(bucket_name: str, prefix: str = "strava/landing/") -> list:
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
    dest_path = f"strava/{dest_prefix}/{filename}"
    bucket.copy_blob(source_blob, bucket, dest_path)
    source_blob.delete()
    print(f"📁 {source_path} moved to {dest_path}")


def load_jsonl_with_metadata(uri: str, table_id: str, inserted_at: str, file_type: str):
    """Load JSONL file from GCS to BigQuery with Strava-specific validation."""
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
    for line in content:
        try:
            data = json.loads(line)

            # Add metadata
            data["dp_inserted_at"] = inserted_at
            data["source_file"] = filename

            # Strava-specific data validation
            if file_type == "activities":
                # Ensure required fields exist
                if "id" not in data or data["id"] is None:
                    continue  # Skip invalid activities

                # Handle GPS coordinates properly
                if "start_latlng" in data and data["start_latlng"]:
                    if not isinstance(data["start_latlng"], list):
                        data["start_latlng"] = []

                if "end_latlng" in data and data["end_latlng"]:
                    if not isinstance(data["end_latlng"], list):
                        data["end_latlng"] = []

                # Ensure splits are lists
                if "splits_metric" not in data:
                    data["splits_metric"] = []
                elif not isinstance(data["splits_metric"], list):
                    data["splits_metric"] = []

                if "splits_standard" not in data:
                    data["splits_standard"] = []
                elif not isinstance(data["splits_standard"], list):
                    data["splits_standard"] = []

                # Ensure laps are lists
                if "laps" not in data:
                    data["laps"] = []
                elif not isinstance(data["laps"], list):
                    data["laps"] = []

                # Handle athlete record properly
                if "athlete" not in data or data["athlete"] is None:
                    data["athlete"] = {}

                # Handle gear record properly
                if "gear" in data and data["gear"] is not None:
                    if not isinstance(data["gear"], dict):
                        data["gear"] = {}

            elif file_type == "athlete":
                # Validate athlete data
                if "id" not in data:
                    continue

                # Ensure clubs, bikes, shoes are lists
                for list_field in ["clubs", "bikes", "shoes"]:
                    if list_field not in data:
                        data[list_field] = []
                    elif not isinstance(data[list_field], list):
                        data[list_field] = []

            elif file_type == "streams":
                # Ensure streams have activity reference
                if "activity_id" not in data:
                    # Try to extract from filename or skip
                    continue

                # Ensure data is a list
                if "data" not in data:
                    data["data"] = []
                elif not isinstance(data["data"], list):
                    # Convert data to string list
                    if isinstance(data["data"], str):
                        data["data"] = [data["data"]]
                    else:
                        data["data"] = [str(data["data"])]

            elif file_type in ["kudos", "comments", "laps", "gears", "clubs"]:
                # Add activity_id if missing for social data
                if (
                    file_type in ["kudos", "comments", "laps"]
                    and "activity_id" not in data
                ):
                    # Try to extract from filename
                    try:
                        activity_id = int(filename.split("_")[0])
                        data["activity_id"] = activity_id
                    except (ValueError, IndexError):
                        continue  # Skip if can't determine activity ID

            rows.append(data)

        except json.JSONDecodeError:
            print(f"❌ Invalid line ignored in {filename}")
            continue
        except Exception as e:
            print(f"⚠️  Data validation error in {filename}: {e}")
            continue

    if not rows:
        raise ValueError(f"Empty or invalid file: {filename}")

    # Get appropriate schema and load to BigQuery
    schema = get_schema_for_type(file_type)

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
    print(f"✅ {filename} loaded with {len(rows)} rows to {table_id}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Ingest Strava data from GCS to BigQuery"
    )
    parser.add_argument(
        "--env", choices=["dev", "prd"], required=True, help="Environment (dev or prd)"
    )
    parser.add_argument("--project", required=True, help="GCP Project ID")
    args = parser.parse_args()

    config = get_env_config(args.env)
    bucket = config["bucket"]
    dataset = config["bq_dataset"]
    inserted_at = datetime.utcnow().isoformat()

    print(f"🔍 Searching for Strava files in gs://{bucket}/strava/landing/")
    uris = list_gcs_files(bucket)
    print(f"📁 Found {len(uris)} files to process")

    for uri in uris:
        try:
            filename = uri.split("/")[-1]
            file_type = detect_file_type(filename)

            # Route to appropriate BigQuery table
            table_id = f"{args.project}.{dataset}.staging_strava_{file_type}"

            print(f"📊 Processing {file_type} file: {filename}")
            load_jsonl_with_metadata(uri, table_id, inserted_at, file_type)

            # Move to archive on success
            source_path = "/".join(uri.split("/")[3:])
            move_gcs_file(bucket, source_path, "archive")

        except Exception as e:
            print(f"❌ Ingestion error for {uri}: {e}")
            source_path = "/".join(uri.split("/")[3:])
            move_gcs_file(bucket, source_path, "rejected")

    print(f"✅ Strava ingestion completed for {args.env} environment")
