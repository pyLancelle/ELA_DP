#!/usr/bin/env python3
"""
Garmin Connect BigQuery Ingestion
---------------------------------
Ingests Garmin health and fitness data from GCS to BigQuery.
Follows the same pattern as spotify_ingest.py for consistency.

Supported data types:
- Activities (workouts, GPS, performance metrics)
- Sleep (daily sleep quality, stages, duration)  
- Heart Rate (time-series health data)
- Body Battery (wellness metrics)
- Stress levels

Usage:
    python -m src.connectors.garmin.garmin_ingest --env dev --project YOUR_PROJECT_ID
    python -m src.connectors.garmin.garmin_ingest --env prd --project YOUR_PROJECT_ID
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


# Garmin Activities Schema
garmin_activities_schema = [
    bigquery.SchemaField("activityId", "STRING", "NULLABLE"),
    bigquery.SchemaField("activityName", "STRING", "NULLABLE"),
    bigquery.SchemaField("description", "STRING", "NULLABLE"),
    bigquery.SchemaField("startTimeLocal", "STRING", "NULLABLE"),
    bigquery.SchemaField("startTimeGMT", "STRING", "NULLABLE"),
    bigquery.SchemaField(
        "activityType",
        "RECORD",
        "NULLABLE",
        None,
        None,
        (
            bigquery.SchemaField("typeId", "INTEGER", "NULLABLE"),
            bigquery.SchemaField("typeKey", "STRING", "NULLABLE"),
            bigquery.SchemaField("parentTypeId", "INTEGER", "NULLABLE"),
        ),
    ),
    bigquery.SchemaField("distance", "FLOAT", "NULLABLE"),
    bigquery.SchemaField("duration", "FLOAT", "NULLABLE"),
    bigquery.SchemaField("elapsedDuration", "FLOAT", "NULLABLE"),
    bigquery.SchemaField("movingDuration", "FLOAT", "NULLABLE"),
    bigquery.SchemaField("elevationGain", "FLOAT", "NULLABLE"),
    bigquery.SchemaField("elevationLoss", "FLOAT", "NULLABLE"),
    bigquery.SchemaField("averageSpeed", "FLOAT", "NULLABLE"),
    bigquery.SchemaField("maxSpeed", "FLOAT", "NULLABLE"),
    bigquery.SchemaField("startLatitude", "FLOAT", "NULLABLE"),
    bigquery.SchemaField("startLongitude", "FLOAT", "NULLABLE"),
    bigquery.SchemaField("endLatitude", "FLOAT", "NULLABLE"),
    bigquery.SchemaField("endLongitude", "FLOAT", "NULLABLE"),
    bigquery.SchemaField("averageHR", "INTEGER", "NULLABLE"),
    bigquery.SchemaField("maxHR", "INTEGER", "NULLABLE"),
    bigquery.SchemaField("calories", "INTEGER", "NULLABLE"),
    bigquery.SchemaField("bmrCalories", "INTEGER", "NULLABLE"),
    bigquery.SchemaField("averageRunningCadenceInStepsPerMinute", "FLOAT", "NULLABLE"),
    bigquery.SchemaField("maxRunningCadenceInStepsPerMinute", "FLOAT", "NULLABLE"),
    bigquery.SchemaField("steps", "INTEGER", "NULLABLE"),
    bigquery.SchemaField("strokes", "INTEGER", "NULLABLE"),
    bigquery.SchemaField("avgStrokes", "FLOAT", "NULLABLE"),
    bigquery.SchemaField("minStrokes", "INTEGER", "NULLABLE"),
    bigquery.SchemaField("workoutStepCount", "INTEGER", "NULLABLE"),
    bigquery.SchemaField("poolLength", "FLOAT", "NULLABLE"),
    bigquery.SchemaField(
        "unitOfPoolLength",
        "RECORD",
        "NULLABLE",
        None,
        None,
        (
            bigquery.SchemaField("unitId", "INTEGER", "NULLABLE"),
            bigquery.SchemaField("unitKey", "STRING", "NULLABLE"),
            bigquery.SchemaField("factor", "FLOAT", "NULLABLE"),
        ),
    ),
    bigquery.SchemaField("hasPolyline", "BOOLEAN", "NULLABLE"),
    bigquery.SchemaField("ownerId", "INTEGER", "NULLABLE"),
    bigquery.SchemaField("ownerDisplayName", "STRING", "NULLABLE"),
    bigquery.SchemaField("ownerFullName", "STRING", "NULLABLE"),
    bigquery.SchemaField("ownerProfileImageUrlLarge", "STRING", "NULLABLE"),
    bigquery.SchemaField("ownerProfileImageUrlMedium", "STRING", "NULLABLE"),
    bigquery.SchemaField("ownerProfileImageUrlSmall", "STRING", "NULLABLE"),
    bigquery.SchemaField(
        "eventType",
        "RECORD",
        "NULLABLE",
        None,
        None,
        (
            bigquery.SchemaField("typeId", "INTEGER", "NULLABLE"),
            bigquery.SchemaField("typeKey", "STRING", "NULLABLE"),
            bigquery.SchemaField("sortOrder", "INTEGER", "NULLABLE"),
        ),
    ),
    bigquery.SchemaField(
        "accessControlRuleList",
        "RECORD",
        "REPEATED",
        None,
        None,
        (
            bigquery.SchemaField("typeId", "INTEGER", "NULLABLE"),
            bigquery.SchemaField("typeKey", "STRING", "NULLABLE"),
        ),
    ),
    bigquery.SchemaField("metadataId", "INTEGER", "NULLABLE"),
    bigquery.SchemaField("moderateIntensityMinutes", "INTEGER", "NULLABLE"),
    bigquery.SchemaField("vigorousIntensityMinutes", "INTEGER", "NULLABLE"),
    bigquery.SchemaField("pr", "BOOLEAN", "NULLABLE"),
    # Metadata fields
    bigquery.SchemaField("dp_inserted_at", "TIMESTAMP", "NULLABLE"),
    bigquery.SchemaField("source_file", "STRING", "NULLABLE"),
]

# Garmin Sleep Schema
garmin_sleep_schema = [
    bigquery.SchemaField(
        "dailySleepDTO",
        "RECORD",
        "NULLABLE",
        None,
        None,
        (
            bigquery.SchemaField("id", "STRING", "NULLABLE"),
            bigquery.SchemaField("userProfilePK", "STRING", "NULLABLE"),
            bigquery.SchemaField("calendarDate", "STRING", "NULLABLE"),
            bigquery.SchemaField("sleepTimeSeconds", "INTEGER", "NULLABLE"),
            bigquery.SchemaField("napTimeSeconds", "INTEGER", "NULLABLE"),
            bigquery.SchemaField("sleepStartTimestampGMT", "STRING", "NULLABLE"),
            bigquery.SchemaField("sleepEndTimestampGMT", "STRING", "NULLABLE"),
            bigquery.SchemaField("sleepStartTimestampLocal", "STRING", "NULLABLE"),
            bigquery.SchemaField("sleepEndTimestampLocal", "STRING", "NULLABLE"),
            bigquery.SchemaField("autoSleepStartTimestampGMT", "STRING", "NULLABLE"),
            bigquery.SchemaField("autoSleepEndTimestampGMT", "STRING", "NULLABLE"),
            bigquery.SchemaField("sleepQualityTypePK", "INTEGER", "NULLABLE"),
            bigquery.SchemaField("sleepResultTypePK", "INTEGER", "NULLABLE"),
            bigquery.SchemaField("unmeasurableSleepSeconds", "INTEGER", "NULLABLE"),
            bigquery.SchemaField("deepSleepSeconds", "INTEGER", "NULLABLE"),
            bigquery.SchemaField("lightSleepSeconds", "INTEGER", "NULLABLE"),
            bigquery.SchemaField("remSleepSeconds", "INTEGER", "NULLABLE"),
            bigquery.SchemaField("awakeSleepSeconds", "INTEGER", "NULLABLE"),
            bigquery.SchemaField(
                "sleepScores",
                "RECORD",
                "NULLABLE",
                None,
                None,
                (
                    bigquery.SchemaField(
                        "overall",
                        "RECORD",
                        "NULLABLE",
                        None,
                        None,
                        (
                            bigquery.SchemaField("value", "INTEGER", "NULLABLE"),
                            bigquery.SchemaField("qualifierKey", "STRING", "NULLABLE"),
                        ),
                    ),
                    bigquery.SchemaField(
                        "composition",
                        "RECORD",
                        "NULLABLE",
                        None,
                        None,
                        (
                            bigquery.SchemaField("value", "INTEGER", "NULLABLE"),
                            bigquery.SchemaField("qualifierKey", "STRING", "NULLABLE"),
                        ),
                    ),
                    bigquery.SchemaField(
                        "revitalization",
                        "RECORD",
                        "NULLABLE",
                        None,
                        None,
                        (
                            bigquery.SchemaField("value", "INTEGER", "NULLABLE"),
                            bigquery.SchemaField("qualifierKey", "STRING", "NULLABLE"),
                        ),
                    ),
                    bigquery.SchemaField(
                        "duration",
                        "RECORD",
                        "NULLABLE",
                        None,
                        None,
                        (
                            bigquery.SchemaField("value", "INTEGER", "NULLABLE"),
                            bigquery.SchemaField("qualifierKey", "STRING", "NULLABLE"),
                        ),
                    ),
                ),
            ),
        ),
    ),
    # Sleep level data
    bigquery.SchemaField(
        "sleepLevels",
        "RECORD",
        "REPEATED",
        None,
        None,
        (
            bigquery.SchemaField("startGMT", "STRING", "NULLABLE"),
            bigquery.SchemaField("endGMT", "STRING", "NULLABLE"),
            bigquery.SchemaField("activityLevel", "STRING", "NULLABLE"),
        ),
    ),
    # Sleep movement data
    bigquery.SchemaField(
        "sleepMovement",
        "RECORD",
        "REPEATED",
        None,
        None,
        (
            bigquery.SchemaField("startGMT", "STRING", "NULLABLE"),
            bigquery.SchemaField("endGMT", "STRING", "NULLABLE"),
            bigquery.SchemaField("activityLevel", "STRING", "NULLABLE"),
        ),
    ),
    # Metadata fields
    bigquery.SchemaField("dp_inserted_at", "TIMESTAMP", "NULLABLE"),
    bigquery.SchemaField("source_file", "STRING", "NULLABLE"),
]

# Garmin Heart Rate Schema
garmin_heart_rate_schema = [
    bigquery.SchemaField("userProfilePK", "STRING", "NULLABLE"),
    bigquery.SchemaField("calendarDate", "STRING", "NULLABLE"),
    bigquery.SchemaField("startTimestampGMT", "STRING", "NULLABLE"),
    bigquery.SchemaField("endTimestampGMT", "STRING", "NULLABLE"),
    bigquery.SchemaField("startTimestampLocal", "STRING", "NULLABLE"),
    bigquery.SchemaField("endTimestampLocal", "STRING", "NULLABLE"),
    bigquery.SchemaField("maxHeartRate", "INTEGER", "NULLABLE"),
    bigquery.SchemaField("minHeartRate", "INTEGER", "NULLABLE"),
    bigquery.SchemaField("restingHeartRate", "INTEGER", "NULLABLE"),
    bigquery.SchemaField("lastSevenDaysAvgRestingHeartRate", "INTEGER", "NULLABLE"),
    bigquery.SchemaField(
        "heartRateValueDescriptors",
        "RECORD",
        "REPEATED",
        None,
        None,
        (
            bigquery.SchemaField("key", "STRING", "NULLABLE"),
            bigquery.SchemaField("index", "INTEGER", "NULLABLE"),
        ),
    ),
    # Heart rate values (time series)
    bigquery.SchemaField(
        "heartRateValues",
        "RECORD",
        "REPEATED",
        None,
        None,
        (
            bigquery.SchemaField("timestamp", "STRING", "NULLABLE"),
            bigquery.SchemaField("heartRate", "INTEGER", "NULLABLE"),
        ),
    ),
    # Metadata fields
    bigquery.SchemaField("dp_inserted_at", "TIMESTAMP", "NULLABLE"),
    bigquery.SchemaField("source_file", "STRING", "NULLABLE"),
]

# Body Battery Schema
garmin_body_battery_schema = [
    bigquery.SchemaField("userProfilePK", "STRING", "NULLABLE"),
    bigquery.SchemaField("calendarDate", "STRING", "NULLABLE"),
    bigquery.SchemaField("startTimestampGMT", "STRING", "NULLABLE"),
    bigquery.SchemaField("endTimestampGMT", "STRING", "NULLABLE"),
    bigquery.SchemaField("startTimestampLocal", "STRING", "NULLABLE"),
    bigquery.SchemaField("endTimestampLocal", "STRING", "NULLABLE"),
    bigquery.SchemaField("charged", "INTEGER", "NULLABLE"),
    bigquery.SchemaField("drained", "INTEGER", "NULLABLE"),
    bigquery.SchemaField(
        "bodyBatteryValueDescriptors",
        "RECORD",
        "REPEATED",
        None,
        None,
        (
            bigquery.SchemaField("key", "STRING", "NULLABLE"),
            bigquery.SchemaField("index", "INTEGER", "NULLABLE"),
        ),
    ),
    # Body battery values (time series)
    bigquery.SchemaField(
        "bodyBatteryValues",
        "RECORD",
        "REPEATED",
        None,
        None,
        (
            bigquery.SchemaField("timestamp", "STRING", "NULLABLE"),
            bigquery.SchemaField("level", "INTEGER", "NULLABLE"),
        ),
    ),
    # Metadata fields
    bigquery.SchemaField("dp_inserted_at", "TIMESTAMP", "NULLABLE"),
    bigquery.SchemaField("source_file", "STRING", "NULLABLE"),
]

# Stress Schema
garmin_stress_schema = [
    bigquery.SchemaField("userProfilePK", "STRING", "NULLABLE"),
    bigquery.SchemaField("calendarDate", "STRING", "NULLABLE"),
    bigquery.SchemaField("startTimestampGMT", "STRING", "NULLABLE"),
    bigquery.SchemaField("endTimestampGMT", "STRING", "NULLABLE"),
    bigquery.SchemaField("startTimestampLocal", "STRING", "NULLABLE"),
    bigquery.SchemaField("endTimestampLocal", "STRING", "NULLABLE"),
    bigquery.SchemaField("maxStressLevel", "INTEGER", "NULLABLE"),
    bigquery.SchemaField("avgStressLevel", "INTEGER", "NULLABLE"),
    bigquery.SchemaField(
        "stressValueDescriptors",
        "RECORD",
        "REPEATED",
        None,
        None,
        (
            bigquery.SchemaField("key", "STRING", "NULLABLE"),
            bigquery.SchemaField("index", "INTEGER", "NULLABLE"),
        ),
    ),
    # Stress values (time series)
    bigquery.SchemaField(
        "stressValues",
        "RECORD",
        "REPEATED",
        None,
        None,
        (
            bigquery.SchemaField("timestamp", "STRING", "NULLABLE"),
            bigquery.SchemaField("stress", "INTEGER", "NULLABLE"),
        ),
    ),
    # Metadata fields
    bigquery.SchemaField("dp_inserted_at", "TIMESTAMP", "NULLABLE"),
    bigquery.SchemaField("source_file", "STRING", "NULLABLE"),
]

# Race Predictor Schema
garmin_race_predictor_schema = [
    bigquery.SchemaField("userProfilePK", "STRING", "NULLABLE"),
    bigquery.SchemaField("calendarDate", "STRING", "NULLABLE"),
    bigquery.SchemaField("generalMessage", "STRING", "NULLABLE"),
    bigquery.SchemaField(
        "raceDistances",
        "RECORD",
        "REPEATED",
        None,
        None,
        (
            bigquery.SchemaField("raceDistanceId", "INTEGER", "NULLABLE"),
            bigquery.SchemaField("raceDistanceKey", "STRING", "NULLABLE"),
            bigquery.SchemaField("raceDistanceTypeId", "INTEGER", "NULLABLE"),
            bigquery.SchemaField("distanceInMeters", "FLOAT", "NULLABLE"),
            bigquery.SchemaField("predictedTime", "INTEGER", "NULLABLE"),  # in seconds
            bigquery.SchemaField("predictedTimeFormatted", "STRING", "NULLABLE"),
            bigquery.SchemaField("confidence", "STRING", "NULLABLE"),
            bigquery.SchemaField("workoutProgramAvailable", "BOOLEAN", "NULLABLE"),
        ),
    ),
    bigquery.SchemaField(
        "vo2Max",
        "RECORD",
        "NULLABLE",
        None,
        None,
        (
            bigquery.SchemaField(
                "generic",
                "RECORD",
                "NULLABLE",
                None,
                None,
                (
                    bigquery.SchemaField("value", "FLOAT", "NULLABLE"),
                    bigquery.SchemaField(
                        "measurementTimestampGMT", "STRING", "NULLABLE"
                    ),
                ),
            ),
            bigquery.SchemaField(
                "running",
                "RECORD",
                "NULLABLE",
                None,
                None,
                (
                    bigquery.SchemaField("value", "FLOAT", "NULLABLE"),
                    bigquery.SchemaField(
                        "measurementTimestampGMT", "STRING", "NULLABLE"
                    ),
                ),
            ),
            bigquery.SchemaField(
                "cycling",
                "RECORD",
                "NULLABLE",
                None,
                None,
                (
                    bigquery.SchemaField("value", "FLOAT", "NULLABLE"),
                    bigquery.SchemaField(
                        "measurementTimestampGMT", "STRING", "NULLABLE"
                    ),
                ),
            ),
        ),
    ),
    bigquery.SchemaField(
        "fitnessAge",
        "RECORD",
        "NULLABLE",
        None,
        None,
        (
            bigquery.SchemaField("value", "INTEGER", "NULLABLE"),
            bigquery.SchemaField("measurementTimestampGMT", "STRING", "NULLABLE"),
        ),
    ),
    # Metadata fields
    bigquery.SchemaField("dp_inserted_at", "TIMESTAMP", "NULLABLE"),
    bigquery.SchemaField("source_file", "STRING", "NULLABLE"),
]

# HRV (Heart Rate Variability) Schema
garmin_hrv_schema = [
    bigquery.SchemaField("userProfilePK", "STRING", "NULLABLE"),
    bigquery.SchemaField("calendarDate", "STRING", "NULLABLE"),
    bigquery.SchemaField("createTimeStampGMT", "STRING", "NULLABLE"),
    bigquery.SchemaField("createTimeStampLocal", "STRING", "NULLABLE"),
    bigquery.SchemaField(
        "hrvSummary",
        "RECORD",
        "NULLABLE",
        None,
        None,
        (
            bigquery.SchemaField("lastNightAvg", "FLOAT", "NULLABLE"),
            bigquery.SchemaField("lastNight5MinHigh", "FLOAT", "NULLABLE"),
            bigquery.SchemaField(
                "baseline",
                "RECORD",
                "NULLABLE",
                None,
                None,
                (
                    bigquery.SchemaField("lowUpper", "FLOAT", "NULLABLE"),
                    bigquery.SchemaField("balancedLower", "FLOAT", "NULLABLE"),
                    bigquery.SchemaField("balancedUpper", "FLOAT", "NULLABLE"),
                    bigquery.SchemaField("marker", "FLOAT", "NULLABLE"),
                ),
            ),
            bigquery.SchemaField("status", "STRING", "NULLABLE"),
            bigquery.SchemaField("feedbackPhrase", "STRING", "NULLABLE"),
            bigquery.SchemaField("statusColor", "STRING", "NULLABLE"),
        ),
    ),
    bigquery.SchemaField(
        "hrvReadings",
        "RECORD",
        "REPEATED",
        None,
        None,
        (
            bigquery.SchemaField("readingTimeGMT", "STRING", "NULLABLE"),
            bigquery.SchemaField("readingTimeLocal", "STRING", "NULLABLE"),
            bigquery.SchemaField("value", "FLOAT", "NULLABLE"),
            bigquery.SchemaField("hrvStatus", "STRING", "NULLABLE"),
        ),
    ),
    # Weekly and monthly averages
    bigquery.SchemaField("weeklyAvg", "FLOAT", "NULLABLE"),
    bigquery.SchemaField("lastSevenDaysAvg", "FLOAT", "NULLABLE"),
    bigquery.SchemaField("monthlyAvg", "FLOAT", "NULLABLE"),
    # Metadata fields
    bigquery.SchemaField("dp_inserted_at", "TIMESTAMP", "NULLABLE"),
    bigquery.SchemaField("source_file", "STRING", "NULLABLE"),
]


def detect_file_type(filename: str) -> str:
    """Detect the type of Garmin data file based on filename patterns."""
    filename_lower = filename.lower()
    if "activities" in filename_lower:
        return "activities"
    elif "sleep" in filename_lower:
        return "sleep"
    elif "heart_rate" in filename_lower or "heartrate" in filename_lower:
        return "heart_rate"
    elif "body_battery" in filename_lower:
        return "body_battery"
    elif "stress" in filename_lower:
        return "stress"
    elif "race_predictor" in filename_lower or "racepredictor" in filename_lower:
        return "race_predictor"
    elif "hrv" in filename_lower:
        return "hrv"
    else:
        return "activities"  # Default fallback


def get_schema_for_type(file_type: str):
    """Get the appropriate schema for a Garmin file type."""
    schemas = {
        "activities": garmin_activities_schema,
        "sleep": garmin_sleep_schema,
        "heart_rate": garmin_heart_rate_schema,
        "body_battery": garmin_body_battery_schema,
        "stress": garmin_stress_schema,
        "race_predictor": garmin_race_predictor_schema,
        "hrv": garmin_hrv_schema,
    }
    return schemas.get(file_type, garmin_activities_schema)  # Default fallback


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


def load_jsonl_with_metadata(uri: str, table_id: str, inserted_at: str, file_type: str):
    """Load JSONL file from GCS to BigQuery with metadata and data validation."""
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

            # Validate and clean data based on file type
            if file_type == "activities":
                # Ensure required fields exist
                if "activityId" not in data or data["activityId"] is None:
                    continue  # Skip invalid activities

                # Ensure activityType is properly structured
                if "activityType" not in data or data["activityType"] is None:
                    data["activityType"] = {}

                # Handle accessControlRuleList properly
                if "accessControlRuleList" not in data:
                    data["accessControlRuleList"] = []
                elif not isinstance(data["accessControlRuleList"], list):
                    data["accessControlRuleList"] = []

            elif file_type == "sleep":
                # Validate sleep data structure
                if "dailySleepDTO" not in data:
                    data["dailySleepDTO"] = {}

                # Ensure sleepLevels and sleepMovement are lists
                if "sleepLevels" not in data:
                    data["sleepLevels"] = []
                elif not isinstance(data["sleepLevels"], list):
                    data["sleepLevels"] = []

                if "sleepMovement" not in data:
                    data["sleepMovement"] = []
                elif not isinstance(data["sleepMovement"], list):
                    data["sleepMovement"] = []

            elif file_type == "heart_rate":
                # Ensure heart rate values are properly structured
                if "heartRateValues" not in data:
                    data["heartRateValues"] = []
                elif not isinstance(data["heartRateValues"], list):
                    data["heartRateValues"] = []

                if "heartRateValueDescriptors" not in data:
                    data["heartRateValueDescriptors"] = []
                elif not isinstance(data["heartRateValueDescriptors"], list):
                    data["heartRateValueDescriptors"] = []

            elif file_type == "body_battery":
                # Ensure body battery values are properly structured
                if "bodyBatteryValues" not in data:
                    data["bodyBatteryValues"] = []
                elif not isinstance(data["bodyBatteryValues"], list):
                    data["bodyBatteryValues"] = []

                if "bodyBatteryValueDescriptors" not in data:
                    data["bodyBatteryValueDescriptors"] = []
                elif not isinstance(data["bodyBatteryValueDescriptors"], list):
                    data["bodyBatteryValueDescriptors"] = []

            elif file_type == "stress":
                # Ensure stress values are properly structured
                if "stressValues" not in data:
                    data["stressValues"] = []
                elif not isinstance(data["stressValues"], list):
                    data["stressValues"] = []

                if "stressValueDescriptors" not in data:
                    data["stressValueDescriptors"] = []
                elif not isinstance(data["stressValueDescriptors"], list):
                    data["stressValueDescriptors"] = []

            elif file_type == "race_predictor":
                # Ensure race distances are properly structured
                if "raceDistances" not in data:
                    data["raceDistances"] = []
                elif not isinstance(data["raceDistances"], list):
                    data["raceDistances"] = []

                # Ensure vo2Max is properly structured
                if "vo2Max" not in data or data["vo2Max"] is None:
                    data["vo2Max"] = {}

                # Ensure fitnessAge is properly structured
                if "fitnessAge" not in data or data["fitnessAge"] is None:
                    data["fitnessAge"] = {}

            elif file_type == "hrv":
                # Ensure HRV summary is properly structured
                if "hrvSummary" not in data or data["hrvSummary"] is None:
                    data["hrvSummary"] = {}

                # Ensure HRV readings are properly structured
                if "hrvReadings" not in data:
                    data["hrvReadings"] = []
                elif not isinstance(data["hrvReadings"], list):
                    data["hrvReadings"] = []

            rows.append(data)

        except json.JSONDecodeError:
            print(f"❌ Invalid line ignored in {filename}")
            continue
        except Exception as e:
            print(f"⚠️  Data validation error in {filename}: {e}")
            continue

    if not rows:
        raise ValueError(f"Empty or invalid file: {filename}")

    # Get appropriate schema
    schema = get_schema_for_type(file_type)

    # Load to BigQuery
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
        description="Ingest Garmin data from GCS to BigQuery"
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

    print(f"🔍 Searching for Garmin files in gs://{bucket}/garmin/landing/")
    uris = list_gcs_files(bucket)
    print(f"📁 Found {len(uris)} files to process")

    for uri in uris:
        try:
            filename = uri.split("/")[-1]
            file_type = detect_file_type(filename)

            # Route to appropriate BigQuery table
            table_id = f"{args.project}.{dataset}.staging_garmin_{file_type}"

            print(f"📊 Processing {file_type} file: {filename}")
            load_jsonl_with_metadata(uri, table_id, inserted_at, file_type)

            # Move to archive on success
            source_path = "/".join(uri.split("/")[3:])
            move_gcs_file(bucket, source_path, "archive")

        except Exception as e:
            print(f"❌ Ingestion error for {uri}: {e}")
            source_path = "/".join(uri.split("/")[3:])
            move_gcs_file(bucket, source_path, "rejected")

    print(f"✅ Garmin ingestion completed for {args.env} environment")
