# Data Ingestion Pipeline Implementation Guide

This guide provides step-by-step instructions to complete the data pipeline for Garmin and Strava, following the proven Spotify pattern: **fetch data → GCS → BigQuery**.

## 🎯 **Objective**

Extend the robust BigQuery ingestion pattern from Spotify to Garmin and Strava, creating a unified data pipeline with:
- **Fetch scripts** (✅ already exist)
- **GCS upload** (✅ already working via orchestration)  
- **BigQuery ingestion** (❌ need to create)
- **Multi-environment support** (DEV/PRD)

## 📋 **Prerequisites**

Before starting, ensure you have:
- [x] Spotify ingestion working as reference (`spotify_ingest.py`)
- [x] Garmin and Strava fetch scripts working
- [x] Multi-environment orchestration configured
- [x] BigQuery datasets created (`dp_lake_dev`, `dp_lake_prd`)
- [x] GCS buckets configured (`ela-dp-dev`, `ela-dp-prd`)

## 🏃‍♂️ **Phase 1: Garmin Ingestion Pipeline**

### **Step 1.1: Create Garmin Ingestion Script**

Create `/src/connectors/garmin/garmin_ingest.py`:

```python
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
"""

import argparse
from datetime import datetime, timezone
from google.cloud import bigquery, storage
import os
import json

# Environment configuration
def get_env_config(env: str):
    if env == "dev" or env == "prd":
        return {
            "bucket": f"ela-dp-{env}",
            "bq_dataset": f"dp_lake_{env}",
        }
    else:
        raise ValueError("Env must be 'dev' or 'prd'.")

# File type detection based on Garmin data patterns
def detect_file_type(filename: str) -> str:
    """Detect the type of Garmin data file based on filename patterns."""
    if "activities" in filename.lower():
        return "activities"
    elif "sleep" in filename.lower():
        return "sleep"
    elif "heart_rate" in filename.lower() or "heartrate" in filename.lower():
        return "heart_rate"
    elif "body_battery" in filename.lower():
        return "body_battery"
    elif "stress" in filename.lower():
        return "stress"
    else:
        return "activities"  # Default fallback

# GCS operations (same pattern as Spotify)
def list_gcs_files(bucket_name: str, prefix: str = "garmin/landing/") -> list:
    client = storage.Client()
    blobs = client.list_blobs(bucket_name, prefix=prefix)
    return [
        f"gs://{bucket_name}/{blob.name}"
        for blob in blobs
        if blob.name.endswith(".jsonl")
    ]

def move_gcs_file(bucket_name: str, source_path: str, dest_prefix: str):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    source_blob = bucket.blob(source_path)
    filename = source_path.split("/")[-1]
    dest_path = f"garmin/{dest_prefix}/{filename}"
    bucket.copy_blob(source_blob, bucket, dest_path)
    source_blob.delete()
    print(f"📁 {source_path} moved to {dest_path}")

# Main ingestion function
def load_jsonl_with_metadata(uri: str, table_id: str, inserted_at: str, file_type: str):
    # Implementation follows spotify_ingest.py pattern
    # Load from GCS, add metadata, insert to BigQuery
    pass  # Implementation details below

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", choices=["dev", "prd"], required=True)
    parser.add_argument("--project", required=True)
    args = parser.parse_args()

    config = get_env_config(args.env)
    bucket = config["bucket"]
    dataset = config["bq_dataset"]
    inserted_at = datetime.utcnow().isoformat()

    uris = list_gcs_files(bucket)
    print(f"🔍 Files found: {len(uris)}")

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
            print(f"❌ Ingestion error {uri}: {e}")
            source_path = "/".join(uri.split("/")[3:])
            move_gcs_file(bucket, source_path, "rejected")
```

### **Step 1.2: Define Garmin BigQuery Schemas**

Add these schema definitions to `garmin_ingest.py`:

```python
# Garmin Activities Schema
garmin_activities_schema = [
    bigquery.SchemaField("activityId", "STRING", "NULLABLE"),
    bigquery.SchemaField("activityName", "STRING", "NULLABLE"),
    bigquery.SchemaField("description", "STRING", "NULLABLE"),
    bigquery.SchemaField("startTimeLocal", "STRING", "NULLABLE"),
    bigquery.SchemaField("startTimeGMT", "STRING", "NULLABLE"),
    bigquery.SchemaField("activityType", "RECORD", "NULLABLE", None, None, (
        bigquery.SchemaField("typeId", "INTEGER", "NULLABLE"),
        bigquery.SchemaField("typeKey", "STRING", "NULLABLE"),
        bigquery.SchemaField("parentTypeId", "INTEGER", "NULLABLE"),
    )),
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
    # Metadata fields
    bigquery.SchemaField("dp_inserted_at", "TIMESTAMP", "NULLABLE"),
    bigquery.SchemaField("source_file", "STRING", "NULLABLE"),
]

# Garmin Sleep Schema
garmin_sleep_schema = [
    bigquery.SchemaField("dailySleepDTO", "RECORD", "NULLABLE", None, None, (
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
    )),
    # Sleep level data
    bigquery.SchemaField("sleepLevels", "RECORD", "REPEATED", None, None, (
        bigquery.SchemaField("startGMT", "STRING", "NULLABLE"),
        bigquery.SchemaField("endGMT", "STRING", "NULLABLE"),
        bigquery.SchemaField("activityLevel", "STRING", "NULLABLE"),
    )),
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
    # Heart rate values (time series)
    bigquery.SchemaField("heartRateValues", "RECORD", "REPEATED", None, None, (
        bigquery.SchemaField("timestamp", "STRING", "NULLABLE"),
        bigquery.SchemaField("heartRate", "INTEGER", "NULLABLE"),
    )),
    # Metadata fields
    bigquery.SchemaField("dp_inserted_at", "TIMESTAMP", "NULLABLE"),
    bigquery.SchemaField("source_file", "STRING", "NULLABLE"),
]

# Schema selection function
def get_schema_for_type(file_type: str):
    """Get the appropriate schema for a Garmin file type."""
    schemas = {
        "activities": garmin_activities_schema,
        "sleep": garmin_sleep_schema,
        "heart_rate": garmin_heart_rate_schema,
        # Add more as needed
    }
    return schemas.get(file_type, garmin_activities_schema)  # Default fallback
```

### **Step 1.3: Complete the Ingestion Function**

Complete the `load_jsonl_with_metadata` function:

```python
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
                    
            elif file_type == "sleep":
                # Validate sleep data structure
                if "dailySleepDTO" not in data:
                    data["dailySleepDTO"] = {}
                    
            elif file_type == "heart_rate":
                # Ensure heart rate values are properly structured
                if "heartRateValues" not in data:
                    data["heartRateValues"] = []

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
```

### **Step 1.4: Test Garmin Ingestion**

Test the Garmin ingestion script:

```bash
# Activate virtual environment
source .venv/bin/activate

# Test DEV environment
python -m src.connectors.garmin.garmin_ingest --env dev --project YOUR_PROJECT_ID

# Test PRD environment (when ready)
python -m src.connectors.garmin.garmin_ingest --env prd --project YOUR_PROJECT_ID
```

## 🏃‍♂️ **Phase 2: Strava Ingestion Pipeline**

### **Step 2.1: Create Strava Ingestion Script**

Create `/src/connectors/strava/strava_ingest.py`:

```python
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
"""

import argparse
from datetime import datetime, timezone
from google.cloud import bigquery, storage
import os
import json

# Follow the same structure as garmin_ingest.py
def get_env_config(env: str):
    if env == "dev" or env == "prd":
        return {
            "bucket": f"ela-dp-{env}",
            "bq_dataset": f"dp_lake_{env}",
        }
    else:
        raise ValueError("Env must be 'dev' or 'prd'.")

def detect_file_type(filename: str) -> str:
    """Detect the type of Strava data file based on filename patterns."""
    if "activities" in filename.lower():
        return "activities"
    elif "athlete" in filename.lower():
        return "athlete"
    elif "kudos" in filename.lower():
        return "kudos"
    elif "comments" in filename.lower():
        return "comments"
    elif "laps" in filename.lower():
        return "laps"
    elif "streams" in filename.lower():
        return "streams"
    elif "gears" in filename.lower():
        return "gears"
    elif "clubs" in filename.lower():
        return "clubs"
    else:
        return "activities"  # Default fallback

# GCS operations (same as Garmin/Spotify)
def list_gcs_files(bucket_name: str, prefix: str = "strava/landing/") -> list:
    client = storage.Client()
    blobs = client.list_blobs(bucket_name, prefix=prefix)
    return [
        f"gs://{bucket_name}/{blob.name}"
        for blob in blobs
        if blob.name.endswith(".jsonl")
    ]

def move_gcs_file(bucket_name: str, source_path: str, dest_prefix: str):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    source_blob = bucket.blob(source_path)
    filename = source_path.split("/")[-1]
    dest_path = f"strava/{dest_prefix}/{filename}"
    bucket.copy_blob(source_blob, bucket, dest_path)
    source_blob.delete()
    print(f"📁 {source_path} moved to {dest_path}")

# Main execution (same pattern)
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", choices=["dev", "prd"], required=True)
    parser.add_argument("--project", required=True)
    args = parser.parse_args()

    config = get_env_config(args.env)
    bucket = config["bucket"]
    dataset = config["bq_dataset"]
    inserted_at = datetime.utcnow().isoformat()

    uris = list_gcs_files(bucket)
    print(f"🔍 Files found: {len(uris)}")

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
            print(f"❌ Ingestion error {uri}: {e}")
            source_path = "/".join(uri.split("/")[3:])
            move_gcs_file(bucket, source_path, "rejected")
```

### **Step 2.2: Define Strava BigQuery Schemas**

Add these schema definitions to `strava_ingest.py`:

```python
# Strava Activities Schema
strava_activities_schema = [
    bigquery.SchemaField("resource_state", "INTEGER", "NULLABLE"),
    bigquery.SchemaField("athlete", "RECORD", "NULLABLE", None, None, (
        bigquery.SchemaField("id", "INTEGER", "NULLABLE"),
        bigquery.SchemaField("resource_state", "INTEGER", "NULLABLE"),
    )),
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
    # Metadata fields
    bigquery.SchemaField("dp_inserted_at", "TIMESTAMP", "NULLABLE"),
    bigquery.SchemaField("source_file", "STRING", "NULLABLE"),
]

# Strava Streams Schema (GPS and sensor data)
strava_streams_schema = [
    bigquery.SchemaField("activity_id", "INTEGER", "NULLABLE"),
    bigquery.SchemaField("type", "STRING", "NULLABLE"),  # latlng, distance, time, altitude, velocity_smooth, etc.
    bigquery.SchemaField("data", "STRING", "REPEATED"),  # Time series data points
    bigquery.SchemaField("series_type", "STRING", "NULLABLE"),
    bigquery.SchemaField("original_size", "INTEGER", "NULLABLE"),
    bigquery.SchemaField("resolution", "STRING", "NULLABLE"),
    # Metadata fields
    bigquery.SchemaField("dp_inserted_at", "TIMESTAMP", "NULLABLE"),
    bigquery.SchemaField("source_file", "STRING", "NULLABLE"),
]

# Schema selection function
def get_schema_for_type(file_type: str):
    """Get the appropriate schema for a Strava file type."""
    schemas = {
        "activities": strava_activities_schema,
        "athlete": strava_athlete_schema,
        "streams": strava_streams_schema,
        # Add simple schemas for other types
        "kudos": [
            bigquery.SchemaField("activity_id", "INTEGER", "NULLABLE"),
            bigquery.SchemaField("data", "STRING", "NULLABLE"),
            bigquery.SchemaField("dp_inserted_at", "TIMESTAMP", "NULLABLE"),
            bigquery.SchemaField("source_file", "STRING", "NULLABLE"),
        ],
        "comments": [
            bigquery.SchemaField("activity_id", "INTEGER", "NULLABLE"),
            bigquery.SchemaField("data", "STRING", "NULLABLE"),
            bigquery.SchemaField("dp_inserted_at", "TIMESTAMP", "NULLABLE"),
            bigquery.SchemaField("source_file", "STRING", "NULLABLE"),
        ],
        # Add more as needed
    }
    return schemas.get(file_type, strava_activities_schema)  # Default fallback
```

### **Step 2.3: Complete Strava Ingestion Function**

Add the same `load_jsonl_with_metadata` function as Garmin, with Strava-specific validation:

```python
def load_jsonl_with_metadata(uri: str, table_id: str, inserted_at: str, file_type: str):
    """Load JSONL file from GCS to BigQuery with Strava-specific validation."""
    from google.cloud import bigquery, storage
    import json

    # Same GCS download logic as Garmin
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
                        
            elif file_type == "athlete":
                # Validate athlete data
                if "id" not in data:
                    continue
                    
            elif file_type == "streams":
                # Ensure streams have activity reference
                if "activity_id" not in data:
                    # Try to extract from filename or skip
                    continue

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
```

### **Step 2.4: Test Strava Ingestion**

Test the Strava ingestion script:

```bash
# Test DEV environment
python -m src.connectors.strava.strava_ingest --env dev --project YOUR_PROJECT_ID

# Test PRD environment (when ready)
python -m src.connectors.strava.strava_ingest --env prd --project YOUR_PROJECT_ID
```

## 🔧 **Phase 3: Integration with Orchestration**

### **Step 3.1: Add Ingestion Jobs to Configuration**

Update `ingestion-config-dev.yaml` to include ingestion steps:

```yaml
# Add these new jobs to the existing configuration

# =================== GARMIN INGESTION JOBS (DEV) ===================
garmin_ingest:
  service: "garmin"
  data_type: "ingest"
  description: "Ingest Garmin data from GCS to BigQuery (DEV)"
  cron: "30 */2 * * *"  # 30 minutes after data fetch
  command: "python -m src.connectors.garmin.garmin_ingest --env dev --project YOUR_PROJECT_ID"
  environment: "dev"
  enabled: true
  dependencies: ["GCP_SERVICE_ACCOUNT_KEY", "GCP_PROJECT_ID"]

# =================== STRAVA INGESTION JOBS (DEV) ===================
strava_ingest:
  service: "strava"
  data_type: "ingest"
  description: "Ingest Strava data from GCS to BigQuery (DEV)"
  cron: "30 */2 * * *"  # 30 minutes after data fetch
  command: "python -m src.connectors.strava.strava_ingest --env dev --project YOUR_PROJECT_ID"
  environment: "dev"
  enabled: true
  dependencies: ["GCP_SERVICE_ACCOUNT_KEY", "GCP_PROJECT_ID"]
```

Update `ingestion-config-prd.yaml` similarly:

```yaml
# =================== GARMIN INGESTION JOBS (PRD) ===================
garmin_ingest:
  service: "garmin"
  data_type: "ingest"
  description: "Ingest Garmin data from GCS to BigQuery (PRD)"
  cron: "30 */4 * * *"  # 30 minutes after data fetch
  command: "python -m src.connectors.garmin.garmin_ingest --env prd --project YOUR_PROJECT_ID"
  environment: "prd"
  enabled: true
  dependencies: ["GCP_SERVICE_ACCOUNT_KEY", "GCP_PROJECT_ID"]

# =================== STRAVA INGESTION JOBS (PRD) ===================
strava_ingest:
  service: "strava"
  data_type: "ingest"
  description: "Ingest Strava data from GCS to BigQuery (PRD)"
  cron: "30 */6 * * *"  # 30 minutes after data fetch
  command: "python -m src.connectors.strava.strava_ingest --env prd --project YOUR_PROJECT_ID"
  environment: "prd"
  enabled: true
  dependencies: ["GCP_SERVICE_ACCOUNT_KEY", "GCP_PROJECT_ID"]
```

### **Step 3.2: Update Job Groups**

Add ingestion jobs to relevant job groups:

```yaml
job_groups:
  garmin_pipeline:
    description: "Complete Garmin pipeline (fetch + ingest)"
    jobs: ["garmin_activities", "garmin_sleep", "garmin_heart_rate", "garmin_ingest"]
  
  strava_pipeline:
    description: "Complete Strava pipeline (fetch + ingest)"
    jobs: ["strava_activities", "strava_athlete", "strava_ingest"]
  
  ingestion_only:
    description: "Run only ingestion jobs (GCS → BigQuery)"
    jobs: ["garmin_ingest", "strava_ingest"]
```

## 🧪 **Phase 4: Testing and Validation**

### **Step 4.1: End-to-End Pipeline Test**

Test the complete pipeline:

```bash
# 1. Test fetch + GCS upload (via orchestration)
# Go to GitHub Actions → Data Ingestion DEV
# Run workflow with job_group: "garmin_pipeline"

# 2. Verify data in BigQuery
# Check tables: dp_lake_dev.staging_garmin_activities, staging_garmin_sleep, etc.

# 3. Test manual ingestion
python -m src.connectors.garmin.garmin_ingest --env dev --project YOUR_PROJECT_ID
python -m src.connectors.strava.strava_ingest --env dev --project YOUR_PROJECT_ID

# 4. Verify data quality
# Check row counts, schema compliance, metadata fields
```

### **Step 4.2: Data Quality Validation**

Create validation queries to check data quality:

```sql
-- Garmin data validation
SELECT 
  source_file,
  COUNT(*) as row_count,
  MIN(dp_inserted_at) as first_insert,
  MAX(dp_inserted_at) as last_insert
FROM `YOUR_PROJECT.dp_lake_dev.staging_garmin_activities`
GROUP BY source_file
ORDER BY last_insert DESC;

-- Strava data validation
SELECT 
  source_file,
  COUNT(*) as row_count,
  MIN(dp_inserted_at) as first_insert,
  MAX(dp_inserted_at) as last_insert
FROM `YOUR_PROJECT.dp_lake_dev.staging_strava_activities`
GROUP BY source_file
ORDER BY last_insert DESC;

-- Check for data quality issues
SELECT 
  'garmin_activities' as table_name,
  COUNT(*) as total_rows,
  COUNT(DISTINCT source_file) as unique_files,
  COUNT(CASE WHEN activityId IS NULL THEN 1 END) as null_activity_ids
FROM `YOUR_PROJECT.dp_lake_dev.staging_garmin_activities`

UNION ALL

SELECT 
  'strava_activities' as table_name,
  COUNT(*) as total_rows,
  COUNT(DISTINCT source_file) as unique_files,
  COUNT(CASE WHEN id IS NULL THEN 1 END) as null_activity_ids
FROM `YOUR_PROJECT.dp_lake_dev.staging_strava_activities`;
```

## 🚀 **Phase 5: Production Deployment**

### **Step 5.1: Enable Production Jobs**

Once DEV testing is successful:

1. **Update PRD configs** with correct PROJECT_ID
2. **Enable PRD ingestion jobs** by setting `enabled: true`
3. **Test PRD pipeline** with manual triggers
4. **Monitor production data quality**

### **Step 5.2: Monitoring and Alerting**

Set up monitoring for:
- **Ingestion job success/failure rates**
- **Data freshness** (time since last successful ingestion)
- **Row count trends** (detect missing data)
- **Error patterns** in rejected files

## 🔄 **Phase 6: Future Enhancements**

### **Immediate Next Steps**
1. **Add more Garmin data types**: Body Battery, Stress, Steps
2. **Enhanced Strava data**: Segments, Efforts, Gear details
3. **Data quality checks**: Automated validation and alerting
4. **DBT models**: Create lake → hub → product layers

### **Advanced Features**
1. **Real-time ingestion**: Webhook-triggered updates
2. **Data lineage tracking**: Enhanced metadata and provenance
3. **Schema evolution**: Handle API changes gracefully
4. **Cost optimization**: Partition tables, optimize queries

## 📚 **Troubleshooting Guide**

### **Common Issues**

1. **Schema Mismatch Errors**
   - Check BigQuery error logs for specific field issues
   - Validate JSON structure matches schema definitions
   - Update schemas for new API fields

2. **GCS Access Issues**
   - Verify service account permissions
   - Check bucket names and paths
   - Ensure secrets are properly configured

3. **Data Quality Issues**
   - Check source data format consistency
   - Validate API response structures
   - Monitor for API changes or rate limits

4. **Orchestration Failures**
   - Check dependency availability (secrets, credentials)
   - Verify job timing and scheduling conflicts
   - Monitor resource usage and timeouts

### **Debugging Commands**

```bash
# Check GCS file structure
gsutil ls -la gs://ela-dp-dev/garmin/landing/
gsutil ls -la gs://ela-dp-dev/strava/landing/

# Test ingestion with verbose logging
python -m src.connectors.garmin.garmin_ingest --env dev --project YOUR_PROJECT_ID --verbose

# Check BigQuery table schemas
bq show YOUR_PROJECT:dp_lake_dev.staging_garmin_activities
bq show YOUR_PROJECT:dp_lake_dev.staging_strava_activities

# Validate configuration
python test_ingestion_config_multi.py
```

## ✅ **Success Criteria**

Your implementation is successful when:

1. **✅ Garmin Pipeline**: Activities, sleep, and heart rate data flows from GCS → BigQuery
2. **✅ Strava Pipeline**: Activities and athlete data flows from GCS → BigQuery  
3. **✅ Multi-Environment**: Both DEV and PRD environments working independently
4. **✅ Orchestration**: Ingestion jobs running automatically via GitHub Actions
5. **✅ Data Quality**: Clean data with proper schemas and metadata
6. **✅ Error Handling**: Failed files moved to rejected folders with clear error logs
7. **✅ Monitoring**: Ability to track pipeline health and data freshness

---

This guide provides everything you need to complete the data ingestion pipeline. Each phase builds on the previous one, and you can implement them incrementally while testing at each step.

**Next Steps**: Start with Garmin ingestion (Phase 1) since it's simpler than Strava, then move to Strava once the pattern is proven. The entire implementation should take 1-2 days of focused work.