#!/usr/bin/env python3
"""
Schema definition for Garmin Activities with hybrid approach.

Core fields are parsed and typed in Python for performance.
Extended fields remain in raw_data JSON for flexibility.
"""

from google.cloud import bigquery
from typing import Dict, Any, Optional
from datetime import datetime


# Core fields that will be parsed as typed columns
CORE_FIELDS_MAPPING = {
    # Identifiers
    "activity_id": ("$.activityId", "INT64"),
    "activity_name": ("$.activityName", "STRING"),
    "activity_date": ("$.startTimeGMT", "DATE"),  # Extracted as date

    # Timestamps
    "start_time_gmt": ("$.startTimeGMT", "TIMESTAMP"),
    "start_time_local": ("$.startTimeLocal", "TIMESTAMP"),
    "end_time_gmt": ("$.endTimeGMT", "TIMESTAMP"),

    # Activity type (used for filtering)
    "activity_type_id": ("$.activityType.typeId", "INT64"),
    "activity_type_key": ("$.activityType.typeKey", "STRING"),
    "sport_type_id": ("$.sportTypeId", "INT64"),

    # Core metrics (used in most analyses)
    "distance_meters": ("$.distance", "FLOAT64"),
    "duration_seconds": ("$.duration", "FLOAT64"),
    "elapsed_duration_seconds": ("$.elapsedDuration", "FLOAT64"),
    "moving_duration_seconds": ("$.movingDuration", "FLOAT64"),

    # Elevation (common filter/aggregation)
    "elevation_gain_meters": ("$.elevationGain", "FLOAT64"),
    "elevation_loss_meters": ("$.elevationLoss", "FLOAT64"),

    # Heart rate (frequent analysis)
    "average_hr_bpm": ("$.averageHR", "INT64"),
    "max_hr_bpm": ("$.maxHR", "INT64"),

    # Speed
    "average_speed_mps": ("$.averageSpeed", "FLOAT64"),
    "max_speed_mps": ("$.maxSpeed", "FLOAT64"),

    # Energy
    "calories": ("$.calories", "FLOAT64"),

    # Location (for geography queries)
    "start_latitude": ("$.startLatitude", "FLOAT64"),
    "start_longitude": ("$.startLongitude", "FLOAT64"),
    "location_name": ("$.locationName", "STRING"),
}


def get_bigquery_schema() -> list[bigquery.SchemaField]:
    """
    Returns the BigQuery schema for lake_garmin__svc_activities.

    Includes:
    - Core fields as typed columns (parsed in Python)
    - raw_data as JSON (for extended fields)
    - Metadata fields
    """
    schema = []

    # Add core fields as typed columns
    for field_name, (_, bq_type) in CORE_FIELDS_MAPPING.items():
        schema.append(
            bigquery.SchemaField(
                field_name,
                bq_type,
                mode="NULLABLE",
                description=f"Core field extracted from JSON for performance"
            )
        )

    # Add raw_data for extended fields (flexibility)
    schema.append(
        bigquery.SchemaField(
            "raw_data",
            "JSON",
            mode="NULLABLE",
            description="Complete activity data for extended fields extraction"
        )
    )

    # Add metadata
    schema.extend([
        bigquery.SchemaField(
            "dp_inserted_at",
            "TIMESTAMP",
            mode="NULLABLE",
            description="Data platform ingestion timestamp"
        ),
        bigquery.SchemaField(
            "source_file",
            "STRING",
            mode="NULLABLE",
            description="Source JSONL filename"
        ),
    ])

    return schema


def extract_value(data: Dict[str, Any], json_path: str, target_type: str) -> Optional[Any]:
    """
    Extract a value from nested JSON using dot notation path.

    Args:
        data: Source JSON dictionary
        json_path: JSONPath-like string (e.g., "$.activityType.typeId")
        target_type: Target BigQuery type (INT64, STRING, FLOAT64, etc.)

    Returns:
        Extracted and casted value, or None if not found
    """
    # Remove $. prefix if present
    path = json_path.replace("$.", "")

    # Navigate nested structure
    current = data
    for key in path.split("."):
        if isinstance(current, dict) and key in current:
            current = current[key]
        else:
            return None

    # Cast to target type
    if current is None:
        return None

    try:
        if target_type == "INT64":
            return int(current)
        elif target_type == "FLOAT64":
            return float(current)
        elif target_type == "STRING":
            return str(current)
        elif target_type == "TIMESTAMP":
            # Garmin timestamps are in milliseconds
            if isinstance(current, (int, float)):
                return datetime.fromtimestamp(current / 1000.0)
            return None
        elif target_type == "DATE":
            # Extract date from timestamp
            if isinstance(current, (int, float)):
                return datetime.fromtimestamp(current / 1000.0).date()
            return None
        elif target_type == "BOOL":
            return bool(current)
        else:
            return current
    except (ValueError, TypeError):
        return None


def parse_activity(raw_data: Dict[str, Any], filename: str) -> Dict[str, Any]:
    """
    Parse Garmin activity JSON into structured record with core fields.

    Args:
        raw_data: Complete activity JSON from Garmin API
        filename: Source file name

    Returns:
        Dictionary with core fields + raw_data + metadata
    """
    parsed = {}

    # Extract core fields
    for field_name, (json_path, target_type) in CORE_FIELDS_MAPPING.items():
        parsed[field_name] = extract_value(raw_data, json_path, target_type)

    # Keep complete raw data for extended fields
    parsed["raw_data"] = raw_data

    # Add metadata
    parsed["dp_inserted_at"] = datetime.utcnow()
    parsed["source_file"] = filename

    return parsed
