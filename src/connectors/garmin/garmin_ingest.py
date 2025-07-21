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


# Comprehensive Garmin Activities Schema - Based on Real Data Analysis
garmin_activities_schema = [
    # Core identifiers
    bigquery.SchemaField("activityId", "INTEGER", "NULLABLE"),
    bigquery.SchemaField("activityName", "STRING", "NULLABLE"),
    bigquery.SchemaField("description", "STRING", "NULLABLE"),
    # Timestamps
    bigquery.SchemaField("startTimeLocal", "STRING", "NULLABLE"),
    bigquery.SchemaField("startTimeGMT", "STRING", "NULLABLE"),
    bigquery.SchemaField("endTimeGMT", "STRING", "NULLABLE"),
    bigquery.SchemaField("beginTimestamp", "INTEGER", "NULLABLE"),
    # Activity type classification
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
            bigquery.SchemaField("isHidden", "BOOLEAN", "NULLABLE"),
            bigquery.SchemaField("restricted", "BOOLEAN", "NULLABLE"),
            bigquery.SchemaField("trimmable", "BOOLEAN", "NULLABLE"),
        ),
    ),
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
    # Basic metrics
    bigquery.SchemaField("distance", "FLOAT", "NULLABLE"),
    bigquery.SchemaField("duration", "FLOAT", "NULLABLE"),
    bigquery.SchemaField("elapsedDuration", "FLOAT", "NULLABLE"),
    bigquery.SchemaField("movingDuration", "FLOAT", "NULLABLE"),
    # Elevation metrics
    bigquery.SchemaField("elevationGain", "FLOAT", "NULLABLE"),
    bigquery.SchemaField("elevationLoss", "FLOAT", "NULLABLE"),
    bigquery.SchemaField("minElevation", "FLOAT", "NULLABLE"),
    bigquery.SchemaField("maxElevation", "FLOAT", "NULLABLE"),
    # Speed metrics
    bigquery.SchemaField("averageSpeed", "FLOAT", "NULLABLE"),
    bigquery.SchemaField("maxSpeed", "FLOAT", "NULLABLE"),
    bigquery.SchemaField("avgGradeAdjustedSpeed", "FLOAT", "NULLABLE"),
    # GPS coordinates
    bigquery.SchemaField("startLatitude", "FLOAT", "NULLABLE"),
    bigquery.SchemaField("startLongitude", "FLOAT", "NULLABLE"),
    bigquery.SchemaField("endLatitude", "FLOAT", "NULLABLE"),
    bigquery.SchemaField("endLongitude", "FLOAT", "NULLABLE"),
    # Health metrics
    bigquery.SchemaField("calories", "FLOAT", "NULLABLE"),
    bigquery.SchemaField("bmrCalories", "FLOAT", "NULLABLE"),
    bigquery.SchemaField(
        "averageHR", "FLOAT", "NULLABLE"
    ),  # Changed to FLOAT to handle decimal values
    bigquery.SchemaField(
        "maxHR", "FLOAT", "NULLABLE"
    ),  # Changed to FLOAT to handle decimal values
    # Running metrics
    bigquery.SchemaField("steps", "INTEGER", "NULLABLE"),
    bigquery.SchemaField("averageRunningCadenceInStepsPerMinute", "FLOAT", "NULLABLE"),
    bigquery.SchemaField("maxRunningCadenceInStepsPerMinute", "FLOAT", "NULLABLE"),
    bigquery.SchemaField("maxDoubleCadence", "FLOAT", "NULLABLE"),
    # Advanced running metrics
    bigquery.SchemaField("avgVerticalOscillation", "FLOAT", "NULLABLE"),
    bigquery.SchemaField("avgGroundContactTime", "FLOAT", "NULLABLE"),
    bigquery.SchemaField("avgStrideLength", "FLOAT", "NULLABLE"),
    bigquery.SchemaField("avgVerticalRatio", "FLOAT", "NULLABLE"),
    # Power metrics
    bigquery.SchemaField("avgPower", "FLOAT", "NULLABLE"),
    bigquery.SchemaField("maxPower", "FLOAT", "NULLABLE"),
    bigquery.SchemaField("normPower", "FLOAT", "NULLABLE"),
    # Training metrics
    bigquery.SchemaField("aerobicTrainingEffect", "FLOAT", "NULLABLE"),
    bigquery.SchemaField("anaerobicTrainingEffect", "FLOAT", "NULLABLE"),
    bigquery.SchemaField("vO2MaxValue", "FLOAT", "NULLABLE"),
    bigquery.SchemaField("trainingEffectLabel", "STRING", "NULLABLE"),
    bigquery.SchemaField("activityTrainingLoad", "FLOAT", "NULLABLE"),
    bigquery.SchemaField("aerobicTrainingEffectMessage", "STRING", "NULLABLE"),
    bigquery.SchemaField("anaerobicTrainingEffectMessage", "STRING", "NULLABLE"),
    # Heart rate zones (time in seconds)
    bigquery.SchemaField("hrTimeInZone_1", "FLOAT", "NULLABLE"),
    bigquery.SchemaField("hrTimeInZone_2", "FLOAT", "NULLABLE"),
    bigquery.SchemaField("hrTimeInZone_3", "FLOAT", "NULLABLE"),
    bigquery.SchemaField("hrTimeInZone_4", "FLOAT", "NULLABLE"),
    bigquery.SchemaField("hrTimeInZone_5", "FLOAT", "NULLABLE"),
    # Power zones (time in seconds)
    bigquery.SchemaField("powerTimeInZone_1", "FLOAT", "NULLABLE"),
    bigquery.SchemaField("powerTimeInZone_2", "FLOAT", "NULLABLE"),
    bigquery.SchemaField("powerTimeInZone_3", "FLOAT", "NULLABLE"),
    bigquery.SchemaField("powerTimeInZone_4", "FLOAT", "NULLABLE"),
    bigquery.SchemaField("powerTimeInZone_5", "FLOAT", "NULLABLE"),
    # Environment
    bigquery.SchemaField("minTemperature", "FLOAT", "NULLABLE"),
    bigquery.SchemaField("maxTemperature", "FLOAT", "NULLABLE"),
    bigquery.SchemaField("locationName", "STRING", "NULLABLE"),
    # Device and user info
    bigquery.SchemaField("deviceId", "INTEGER", "NULLABLE"),
    bigquery.SchemaField("manufacturer", "STRING", "NULLABLE"),
    bigquery.SchemaField("ownerId", "INTEGER", "NULLABLE"),
    bigquery.SchemaField("ownerDisplayName", "STRING", "NULLABLE"),
    bigquery.SchemaField("ownerFullName", "STRING", "NULLABLE"),
    bigquery.SchemaField("ownerProfileImageUrlLarge", "STRING", "NULLABLE"),
    bigquery.SchemaField("ownerProfileImageUrlMedium", "STRING", "NULLABLE"),
    bigquery.SchemaField("ownerProfileImageUrlSmall", "STRING", "NULLABLE"),
    # Activity features
    bigquery.SchemaField("hasPolyline", "BOOLEAN", "NULLABLE"),
    bigquery.SchemaField("hasImages", "BOOLEAN", "NULLABLE"),
    bigquery.SchemaField("hasVideo", "BOOLEAN", "NULLABLE"),
    bigquery.SchemaField("hasHeatMap", "BOOLEAN", "NULLABLE"),
    bigquery.SchemaField("lapCount", "INTEGER", "NULLABLE"),
    bigquery.SchemaField("hasSplits", "BOOLEAN", "NULLABLE"),
    # Intensity minutes
    bigquery.SchemaField("moderateIntensityMinutes", "INTEGER", "NULLABLE"),
    bigquery.SchemaField("vigorousIntensityMinutes", "INTEGER", "NULLABLE"),
    # Body battery and wellness
    bigquery.SchemaField("differenceBodyBattery", "INTEGER", "NULLABLE"),
    bigquery.SchemaField("waterEstimated", "FLOAT", "NULLABLE"),
    # Performance records
    bigquery.SchemaField("fastestSplit_1000", "FLOAT", "NULLABLE"),
    bigquery.SchemaField("fastestSplit_1609", "FLOAT", "NULLABLE"),
    bigquery.SchemaField("fastestSplit_5000", "FLOAT", "NULLABLE"),
    # Activity type specific
    bigquery.SchemaField("sportTypeId", "INTEGER", "NULLABLE"),
    bigquery.SchemaField("workoutId", "INTEGER", "NULLABLE"),
    bigquery.SchemaField("timeZoneId", "INTEGER", "NULLABLE"),
    bigquery.SchemaField("maxVerticalSpeed", "FLOAT", "NULLABLE"),
    bigquery.SchemaField("minActivityLapDuration", "FLOAT", "NULLABLE"),
    # Privacy and sharing
    bigquery.SchemaField(
        "privacy",
        "RECORD",
        "NULLABLE",
        None,
        None,
        (
            bigquery.SchemaField("typeId", "INTEGER", "NULLABLE"),
            bigquery.SchemaField("typeKey", "STRING", "NULLABLE"),
        ),
    ),
    bigquery.SchemaField("userRoles", "STRING", "REPEATED"),
    # Dive info (for water sports)
    bigquery.SchemaField(
        "summarizedDiveInfo",
        "RECORD",
        "NULLABLE",
        None,
        None,
        (bigquery.SchemaField("summarizedDiveGases", "STRING", "REPEATED"),),
    ),
    # Complex nested data stored as JSON strings
    bigquery.SchemaField("split_summaries", "JSON", "NULLABLE"),
    bigquery.SchemaField("access_control_rule_list", "JSON", "NULLABLE"),
    bigquery.SchemaField(
        "activity_type_details", "JSON", "NULLABLE"
    ),  # Store full activityType object
    bigquery.SchemaField(
        "event_type_details", "JSON", "NULLABLE"
    ),  # Store full eventType object
    bigquery.SchemaField(
        "privacy_details", "JSON", "NULLABLE"
    ),  # Store full privacy object
    bigquery.SchemaField(
        "dive_info", "JSON", "NULLABLE"
    ),  # Store full dive info as JSON
    # Boolean flags
    bigquery.SchemaField("favorite", "BOOLEAN", "NULLABLE"),
    bigquery.SchemaField("pr", "BOOLEAN", "NULLABLE"),
    bigquery.SchemaField("purposeful", "BOOLEAN", "NULLABLE"),
    bigquery.SchemaField("parent", "BOOLEAN", "NULLABLE"),
    bigquery.SchemaField("manualActivity", "BOOLEAN", "NULLABLE"),
    bigquery.SchemaField("autoCalcCalories", "BOOLEAN", "NULLABLE"),
    bigquery.SchemaField("userPro", "BOOLEAN", "NULLABLE"),
    bigquery.SchemaField("qualifyingDive", "BOOLEAN", "NULLABLE"),
    bigquery.SchemaField("decoDive", "BOOLEAN", "NULLABLE"),
    bigquery.SchemaField("elevationCorrected", "BOOLEAN", "NULLABLE"),
    bigquery.SchemaField("atpActivity", "BOOLEAN", "NULLABLE"),
    # Metadata fields
    bigquery.SchemaField("dp_inserted_at", "TIMESTAMP", "NULLABLE"),
    bigquery.SchemaField("source_file", "STRING", "NULLABLE"),
]

# Flexible Garmin Sleep Schema - Uses JSON for complex nested structures
garmin_sleep_schema = [
    # Store the entire dailySleepDTO as JSON to handle all possible fields
    bigquery.SchemaField("dailySleepDTO", "JSON", "NULLABLE"),
    # Extract only the most commonly used fields for easy querying
    bigquery.SchemaField(
        "sleep_date", "STRING", "NULLABLE"
    ),  # Extracted from dailySleepDTO.calendarDate
    bigquery.SchemaField(
        "user_profile_pk", "INTEGER", "NULLABLE"
    ),  # Extracted from dailySleepDTO.userProfilePK
    bigquery.SchemaField(
        "sleep_time_seconds", "INTEGER", "NULLABLE"
    ),  # Extracted from dailySleepDTO.sleepTimeSeconds
    bigquery.SchemaField(
        "deep_sleep_seconds", "INTEGER", "NULLABLE"
    ),  # Extracted from dailySleepDTO.deepSleepSeconds
    bigquery.SchemaField(
        "light_sleep_seconds", "INTEGER", "NULLABLE"
    ),  # Extracted from dailySleepDTO.lightSleepSeconds
    bigquery.SchemaField(
        "rem_sleep_seconds", "INTEGER", "NULLABLE"
    ),  # Extracted from dailySleepDTO.remSleepSeconds
    bigquery.SchemaField(
        "awake_sleep_seconds", "INTEGER", "NULLABLE"
    ),  # Extracted from dailySleepDTO.awakeSleepSeconds
    bigquery.SchemaField(
        "sleep_start_timestamp_gmt", "INTEGER", "NULLABLE"
    ),  # Extracted from dailySleepDTO.sleepStartTimestampGMT
    bigquery.SchemaField(
        "sleep_end_timestamp_gmt", "INTEGER", "NULLABLE"
    ),  # Extracted from dailySleepDTO.sleepEndTimestampGMT
    bigquery.SchemaField(
        "average_spo2_value", "FLOAT", "NULLABLE"
    ),  # Extracted from dailySleepDTO.averageSpO2Value
    bigquery.SchemaField(
        "average_respiration_value", "FLOAT", "NULLABLE"
    ),  # Extracted from dailySleepDTO.averageRespirationValue
    bigquery.SchemaField(
        "awake_count", "INTEGER", "NULLABLE"
    ),  # Extracted from dailySleepDTO.awakeCount
    # Sleep scores stored as JSON for maximum flexibility
    bigquery.SchemaField(
        "sleep_scores", "JSON", "NULLABLE"
    ),  # Full sleep scores object
    bigquery.SchemaField(
        "overall_sleep_score", "INTEGER", "NULLABLE"
    ),  # Extracted from dailySleepDTO.sleepScores.overall.value
    # Complex time-series data stored as JSON
    bigquery.SchemaField("sleep_movement", "JSON", "NULLABLE"),
    bigquery.SchemaField("wellness_spo2_sleep_summary", "JSON", "NULLABLE"),
    bigquery.SchemaField("sleep_stress", "JSON", "NULLABLE"),
    bigquery.SchemaField("sleep_levels", "JSON", "NULLABLE"),
    # Metadata fields
    bigquery.SchemaField("dp_inserted_at", "TIMESTAMP", "NULLABLE"),
    bigquery.SchemaField("source_file", "STRING", "NULLABLE"),
]

# Simplified Garmin Heart Rate Schema - Fixed for BigQuery compatibility
garmin_heart_rate_schema = [
    # Basic identifiers
    bigquery.SchemaField("userProfilePK", "INTEGER", "NULLABLE"),
    bigquery.SchemaField("calendarDate", "STRING", "NULLABLE"),
    bigquery.SchemaField("date", "STRING", "NULLABLE"),
    # Timestamps
    bigquery.SchemaField("startTimestampGMT", "STRING", "NULLABLE"),
    bigquery.SchemaField("endTimestampGMT", "STRING", "NULLABLE"),
    bigquery.SchemaField("startTimestampLocal", "STRING", "NULLABLE"),
    bigquery.SchemaField("endTimestampLocal", "STRING", "NULLABLE"),
    # Summary heart rate metrics
    bigquery.SchemaField("maxHeartRate", "INTEGER", "NULLABLE"),
    bigquery.SchemaField("minHeartRate", "INTEGER", "NULLABLE"),
    bigquery.SchemaField("restingHeartRate", "INTEGER", "NULLABLE"),
    bigquery.SchemaField("lastSevenDaysAvgRestingHeartRate", "INTEGER", "NULLABLE"),
    # Store complex time-series data as JSON to avoid nested array issues
    bigquery.SchemaField("heartRateValueDescriptors", "JSON", "NULLABLE"),
    bigquery.SchemaField("heartRateValues", "JSON", "NULLABLE"),
    bigquery.SchemaField("heartRateValuesArray", "JSON", "NULLABLE"),
    # Additional fields that might be present
    bigquery.SchemaField("data", "JSON", "NULLABLE"),
    # Metadata fields
    bigquery.SchemaField("dp_inserted_at", "TIMESTAMP", "NULLABLE"),
    bigquery.SchemaField("source_file", "STRING", "NULLABLE"),
]

# Comprehensive Garmin Body Battery Schema - Based on Real Data Analysis
garmin_body_battery_schema = [
    # Date and period
    bigquery.SchemaField("date", "STRING", "NULLABLE"),
    bigquery.SchemaField("startTimestampGMT", "STRING", "NULLABLE"),
    bigquery.SchemaField("endTimestampGMT", "STRING", "NULLABLE"),
    bigquery.SchemaField("startTimestampLocal", "STRING", "NULLABLE"),
    bigquery.SchemaField("endTimestampLocal", "STRING", "NULLABLE"),
    # Daily body battery summary
    bigquery.SchemaField("charged", "INTEGER", "NULLABLE"),
    bigquery.SchemaField("drained", "INTEGER", "NULLABLE"),
    # Time-series data - convert nested arrays to RECORD arrays for BigQuery compatibility
    bigquery.SchemaField(
        "bodyBatteryValues",
        "RECORD",
        "REPEATED",
        None,
        None,
        (
            bigquery.SchemaField("timestamp", "INTEGER", "NULLABLE"),
            bigquery.SchemaField("level", "INTEGER", "NULLABLE"),
        ),
    ),
    # Original nested arrays stored as JSON for reference
    bigquery.SchemaField("bodyBatteryValuesArray", "JSON", "NULLABLE"),
    bigquery.SchemaField("bodyBatteryValueDescriptorDTOList", "JSON", "NULLABLE"),
    # Dynamic feedback events
    bigquery.SchemaField(
        "bodyBatteryDynamicFeedbackEvent",
        "RECORD",
        "NULLABLE",
        None,
        None,
        (
            bigquery.SchemaField("eventTimestampGmt", "STRING", "NULLABLE"),
            bigquery.SchemaField("bodyBatteryLevel", "STRING", "NULLABLE"),
            bigquery.SchemaField("feedbackShortType", "STRING", "NULLABLE"),
            bigquery.SchemaField("feedbackLongType", "STRING", "NULLABLE"),
        ),
    ),
    # End of day feedback event
    bigquery.SchemaField(
        "endOfDayBodyBatteryDynamicFeedbackEvent",
        "RECORD",
        "NULLABLE",
        None,
        None,
        (
            bigquery.SchemaField("eventTimestampGmt", "STRING", "NULLABLE"),
            bigquery.SchemaField("bodyBatteryLevel", "STRING", "NULLABLE"),
            bigquery.SchemaField("feedbackShortType", "STRING", "NULLABLE"),
            bigquery.SchemaField("feedbackLongType", "STRING", "NULLABLE"),
        ),
    ),
    # Activity events affecting body battery
    bigquery.SchemaField("bodyBatteryActivityEvent", "JSON", "NULLABLE"),
    # Metadata fields
    bigquery.SchemaField("dp_inserted_at", "TIMESTAMP", "NULLABLE"),
    bigquery.SchemaField("source_file", "STRING", "NULLABLE"),
]

# Simplified Garmin Stress Schema - Fixed for BigQuery compatibility
garmin_stress_schema = [
    # Basic identifiers
    bigquery.SchemaField("userProfilePK", "INTEGER", "NULLABLE"),
    bigquery.SchemaField("calendarDate", "STRING", "NULLABLE"),
    bigquery.SchemaField("date", "STRING", "NULLABLE"),
    # Timestamps
    bigquery.SchemaField("startTimestampGMT", "STRING", "NULLABLE"),
    bigquery.SchemaField("endTimestampGMT", "STRING", "NULLABLE"),
    bigquery.SchemaField("startTimestampLocal", "STRING", "NULLABLE"),
    bigquery.SchemaField("endTimestampLocal", "STRING", "NULLABLE"),
    # Stress summary metrics
    bigquery.SchemaField("maxStressLevel", "INTEGER", "NULLABLE"),
    bigquery.SchemaField("avgStressLevel", "INTEGER", "NULLABLE"),
    # Fields that appear in real data
    bigquery.SchemaField("stressChartValueOffset", "INTEGER", "NULLABLE"),
    bigquery.SchemaField("stressChartYAxisOrigin", "INTEGER", "NULLABLE"),
    # Store complex time-series data as JSON to avoid nested array issues
    bigquery.SchemaField("stressValueDescriptors", "JSON", "NULLABLE"),
    bigquery.SchemaField("stressValues", "JSON", "NULLABLE"),
    bigquery.SchemaField("stressValuesArray", "JSON", "NULLABLE"),
    bigquery.SchemaField("stressValueDescriptorsDTOList", "JSON", "NULLABLE"),
    bigquery.SchemaField("bodyBatteryValueDescriptorsDTOList", "JSON", "NULLABLE"),
    bigquery.SchemaField("bodyBatteryValuesArray", "JSON", "NULLABLE"),
    # Additional flexible data storage
    bigquery.SchemaField("data", "JSON", "NULLABLE"),
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

# Comprehensive HRV (Heart Rate Variability) Schema - Based on Real Data Analysis
garmin_hrv_schema = [
    # Core identifiers
    bigquery.SchemaField("userProfilePk", "INTEGER", "NULLABLE"),
    bigquery.SchemaField("date", "STRING", "NULLABLE"),
    # Sleep period timestamps
    bigquery.SchemaField("startTimestampGMT", "STRING", "NULLABLE"),
    bigquery.SchemaField("endTimestampGMT", "STRING", "NULLABLE"),
    bigquery.SchemaField("startTimestampLocal", "STRING", "NULLABLE"),
    bigquery.SchemaField("endTimestampLocal", "STRING", "NULLABLE"),
    bigquery.SchemaField("sleepStartTimestampGMT", "STRING", "NULLABLE"),
    bigquery.SchemaField("sleepEndTimestampGMT", "STRING", "NULLABLE"),
    bigquery.SchemaField("sleepStartTimestampLocal", "STRING", "NULLABLE"),
    bigquery.SchemaField("sleepEndTimestampLocal", "STRING", "NULLABLE"),
    # HRV summary metrics
    bigquery.SchemaField(
        "hrvSummary",
        "RECORD",
        "NULLABLE",
        None,
        None,
        (
            bigquery.SchemaField("calendarDate", "STRING", "NULLABLE"),
            bigquery.SchemaField("weeklyAvg", "INTEGER", "NULLABLE"),
            bigquery.SchemaField("lastNightAvg", "INTEGER", "NULLABLE"),
            bigquery.SchemaField("lastNight5MinHigh", "INTEGER", "NULLABLE"),
            bigquery.SchemaField(
                "baseline",
                "RECORD",
                "NULLABLE",
                None,
                None,
                (
                    bigquery.SchemaField("lowUpper", "INTEGER", "NULLABLE"),
                    bigquery.SchemaField("balancedLow", "INTEGER", "NULLABLE"),
                    bigquery.SchemaField("balancedUpper", "INTEGER", "NULLABLE"),
                    bigquery.SchemaField("markerValue", "FLOAT", "NULLABLE"),
                ),
            ),
            bigquery.SchemaField("status", "STRING", "NULLABLE"),
            bigquery.SchemaField("feedbackPhrase", "STRING", "NULLABLE"),
            bigquery.SchemaField("createTimeStamp", "STRING", "NULLABLE"),
        ),
    ),
    # Individual HRV readings (time-series data)
    bigquery.SchemaField(
        "hrvReadings",
        "RECORD",
        "REPEATED",
        None,
        None,
        (
            bigquery.SchemaField("hrvValue", "INTEGER", "NULLABLE"),
            bigquery.SchemaField("readingTimeGMT", "STRING", "NULLABLE"),
            bigquery.SchemaField("readingTimeLocal", "STRING", "NULLABLE"),
        ),
    ),
    # Metadata fields
    bigquery.SchemaField("dp_inserted_at", "TIMESTAMP", "NULLABLE"),
    bigquery.SchemaField("source_file", "STRING", "NULLABLE"),
]

# Steps Schema - 15-minute interval data
garmin_steps_schema = [
    bigquery.SchemaField("startGMT", "STRING", "NULLABLE"),
    bigquery.SchemaField("endGMT", "STRING", "NULLABLE"),
    bigquery.SchemaField("steps", "INTEGER", "NULLABLE"),
    bigquery.SchemaField("pushes", "INTEGER", "NULLABLE"),
    bigquery.SchemaField("primaryActivityLevel", "STRING", "NULLABLE"),
    bigquery.SchemaField("activityLevelConstant", "BOOLEAN", "NULLABLE"),
    bigquery.SchemaField("date", "STRING", "NULLABLE"),
    # Metadata fields
    bigquery.SchemaField("dp_inserted_at", "TIMESTAMP", "NULLABLE"),
    bigquery.SchemaField("source_file", "STRING", "NULLABLE"),
]

# Floors Schema - Fixed for BigQuery nested array compatibility
garmin_floors_schema = [
    bigquery.SchemaField("startTimestampGMT", "STRING", "NULLABLE"),
    bigquery.SchemaField("endTimestampGMT", "STRING", "NULLABLE"),
    bigquery.SchemaField("startTimestampLocal", "STRING", "NULLABLE"),
    bigquery.SchemaField("endTimestampLocal", "STRING", "NULLABLE"),
    bigquery.SchemaField("date", "STRING", "NULLABLE"),
    # Convert nested array to RECORD array for BigQuery compatibility
    bigquery.SchemaField(
        "floorValues",
        "RECORD",
        "REPEATED",
        None,
        None,
        (
            bigquery.SchemaField("startTimeGMT", "STRING", "NULLABLE"),
            bigquery.SchemaField("endTimeGMT", "STRING", "NULLABLE"),
            bigquery.SchemaField("floorsAscended", "INTEGER", "NULLABLE"),
            bigquery.SchemaField("floorsDescended", "INTEGER", "NULLABLE"),
        ),
    ),
    # Original nested arrays stored as JSON for reference
    bigquery.SchemaField("floorsValueDescriptorDTOList", "JSON", "NULLABLE"),
    bigquery.SchemaField("floorValuesArray", "JSON", "NULLABLE"),
    # Metadata fields
    bigquery.SchemaField("dp_inserted_at", "TIMESTAMP", "NULLABLE"),
    bigquery.SchemaField("source_file", "STRING", "NULLABLE"),
]

# Race Predictions Schema - Updated to match real data
garmin_race_predictions_schema = [
    bigquery.SchemaField("userId", "INTEGER", "NULLABLE"),
    bigquery.SchemaField("fromCalendarDate", "STRING", "NULLABLE"),
    bigquery.SchemaField("toCalendarDate", "STRING", "NULLABLE"),
    bigquery.SchemaField("calendarDate", "STRING", "NULLABLE"),
    bigquery.SchemaField("time5K", "INTEGER", "NULLABLE"),  # seconds
    bigquery.SchemaField("time10K", "INTEGER", "NULLABLE"),  # seconds
    bigquery.SchemaField("timeHalfMarathon", "INTEGER", "NULLABLE"),  # seconds
    bigquery.SchemaField("timeMarathon", "INTEGER", "NULLABLE"),  # seconds
    # Metadata fields
    bigquery.SchemaField("dp_inserted_at", "TIMESTAMP", "NULLABLE"),
    bigquery.SchemaField("source_file", "STRING", "NULLABLE"),
]

# Flexible Garmin Training Status Schema - Uses JSON for complex structures
garmin_training_status_schema = [
    # Store the entire record as JSON to handle all possible fields
    bigquery.SchemaField("raw_data", "JSON", "NULLABLE"),
    # Extract commonly used fields for easy querying
    bigquery.SchemaField(
        "user_profile_pk", "INTEGER", "NULLABLE"
    ),  # Extracted from userProfilePK or userId
    bigquery.SchemaField(
        "calendar_date", "STRING", "NULLABLE"
    ),  # Extracted from calendarDate or date
    bigquery.SchemaField(
        "training_status", "STRING", "NULLABLE"
    ),  # Extracted from trainingStatus
    bigquery.SchemaField(
        "training_load", "FLOAT", "NULLABLE"
    ),  # Extracted from trainingLoad
    bigquery.SchemaField(
        "fitness_level", "STRING", "NULLABLE"
    ),  # Extracted from fitnessLevel
    # Complex training metrics stored as JSON for maximum flexibility
    bigquery.SchemaField("vo2_max_data", "JSON", "NULLABLE"),  # All VO2Max related data
    bigquery.SchemaField(
        "training_load_balance", "JSON", "NULLABLE"
    ),  # Training load balance metrics
    bigquery.SchemaField(
        "training_status_details", "JSON", "NULLABLE"
    ),  # Detailed training status
    bigquery.SchemaField(
        "heat_altitude_acclimation", "JSON", "NULLABLE"
    ),  # Environmental adaptation data
    bigquery.SchemaField(
        "additional_metrics", "JSON", "NULLABLE"
    ),  # Any other training metrics
    # Metadata fields
    bigquery.SchemaField("dp_inserted_at", "TIMESTAMP", "NULLABLE"),
    bigquery.SchemaField("source_file", "STRING", "NULLABLE"),
]

# Weight Schema
garmin_weight_schema = [
    bigquery.SchemaField("date", "STRING", "NULLABLE"),
    bigquery.SchemaField("weight", "FLOAT", "NULLABLE"),
    bigquery.SchemaField("bmi", "FLOAT", "NULLABLE"),
    bigquery.SchemaField("bodyFatPercentage", "FLOAT", "NULLABLE"),
    bigquery.SchemaField("bodyMassIndex", "FLOAT", "NULLABLE"),
    bigquery.SchemaField("bodyWaterPercentage", "FLOAT", "NULLABLE"),
    bigquery.SchemaField("boneMass", "FLOAT", "NULLABLE"),
    bigquery.SchemaField("muscleMass", "FLOAT", "NULLABLE"),
    bigquery.SchemaField("timestampGMT", "STRING", "NULLABLE"),
    bigquery.SchemaField("timestampLocal", "STRING", "NULLABLE"),
    # Metadata fields
    bigquery.SchemaField("dp_inserted_at", "TIMESTAMP", "NULLABLE"),
    bigquery.SchemaField("source_file", "STRING", "NULLABLE"),
]

# Device Info Schema - Comprehensive device capabilities
garmin_device_info_schema = [
    # Core device identifiers
    bigquery.SchemaField("deviceId", "INTEGER", "NULLABLE"),
    bigquery.SchemaField("unitId", "INTEGER", "NULLABLE"),
    bigquery.SchemaField("displayName", "STRING", "NULLABLE"),
    bigquery.SchemaField("productDisplayName", "STRING", "NULLABLE"),
    bigquery.SchemaField("serialNumber", "STRING", "NULLABLE"),
    bigquery.SchemaField("partNumber", "STRING", "NULLABLE"),
    bigquery.SchemaField("productSku", "STRING", "NULLABLE"),
    bigquery.SchemaField("actualProductSku", "STRING", "NULLABLE"),
    # Device metadata
    bigquery.SchemaField("applicationKey", "STRING", "NULLABLE"),
    bigquery.SchemaField("deviceTypePk", "INTEGER", "NULLABLE"),
    bigquery.SchemaField("imageUrl", "STRING", "NULLABLE"),
    bigquery.SchemaField("deviceCategories", "STRING", "REPEATED"),
    # Firmware and software
    bigquery.SchemaField("currentFirmwareVersionMajor", "INTEGER", "NULLABLE"),
    bigquery.SchemaField("currentFirmwareVersionMinor", "INTEGER", "NULLABLE"),
    bigquery.SchemaField("currentFirmwareVersion", "STRING", "NULLABLE"),
    # Device capabilities (boolean flags)
    bigquery.SchemaField("appSupport", "BOOLEAN", "NULLABLE"),
    bigquery.SchemaField("bluetoothClassicDevice", "BOOLEAN", "NULLABLE"),
    bigquery.SchemaField("bluetoothLowEnergyDevice", "BOOLEAN", "NULLABLE"),
    bigquery.SchemaField("hasOpticalHeartRate", "BOOLEAN", "NULLABLE"),
    bigquery.SchemaField("wifi", "BOOLEAN", "NULLABLE"),
    bigquery.SchemaField("gpsCapable", "BOOLEAN", "NULLABLE"),
    bigquery.SchemaField("activitySummFitFileCapable", "BOOLEAN", "NULLABLE"),
    bigquery.SchemaField("aerobicTrainingEffectCapable", "BOOLEAN", "NULLABLE"),
    bigquery.SchemaField("bodyBatteryCapable", "BOOLEAN", "NULLABLE"),
    bigquery.SchemaField("hrvStressCapable", "BOOLEAN", "NULLABLE"),
    bigquery.SchemaField("sleepCapable", "BOOLEAN", "NULLABLE"),
    bigquery.SchemaField("pulseOxCapable", "BOOLEAN", "NULLABLE"),
    bigquery.SchemaField("vo2MaxCapable", "BOOLEAN", "NULLABLE"),
    # Device status
    bigquery.SchemaField("deviceStatus", "STRING", "NULLABLE"),
    bigquery.SchemaField("registeredDate", "INTEGER", "NULLABLE"),
    bigquery.SchemaField("activeInd", "INTEGER", "NULLABLE"),
    bigquery.SchemaField("primaryActivityTrackerIndicator", "BOOLEAN", "NULLABLE"),
    bigquery.SchemaField("isPrimaryUser", "BOOLEAN", "NULLABLE"),
    # Store full capability data as JSON for complete feature set
    bigquery.SchemaField("capabilities", "JSON", "NULLABLE"),
    # Metadata fields
    bigquery.SchemaField("dp_inserted_at", "TIMESTAMP", "NULLABLE"),
    bigquery.SchemaField("source_file", "STRING", "NULLABLE"),
]


def detect_file_type(filename: str) -> str:
    """Detect the type of Garmin data file based on filename patterns.

    Supports all 12 Garmin data types from the connector:
    activities, sleep, steps, heart_rate, body_battery, stress, weight,
    device_info, training_status, hrv, race_predictions, floors
    """
    filename_lower = filename.lower()
    if "activities" in filename_lower:
        return "activities"
    elif "sleep" in filename_lower:
        return "sleep"
    elif "steps" in filename_lower:
        return "steps"
    elif "heart_rate" in filename_lower or "heartrate" in filename_lower:
        return "heart_rate"
    elif "body_battery" in filename_lower:
        return "body_battery"
    elif "stress" in filename_lower:
        return "stress"
    elif "weight" in filename_lower:
        return "weight"
    elif "device_info" in filename_lower or "device" in filename_lower:
        return "device_info"
    elif "training_status" in filename_lower or "training" in filename_lower:
        return "training_status"
    elif "hrv" in filename_lower:
        return "hrv"
    elif "race_predictions" in filename_lower or "race_predictor" in filename_lower:
        return "race_predictions"
    elif "floors" in filename_lower:
        return "floors"
    else:
        return "activities"  # Default fallback


def get_schema_for_type(file_type: str):
    """Get the appropriate schema for a Garmin file type.

    Supports all 12 Garmin data types with comprehensive schemas.
    """
    schemas = {
        "activities": garmin_activities_schema,
        "sleep": garmin_sleep_schema,
        "steps": garmin_steps_schema,
        "heart_rate": garmin_heart_rate_schema,
        "body_battery": garmin_body_battery_schema,
        "stress": garmin_stress_schema,
        "weight": garmin_weight_schema,
        "device_info": garmin_device_info_schema,
        "training_status": garmin_training_status_schema,
        "hrv": garmin_hrv_schema,
        "race_predictions": garmin_race_predictions_schema,
        "floors": garmin_floors_schema,
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

                # Store complex objects as JSON
                if "activityType" in data:
                    data["activity_type_details"] = data["activityType"]
                if "eventType" in data:
                    data["event_type_details"] = data["eventType"]
                if "privacy" in data:
                    data["privacy_details"] = data["privacy"]
                if "summarizedDiveInfo" in data:
                    data["dive_info"] = data["summarizedDiveInfo"]
                if "splitSummaries" in data:
                    data["split_summaries"] = data["splitSummaries"]
                if "accessControlRuleList" in data:
                    data["access_control_rule_list"] = data["accessControlRuleList"]

            elif file_type == "sleep":
                # Extract key fields from dailySleepDTO for easy querying
                daily_sleep = data.get("dailySleepDTO", {})
                if daily_sleep:
                    data["sleep_date"] = daily_sleep.get("calendarDate")
                    data["user_profile_pk"] = daily_sleep.get("userProfilePK")
                    data["sleep_time_seconds"] = daily_sleep.get("sleepTimeSeconds")
                    data["deep_sleep_seconds"] = daily_sleep.get("deepSleepSeconds")
                    data["light_sleep_seconds"] = daily_sleep.get("lightSleepSeconds")
                    data["rem_sleep_seconds"] = daily_sleep.get("remSleepSeconds")
                    data["awake_sleep_seconds"] = daily_sleep.get("awakeSleepSeconds")
                    data["sleep_start_timestamp_gmt"] = daily_sleep.get(
                        "sleepStartTimestampGMT"
                    )
                    data["sleep_end_timestamp_gmt"] = daily_sleep.get(
                        "sleepEndTimestampGMT"
                    )
                    data["average_spo2_value"] = daily_sleep.get("averageSpO2Value")
                    data["average_respiration_value"] = daily_sleep.get(
                        "averageRespirationValue"
                    )
                    data["awake_count"] = daily_sleep.get("awakeCount")

                    # Extract overall sleep score
                    sleep_scores = daily_sleep.get("sleepScores", {})
                    if sleep_scores and "overall" in sleep_scores:
                        data["overall_sleep_score"] = sleep_scores["overall"].get(
                            "value"
                        )
                    data["sleep_scores"] = sleep_scores

                # Store time-series data as JSON
                if "sleepMovement" in data:
                    data["sleep_movement"] = data["sleepMovement"]
                if "wellnessSpO2SleepSummaryDTO" in data:
                    data["wellness_spo2_sleep_summary"] = data[
                        "wellnessSpO2SleepSummaryDTO"
                    ]
                if "sleepStress" in data:
                    data["sleep_stress"] = data["sleepStress"]
                if "sleepLevels" in data:
                    data["sleep_levels"] = data["sleepLevels"]

            elif file_type == "heart_rate":
                # Convert heart rate data to JSON format to avoid nested array issues
                # Keep original data structure in JSON fields for compatibility
                pass  # Heart rate data will be stored as-is with JSON fields

            elif file_type == "body_battery":
                # Convert nested arrays to RECORD arrays for BigQuery compatibility
                if "bodyBatteryValuesArray" in data and isinstance(
                    data["bodyBatteryValuesArray"], list
                ):
                    # Convert [[timestamp, level], ...] to [{timestamp: x, level: y}, ...]
                    converted_values = []
                    for item in data["bodyBatteryValuesArray"]:
                        if isinstance(item, list) and len(item) >= 2:
                            converted_values.append(
                                {"timestamp": item[0], "level": item[1]}
                            )
                    data["bodyBatteryValues"] = converted_values

                # Ensure required fields exist for basic validation
                if "date" not in data:
                    continue  # Skip entries without date

            elif file_type == "stress":
                # Stress data will use JSON fields for complex structures
                # Basic validation for required fields
                if "date" not in data and "calendarDate" not in data:
                    continue  # Skip entries without date

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

            elif file_type == "steps":
                # Steps data is typically simple, just ensure required fields
                if "steps" not in data:
                    data["steps"] = 0
                if "date" not in data:
                    continue  # Skip entries without date

            elif file_type == "floors":
                # Convert nested arrays to RECORD arrays for BigQuery compatibility
                if "floorValuesArray" in data and isinstance(
                    data["floorValuesArray"], list
                ):
                    # Convert [[startTime, endTime, ascended, descended], ...] to RECORD array
                    converted_values = []
                    for item in data["floorValuesArray"]:
                        if isinstance(item, list) and len(item) >= 4:
                            converted_values.append(
                                {
                                    "startTimeGMT": item[0],
                                    "endTimeGMT": item[1],
                                    "floorsAscended": item[2],
                                    "floorsDescended": item[3],
                                }
                            )
                    data["floorValues"] = converted_values

                # Ensure required fields exist
                if "date" not in data:
                    continue  # Skip entries without date

            elif file_type == "race_predictions":
                # Race predictions are typically simple structured data
                if "userId" not in data:
                    continue  # Skip entries without user ID

            elif file_type == "training_status":
                # Store entire record as raw data
                data["raw_data"] = data.copy()

                # Extract commonly used fields for easy querying
                data["user_profile_pk"] = (
                    data.get("userProfilePK")
                    or data.get("userProfilePk")
                    or data.get("userId")
                )
                data["calendar_date"] = data.get("calendarDate") or data.get("date")
                data["training_status"] = data.get("trainingStatus")
                data["training_load"] = data.get("trainingLoad")
                data["fitness_level"] = data.get("fitnessLevel")

                # Store complex metrics as JSON
                if "mostRecentVO2Max" in data:
                    data["vo2_max_data"] = data["mostRecentVO2Max"]
                if "mostRecentTrainingLoadBalance" in data:
                    data["training_load_balance"] = data[
                        "mostRecentTrainingLoadBalance"
                    ]
                if "mostRecentTrainingStatus" in data:
                    data["training_status_details"] = data["mostRecentTrainingStatus"]
                if "heatAltitudeAcclimationDTO" in data:
                    data["heat_altitude_acclimation"] = data[
                        "heatAltitudeAcclimationDTO"
                    ]

                # Store any remaining complex data
                additional_metrics = {}
                for key, value in data.items():
                    if isinstance(value, (dict, list)) and key not in [
                        "raw_data",
                        "vo2_max_data",
                        "training_load_balance",
                        "training_status_details",
                        "heat_altitude_acclimation",
                        "dp_inserted_at",
                        "source_file",
                    ]:
                        additional_metrics[key] = value
                if additional_metrics:
                    data["additional_metrics"] = additional_metrics

                # Ensure at least one identifier exists
                if not data["user_profile_pk"] or not data["calendar_date"]:
                    continue  # Skip entries without required identifiers

            elif file_type == "weight":
                # Weight data validation
                if "weight" not in data and "date" not in data:
                    continue  # Skip invalid weight entries

            elif file_type == "device_info":
                # Device info is comprehensive, store full capabilities as JSON
                if "deviceId" not in data:
                    continue  # Skip entries without device ID
                # Store full device data as capabilities for complex nested structure
                data["capabilities"] = data.copy()

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
    args = parser.parse_args()

    # Get project ID from environment variable
    project_id = os.getenv("GCP_PROJECT_ID")
    if not project_id:
        raise ValueError("GCP_PROJECT_ID environment variable is required")

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
            table_id = f"{project_id}.{dataset}.staging_garmin_{file_type}"

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
