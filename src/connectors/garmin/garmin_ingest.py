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
    bigquery.SchemaField("averageHR", "INTEGER", "NULLABLE"),
    bigquery.SchemaField("maxHR", "INTEGER", "NULLABLE"),
    # Running metrics
    bigquery.SchemaField("steps", "INTEGER", "NULLABLE"),
    bigquery.SchemaField("averageRunningCadenceInStepsPerMinute", "FLOAT", "NULLABLE"),
    bigquery.SchemaField("maxRunningCadenceInStepsPerMinute", "FLOAT", "NULLABLE"),
    bigquery.SchemaField("maxDoubleCadence", "INTEGER", "NULLABLE"),
    # Advanced running metrics
    bigquery.SchemaField("avgVerticalOscillation", "FLOAT", "NULLABLE"),
    bigquery.SchemaField("avgGroundContactTime", "FLOAT", "NULLABLE"),
    bigquery.SchemaField("avgStrideLength", "FLOAT", "NULLABLE"),
    bigquery.SchemaField("avgVerticalRatio", "FLOAT", "NULLABLE"),
    # Power metrics
    bigquery.SchemaField("avgPower", "INTEGER", "NULLABLE"),
    bigquery.SchemaField("maxPower", "INTEGER", "NULLABLE"),
    bigquery.SchemaField("normPower", "INTEGER", "NULLABLE"),
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
    bigquery.SchemaField("splitSummaries", "JSON", "NULLABLE"),
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

# Comprehensive Garmin Sleep Schema - Based on Real Data Analysis
garmin_sleep_schema = [
    bigquery.SchemaField(
        "dailySleepDTO",
        "RECORD",
        "NULLABLE",
        None,
        None,
        (
            bigquery.SchemaField("id", "INTEGER", "NULLABLE"),
            bigquery.SchemaField("userProfilePK", "INTEGER", "NULLABLE"),
            bigquery.SchemaField("calendarDate", "STRING", "NULLABLE"),
            # Sleep duration metrics
            bigquery.SchemaField("sleepTimeSeconds", "INTEGER", "NULLABLE"),
            bigquery.SchemaField("napTimeSeconds", "INTEGER", "NULLABLE"),
            bigquery.SchemaField("unmeasurableSleepSeconds", "INTEGER", "NULLABLE"),
            bigquery.SchemaField("deepSleepSeconds", "INTEGER", "NULLABLE"),
            bigquery.SchemaField("lightSleepSeconds", "INTEGER", "NULLABLE"),
            bigquery.SchemaField("remSleepSeconds", "INTEGER", "NULLABLE"),
            bigquery.SchemaField("awakeSleepSeconds", "INTEGER", "NULLABLE"),
            # Sleep timestamps (milliseconds since epoch)
            bigquery.SchemaField("sleepStartTimestampGMT", "INTEGER", "NULLABLE"),
            bigquery.SchemaField("sleepEndTimestampGMT", "INTEGER", "NULLABLE"),
            bigquery.SchemaField("sleepStartTimestampLocal", "INTEGER", "NULLABLE"),
            bigquery.SchemaField("sleepEndTimestampLocal", "INTEGER", "NULLABLE"),
            bigquery.SchemaField("autoSleepStartTimestampGMT", "INTEGER", "NULLABLE"),
            bigquery.SchemaField("autoSleepEndTimestampGMT", "INTEGER", "NULLABLE"),
            # Sleep quality and validation
            bigquery.SchemaField("sleepWindowConfirmed", "BOOLEAN", "NULLABLE"),
            bigquery.SchemaField("sleepWindowConfirmationType", "STRING", "NULLABLE"),
            bigquery.SchemaField("sleepQualityTypePK", "INTEGER", "NULLABLE"),
            bigquery.SchemaField("sleepResultTypePK", "INTEGER", "NULLABLE"),
            bigquery.SchemaField("retro", "BOOLEAN", "NULLABLE"),
            bigquery.SchemaField("sleepFromDevice", "BOOLEAN", "NULLABLE"),
            bigquery.SchemaField("deviceRemCapable", "BOOLEAN", "NULLABLE"),
            # Blood oxygen (SpO2) metrics
            bigquery.SchemaField("averageSpO2Value", "FLOAT", "NULLABLE"),
            bigquery.SchemaField("lowestSpO2Value", "INTEGER", "NULLABLE"),
            bigquery.SchemaField("highestSpO2Value", "INTEGER", "NULLABLE"),
            bigquery.SchemaField("averageSpO2HRSleep", "FLOAT", "NULLABLE"),
            # Respiration metrics
            bigquery.SchemaField("averageRespirationValue", "FLOAT", "NULLABLE"),
            bigquery.SchemaField("lowestRespirationValue", "FLOAT", "NULLABLE"),
            bigquery.SchemaField("highestRespirationValue", "FLOAT", "NULLABLE"),
            # Sleep disruption
            bigquery.SchemaField("awakeCount", "INTEGER", "NULLABLE"),
            bigquery.SchemaField("avgSleepStress", "FLOAT", "NULLABLE"),
            # Sleep feedback and scoring
            bigquery.SchemaField("ageGroup", "STRING", "NULLABLE"),
            bigquery.SchemaField("sleepScoreFeedback", "STRING", "NULLABLE"),
            bigquery.SchemaField("sleepScoreInsight", "STRING", "NULLABLE"),
            bigquery.SchemaField("sleepScorePersonalizedInsight", "STRING", "NULLABLE"),
            # Comprehensive sleep scores
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
                            bigquery.SchemaField("optimalStart", "FLOAT", "NULLABLE"),
                            bigquery.SchemaField("optimalEnd", "FLOAT", "NULLABLE"),
                        ),
                    ),
                    bigquery.SchemaField(
                        "totalDuration",
                        "RECORD",
                        "NULLABLE",
                        None,
                        None,
                        (
                            bigquery.SchemaField("value", "INTEGER", "NULLABLE"),
                            bigquery.SchemaField("qualifierKey", "STRING", "NULLABLE"),
                            bigquery.SchemaField("optimalStart", "FLOAT", "NULLABLE"),
                            bigquery.SchemaField("optimalEnd", "FLOAT", "NULLABLE"),
                        ),
                    ),
                    bigquery.SchemaField(
                        "stress",
                        "RECORD",
                        "NULLABLE",
                        None,
                        None,
                        (
                            bigquery.SchemaField("value", "INTEGER", "NULLABLE"),
                            bigquery.SchemaField("qualifierKey", "STRING", "NULLABLE"),
                            bigquery.SchemaField("optimalStart", "FLOAT", "NULLABLE"),
                            bigquery.SchemaField("optimalEnd", "FLOAT", "NULLABLE"),
                        ),
                    ),
                    bigquery.SchemaField(
                        "awakeCount",
                        "RECORD",
                        "NULLABLE",
                        None,
                        None,
                        (
                            bigquery.SchemaField("value", "INTEGER", "NULLABLE"),
                            bigquery.SchemaField("qualifierKey", "STRING", "NULLABLE"),
                            bigquery.SchemaField("optimalStart", "FLOAT", "NULLABLE"),
                            bigquery.SchemaField("optimalEnd", "FLOAT", "NULLABLE"),
                        ),
                    ),
                    bigquery.SchemaField(
                        "quality",
                        "RECORD",
                        "NULLABLE",
                        None,
                        None,
                        (
                            bigquery.SchemaField("value", "INTEGER", "NULLABLE"),
                            bigquery.SchemaField("qualifierKey", "STRING", "NULLABLE"),
                            bigquery.SchemaField("optimalStart", "FLOAT", "NULLABLE"),
                            bigquery.SchemaField("optimalEnd", "FLOAT", "NULLABLE"),
                        ),
                    ),
                    bigquery.SchemaField(
                        "recovery",
                        "RECORD",
                        "NULLABLE",
                        None,
                        None,
                        (
                            bigquery.SchemaField("value", "INTEGER", "NULLABLE"),
                            bigquery.SchemaField("qualifierKey", "STRING", "NULLABLE"),
                            bigquery.SchemaField("optimalStart", "FLOAT", "NULLABLE"),
                            bigquery.SchemaField("optimalEnd", "FLOAT", "NULLABLE"),
                        ),
                    ),
                    bigquery.SchemaField(
                        "restfulness",
                        "RECORD",
                        "NULLABLE",
                        None,
                        None,
                        (
                            bigquery.SchemaField("value", "INTEGER", "NULLABLE"),
                            bigquery.SchemaField("qualifierKey", "STRING", "NULLABLE"),
                            bigquery.SchemaField("optimalStart", "FLOAT", "NULLABLE"),
                            bigquery.SchemaField("optimalEnd", "FLOAT", "NULLABLE"),
                        ),
                    ),
                ),
            ),
        ),
    ),
    # Complex time-series data stored as JSON
    bigquery.SchemaField("sleepMovement", "JSON", "NULLABLE"),
    bigquery.SchemaField("wellnessSpO2SleepSummaryDTO", "JSON", "NULLABLE"),
    bigquery.SchemaField("sleepStress", "JSON", "NULLABLE"),
    bigquery.SchemaField("sleepLevels", "JSON", "NULLABLE"),
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
    # Time-series data (stored as JSON arrays)
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

# Floors Schema - Elevation/stairs climbed data
garmin_floors_schema = [
    bigquery.SchemaField("startTimestampGMT", "STRING", "NULLABLE"),
    bigquery.SchemaField("endTimestampGMT", "STRING", "NULLABLE"),
    bigquery.SchemaField("startTimestampLocal", "STRING", "NULLABLE"),
    bigquery.SchemaField("endTimestampLocal", "STRING", "NULLABLE"),
    bigquery.SchemaField("floorsValueDescriptorDTOList", "JSON", "NULLABLE"),
    bigquery.SchemaField("floorValuesArray", "JSON", "NULLABLE"),
    bigquery.SchemaField("date", "STRING", "NULLABLE"),
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

# Training Status Schema
garmin_training_status_schema = [
    bigquery.SchemaField("userProfilePk", "INTEGER", "NULLABLE"),
    bigquery.SchemaField("date", "STRING", "NULLABLE"),
    bigquery.SchemaField(
        "data", "JSON", "NULLABLE"
    ),  # Flexible structure for various training metrics
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

            elif file_type == "steps":
                # Steps data is typically simple, just ensure required fields
                if "steps" not in data:
                    data["steps"] = 0
                if "date" not in data:
                    continue  # Skip entries without date

            elif file_type == "floors":
                # Ensure floors data arrays are properly structured
                if "floorValuesArray" not in data:
                    data["floorValuesArray"] = []
                if "floorsValueDescriptorDTOList" not in data:
                    data["floorsValueDescriptorDTOList"] = []

            elif file_type == "race_predictions":
                # Race predictions are typically simple structured data
                if "userId" not in data:
                    continue  # Skip entries without user ID

            elif file_type == "training_status":
                # Training status can have various structures, wrap in data field if needed
                if "date" not in data:
                    continue  # Skip entries without date

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
