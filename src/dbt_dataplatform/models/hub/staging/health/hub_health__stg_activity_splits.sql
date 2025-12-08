/*
In this table, we want one row per split/lap for running activities.
This allows detailed analysis of pacing, heart rate, and performance per kilometer/mile.

Columns:
- Activity info: activity_id, activity_name, date, activity_type
- Split info: split_number, split_type (INTERVAL_LAP, INTERVAL_ACTIVE, etc.)
- Split metrics: distance, duration, pace, heart rate, cadence, elevation, etc.
*/

{{
  config(
      tags=['health', 'hub'],
      materialized='view'
  )
}}

WITH activity_splits AS (
    SELECT
        splits.activityId,
        splits.activityName,
        splits.activityType,
        splits.startTimeLocal,
        typed_splits.splits AS splits_array
    FROM {{ ref('lake_garmin__svc_activity_splits') }} AS splits
    WHERE splits.activityType.typeKey IN ('running', 'trail_running', 'track_running', 'treadmill_running')
)

SELECT
    -- Activity identifiers
    activity_splits.activityId AS activity_id,
    activity_splits.activityName AS activity_name,
    DATE(activity_splits.startTimeLocal) AS date,
    activity_splits.activityType.typeKey AS activity_type,
    activity_splits.startTimeLocal AS activity_start_time,

    -- Split identifiers and metadata
    ROW_NUMBER() OVER (PARTITION BY activity_splits.activityId ORDER BY laps.startTimeLocal) AS split_number,
    laps.type AS split_type,
    laps.startTimeLocal AS split_start_time,
    laps.duration AS split_duration_seconds,
    laps.distance AS split_distance_meters,
    ROUND(laps.distance / 1000, 3) AS split_distance_km,

    -- Pace calculations
    CASE
        WHEN laps.distance > 0 AND laps.duration > 0 THEN
            ROUND((laps.duration / (laps.distance / 1000)) / 60, 2)
        ELSE NULL
    END AS split_pace_min_per_km,

    CASE
        WHEN laps.distance > 0 AND laps.duration > 0 THEN
            ROUND((laps.distance / 1000) / (laps.duration / 3600), 2)
        ELSE NULL
    END AS split_speed_kmh,

    -- Heart rate metrics (nested)
    STRUCT(
        laps.averageHR AS avg_bpm,
        laps.maxHR AS max_bpm
    ) AS heart_rate,

    -- Elevation (nested)
    STRUCT(
        laps.elevationGain AS gain_meters,
        laps.elevationLoss AS loss_meters,
        laps.startElevation AS min_elevation
    ) AS elevation,

    -- Power metrics (nested)
    STRUCT(
        laps.averagePower AS avg_watts,
        laps.maxPower AS max_watts,
        laps.normalizedPower AS normalized_power
    ) AS power,

    -- Raw lap data (if needed for advanced analysis)
    laps AS lap_raw_data

FROM activity_splits
CROSS JOIN UNNEST(activity_splits.splits_array) AS laps
WHERE laps.type LIKE 'INTERVAL_%'  -- Filter for interval laps only
ORDER BY
    activity_splits.startTimeLocal DESC,
    split_number ASC
