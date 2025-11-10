/*
In this table, we want one row per running activity.

Columns:
- Basic info: date, activity_id, activity_name, start_time, duration, distance, avg_pace
- Heart rate (nested): avg_bpm, max_bpm, time in zones
- Running dynamics (nested): cadence, stride length, ground contact time, vertical oscillation, steps
- Elevation (nested): gain, loss, min, max
- Training metrics (nested): aerobic/anaerobic effect, training load, VO2max, body battery delta
- Splits (array): detailed metrics for each split (distance, duration, pace, HR, etc.)
*/

{{
  config(
      tags=['health', 'hub'],
      materialized='view'
  )
}}

SELECT
    -- Basic information
    DATE(activities.startTimeLocal) AS date,
    activities.activityId AS activity_id,
    activities.activityName AS activity_name,
    activities.activityType.typeKey AS activity_type,
    activities.startTimeLocal AS start_time,

    -- Performance metrics
    ROUND(activities.distance / 1000, 2) AS distance_km,
    activities.duration AS duration_seconds,
    activities.movingDuration AS moving_duration_seconds,

    -- Calculate average pace (min/km)
    CASE
        WHEN activities.averageSpeed > 0 THEN
            ROUND((1000.0 / activities.averageSpeed) / 60, 2)
        ELSE NULL
    END AS avg_pace_min_per_km,

    activities.calories,

    -- Heart Rate (nested)
    STRUCT(
        activities.averageHR AS avg_bpm,
        activities.maxHR AS max_bpm,
        activities.hrTimeInZone_1 AS zone1_seconds,
        activities.hrTimeInZone_2 AS zone2_seconds,
        activities.hrTimeInZone_3 AS zone3_seconds,
        activities.hrTimeInZone_4 AS zone4_seconds,
        activities.hrTimeInZone_5 AS zone5_seconds
    ) AS heart_rate,

    -- Running Dynamics (nested)
    STRUCT(
        activities.averageRunningCadenceInStepsPerMinute AS avg_cadence_spm,
        activities.maxRunningCadenceInStepsPerMinute AS max_cadence_spm,
        activities.avgStrideLength AS avg_stride_length_cm,
        activities.avgGroundContactTime AS avg_ground_contact_time_ms,
        activities.avgVerticalOscillation AS avg_vertical_oscillation_cm,
        activities.avgVerticalRatio AS avg_vertical_ratio_pct,
        activities.steps AS total_steps
    ) AS running_dynamics,

    -- Elevation (nested)
    STRUCT(
        activities.elevationGain AS gain_meters,
        activities.elevationLoss AS loss_meters,
        activities.minElevation AS min_elevation,
        activities.maxElevation AS max_elevation
    ) AS elevation,

    -- Training Metrics (nested)
    STRUCT(
        activities.aerobicTrainingEffect AS aerobic_effect,
        activities.anaerobicTrainingEffect AS anaerobic_effect,
        activities.activityTrainingLoad AS training_load,
        activities.vO2MaxValue AS vo2max,
        activities.differencebodybattery AS body_battery_delta,
        activities.moderateIntensityMinutes AS moderate_intensity_minutes,
        activities.vigorousIntensityMinutes AS vigorous_intensity_minutes
    ) AS training_metrics,

    -- Power metrics (nested) - for running with power meter
    STRUCT(
        activities.avgPower AS avg_watts,
        activities.maxPower AS max_watts,
        activities.normPower AS normalized_power
    ) AS power,

    -- Personal records (nested)
    STRUCT(
        activities.fastestSplit_1000 AS best_1km_seconds,
        activities.fastestSplit_5000 AS best_5km_seconds,
        activities.fastestSplit_10000 AS best_10km_seconds,
        activities.fastestSplit_21098 AS best_half_marathon_seconds,
        activities.fastestSplit_42195 AS best_marathon_seconds
    ) AS personal_records,

    -- Splits (array of STRUCT)
    splits.splitSummaries AS splits

FROM
    {{ ref('lake_garmin__svc_activities') }} AS activities
LEFT JOIN
    {{ ref('lake_garmin__svc_activity_splits') }} AS splits
    ON activities.activityId = splits.activityId
WHERE
    activities.activityType.typeKey IN ('running', 'trail_running', 'track_running', 'treadmill_running')
    AND activities.startTimeLocal IS NOT NULL
ORDER BY
    activities.startTimeLocal DESC
