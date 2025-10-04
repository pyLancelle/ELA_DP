{{ config(
    materialized='incremental',
    unique_key='sleep_id',
    partition_by={'field': 'sleep_date', 'data_type': 'date'},
    cluster_by=['sleep_id'],
    dataset=get_schema('hub'),
    tags=["hub", "garmin"]
) }}

-- Hub model for Garmin sleep time series data
-- Contains parsed and structured temporal data arrays for advanced sleep analysis
-- Use this model for minute-by-minute sleep tracking and detailed metrics

SELECT
    -- Core identifiers for joining with main sleep model
    CAST(JSON_VALUE(raw_data, '$.dailySleepDTO.id') AS INT64) as sleep_id,
    CAST(JSON_VALUE(raw_data, '$.dailySleepDTO.userProfilePK') AS INT64) as user_profile_pk,
    DATE(JSON_VALUE(raw_data, '$.dailySleepDTO.calendarDate')) as sleep_date,

    -- Movement and activity patterns (parsed)

    -- Sleep movement timeseries (minute-by-minute activity levels)
    ARRAY(
        SELECT AS STRUCT
            FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', TIMESTAMP(JSON_VALUE(TO_JSON_STRING(value), '$.startGMT'))) as start_time,
            FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', TIMESTAMP(JSON_VALUE(TO_JSON_STRING(value), '$.endGMT'))) as end_time,
            CAST(JSON_VALUE(TO_JSON_STRING(value), '$.activityLevel') AS FLOAT64) as activity_level
        FROM UNNEST(JSON_QUERY_ARRAY(JSON_QUERY(raw_data, '$.sleepMovement'))) AS value
        WHERE JSON_VALUE(TO_JSON_STRING(value), '$.activityLevel') IS NOT NULL
        ORDER BY JSON_VALUE(TO_JSON_STRING(value), '$.startGMT')
    ) as sleep_movement_timeseries,

    -- Sleep levels timeseries (sleep phases: 0=awake, 1=light, 2=deep, 3=REM)
    ARRAY(
        SELECT AS STRUCT
            FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', TIMESTAMP(JSON_VALUE(TO_JSON_STRING(value), '$.startGMT'))) as start_time,
            FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', TIMESTAMP(JSON_VALUE(TO_JSON_STRING(value), '$.endGMT'))) as end_time,
            CAST(JSON_VALUE(TO_JSON_STRING(value), '$.activityLevel') AS INT64) as sleep_level
        FROM UNNEST(JSON_QUERY_ARRAY(JSON_QUERY(raw_data, '$.sleepLevels'))) AS value
        WHERE JSON_VALUE(TO_JSON_STRING(value), '$.activityLevel') IS NOT NULL
        ORDER BY JSON_VALUE(TO_JSON_STRING(value), '$.startGMT')
    ) as sleep_levels_timeseries,

    -- Restless moments timeseries
    ARRAY(
        SELECT AS STRUCT
            FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', TIMESTAMP_MILLIS(CAST(JSON_VALUE(TO_JSON_STRING(value), '$.startGMT') AS INT64))) as time,
            CAST(JSON_VALUE(TO_JSON_STRING(value), '$.value') AS INT64) as value
        FROM UNNEST(JSON_QUERY_ARRAY(JSON_QUERY(raw_data, '$.sleepRestlessMoments'))) AS value
        WHERE JSON_VALUE(TO_JSON_STRING(value), '$.value') IS NOT NULL
        ORDER BY CAST(JSON_VALUE(TO_JSON_STRING(value), '$.startGMT') AS INT64)
    ) as restless_moments_timeseries,

    -- Wellness and health monitoring (parsed)

    -- SpO2 summary (parsed as STRUCT)
    STRUCT(
        CAST(JSON_VALUE(raw_data, '$.wellnessSpO2SleepSummaryDTO.alertThresholdValue') AS INT64) as alert_threshold_value,
        CAST(JSON_VALUE(raw_data, '$.wellnessSpO2SleepSummaryDTO.averageSPO2') AS INT64) as average_spo2,
        CAST(JSON_VALUE(raw_data, '$.wellnessSpO2SleepSummaryDTO.averageSpO2HR') AS INT64) as average_spo2_hr,
        CAST(JSON_VALUE(raw_data, '$.wellnessSpO2SleepSummaryDTO.deviceId') AS INT64) as device_id,
        CAST(JSON_VALUE(raw_data, '$.wellnessSpO2SleepSummaryDTO.durationOfEventsBelowThreshold') AS INT64) as duration_events_below_threshold,
        CAST(JSON_VALUE(raw_data, '$.wellnessSpO2SleepSummaryDTO.lowestSPO2') AS INT64) as lowest_spo2,
        CAST(JSON_VALUE(raw_data, '$.wellnessSpO2SleepSummaryDTO.numberOfEventsBelowThreshold') AS INT64) as number_events_below_threshold,
        FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', TIMESTAMP(JSON_VALUE(raw_data, '$.wellnessSpO2SleepSummaryDTO.sleepMeasurementEndGMT'))) as sleep_measurement_end,
        FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', TIMESTAMP(JSON_VALUE(raw_data, '$.wellnessSpO2SleepSummaryDTO.sleepMeasurementStartGMT'))) as sleep_measurement_start,
        CAST(JSON_VALUE(raw_data, '$.wellnessSpO2SleepSummaryDTO.userProfilePk') AS INT64) as user_profile_pk
    ) as spo2_summary,

    -- Respiration timeseries (minute-by-minute breathing rate)
    ARRAY(
        SELECT AS STRUCT
            FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', TIMESTAMP_MILLIS(CAST(JSON_VALUE(TO_JSON_STRING(value), '$.startTimeGMT') AS INT64))) as time,
            CAST(JSON_VALUE(TO_JSON_STRING(value), '$.respirationValue') AS INT64) as respiration_value
        FROM UNNEST(JSON_QUERY_ARRAY(JSON_QUERY(raw_data, '$.wellnessEpochRespirationDataDTOList'))) AS value
        WHERE JSON_VALUE(TO_JSON_STRING(value), '$.respirationValue') IS NOT NULL
        ORDER BY CAST(JSON_VALUE(TO_JSON_STRING(value), '$.startTimeGMT') AS INT64)
    ) as respiration_timeseries,

    -- Respiration averages by epoch
    ARRAY(
        SELECT AS STRUCT
            FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', TIMESTAMP_MILLIS(CAST(JSON_VALUE(TO_JSON_STRING(value), '$.epochEndTimestampGmt') AS INT64))) as epoch_end_time,
            CAST(JSON_VALUE(TO_JSON_STRING(value), '$.respirationAverageValue') AS FLOAT64) as avg_value,
            CAST(JSON_VALUE(TO_JSON_STRING(value), '$.respirationHighValue') AS INT64) as high_value,
            CAST(JSON_VALUE(TO_JSON_STRING(value), '$.respirationLowValue') AS INT64) as low_value
        FROM UNNEST(JSON_QUERY_ARRAY(JSON_QUERY(raw_data, '$.wellnessEpochRespirationAveragesList'))) AS value
        WHERE JSON_VALUE(TO_JSON_STRING(value), '$.epochEndTimestampGmt') IS NOT NULL
        ORDER BY CAST(JSON_VALUE(TO_JSON_STRING(value), '$.epochEndTimestampGmt') AS INT64)
    ) as respiration_averages_timeseries,

    -- Parsed timeseries arrays (ready to use)

    -- Stress timeseries
    ARRAY(
        SELECT AS STRUCT
            FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', TIMESTAMP_MILLIS(CAST(JSON_VALUE(TO_JSON_STRING(value), '$.startGMT') AS INT64))) as time,
            CAST(JSON_VALUE(TO_JSON_STRING(value), '$.value') AS INT64) as value
        FROM UNNEST(JSON_QUERY_ARRAY(JSON_QUERY(raw_data, '$.sleepStress'))) AS value
        WHERE JSON_VALUE(TO_JSON_STRING(value), '$.value') IS NOT NULL
        ORDER BY CAST(JSON_VALUE(TO_JSON_STRING(value), '$.startGMT') AS INT64)
    ) as stress_timeseries,

    -- HRV timeseries
    ARRAY(
        SELECT AS STRUCT
            FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', TIMESTAMP_MILLIS(CAST(JSON_VALUE(TO_JSON_STRING(value), '$.startGMT') AS INT64))) as time,
            CAST(JSON_VALUE(TO_JSON_STRING(value), '$.value') AS INT64) as value
        FROM UNNEST(JSON_QUERY_ARRAY(JSON_QUERY(raw_data, '$.hrvData'))) AS value
        WHERE JSON_VALUE(TO_JSON_STRING(value), '$.value') IS NOT NULL
        ORDER BY CAST(JSON_VALUE(TO_JSON_STRING(value), '$.startGMT') AS INT64)
    ) as hrv_timeseries,

    -- Body Battery timeseries
    ARRAY(
        SELECT AS STRUCT
            FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', TIMESTAMP_MILLIS(CAST(JSON_VALUE(TO_JSON_STRING(value), '$.startGMT') AS INT64))) as time,
            CAST(JSON_VALUE(TO_JSON_STRING(value), '$.value') AS INT64) as value
        FROM UNNEST(JSON_QUERY_ARRAY(JSON_QUERY(raw_data, '$.sleepBodyBattery'))) AS value
        WHERE JSON_VALUE(TO_JSON_STRING(value), '$.value') IS NOT NULL
        ORDER BY CAST(JSON_VALUE(TO_JSON_STRING(value), '$.startGMT') AS INT64)
    ) as body_battery_timeseries,

    -- Heart Rate timeseries
    ARRAY(
        SELECT AS STRUCT
            FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', TIMESTAMP_MILLIS(CAST(JSON_VALUE(TO_JSON_STRING(value), '$.startGMT') AS INT64))) as time,
            CAST(JSON_VALUE(TO_JSON_STRING(value), '$.value') AS INT64) as value
        FROM UNNEST(JSON_QUERY_ARRAY(JSON_QUERY(raw_data, '$.sleepHeartRate'))) AS value
        WHERE JSON_VALUE(TO_JSON_STRING(value), '$.value') IS NOT NULL
        ORDER BY CAST(JSON_VALUE(TO_JSON_STRING(value), '$.startGMT') AS INT64)
    ) as heart_rate_timeseries,

    -- SpO2 timeseries
    ARRAY(
        SELECT AS STRUCT
            JSON_VALUE(TO_JSON_STRING(value), '$.epochTimestamp') as time,
            CAST(JSON_VALUE(TO_JSON_STRING(value), '$.spo2Reading') AS INT64) as value
        FROM UNNEST(JSON_QUERY_ARRAY(JSON_QUERY(raw_data, '$.wellnessEpochSPO2DataDTOList'))) AS value
        WHERE JSON_VALUE(TO_JSON_STRING(value), '$.spo2Reading') IS NOT NULL
        ORDER BY JSON_VALUE(TO_JSON_STRING(value), '$.epochTimestamp')
    ) as spo2_timeseries,

    -- Breathing disruption timeseries (periods with start/end times)
    ARRAY(
        SELECT AS STRUCT
            FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', TIMESTAMP_MILLIS(CAST(JSON_VALUE(TO_JSON_STRING(value), '$.startGMT') AS INT64))) as start_time,
            FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', TIMESTAMP_MILLIS(CAST(JSON_VALUE(TO_JSON_STRING(value), '$.endGMT') AS INT64))) as end_time,
            CAST(JSON_VALUE(TO_JSON_STRING(value), '$.value') AS INT64) as value
        FROM UNNEST(JSON_QUERY_ARRAY(JSON_QUERY(raw_data, '$.breathingDisruptionData'))) AS value
        WHERE JSON_VALUE(TO_JSON_STRING(value), '$.value') IS NOT NULL
        ORDER BY CAST(JSON_VALUE(TO_JSON_STRING(value), '$.startGMT') AS INT64)
    ) as breathing_disruption_timeseries,

    -- Metadata
    dp_inserted_at,
    source_file

FROM {{ ref('lake_garmin__svc_sleep') }}

{% if is_incremental() %}
WHERE dp_inserted_at > (SELECT MAX(dp_inserted_at) FROM {{ this }})
{% endif %}