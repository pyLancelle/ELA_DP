{{ config(dataset=get_schema('lake')) }}

-- Pure sleep data extraction from staging_garmin_raw
-- Source: sleep data type from Garmin Connect API

SELECT
    -- Sleep session identifier
    SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.dailySleepDTO.id') AS INT64) AS sleep_session_id,
    DATE(SAFE.PARSE_TIMESTAMP('%Y-%m-%d', JSON_EXTRACT_SCALAR(raw_data, '$.dailySleepDTO.calendarDate'))) AS sleep_date,
    
    -- Sleep timing
    TIMESTAMP_MILLIS(SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.dailySleepDTO.sleepStartTimestampGMT') AS INT64)) AS sleep_start_gmt,
    TIMESTAMP_MILLIS(SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.dailySleepDTO.sleepEndTimestampGMT') AS INT64)) AS sleep_end_gmt,
    TIMESTAMP_MILLIS(SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.dailySleepDTO.sleepStartTimestampLocal') AS INT64)) AS sleep_start_local,
    TIMESTAMP_MILLIS(SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.dailySleepDTO.sleepEndTimestampLocal') AS INT64)) AS sleep_end_local,
    
    -- Sleep duration (in seconds as per source)
    SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.dailySleepDTO.sleepTimeSeconds') AS INT64) AS sleep_time_seconds,
    SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.dailySleepDTO.deepSleepSeconds') AS INT64) AS deep_sleep_seconds,
    SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.dailySleepDTO.lightSleepSeconds') AS INT64) AS light_sleep_seconds,
    SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.dailySleepDTO.remSleepSeconds') AS INT64) AS rem_sleep_seconds,
    SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.dailySleepDTO.awakeSleepSeconds') AS INT64) AS awake_sleep_seconds,
    SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.dailySleepDTO.napTimeSeconds') AS INT64) AS nap_time_seconds,
    
    -- Blood oxygen metrics
    SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.dailySleepDTO.averageSpO2Value') AS FLOAT64) AS average_spo2,
    SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.dailySleepDTO.lowestSpO2Value') AS FLOAT64) AS lowest_spo2,
    SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.dailySleepDTO.highestSpO2Value') AS FLOAT64) AS highest_spo2,
    
    -- Respiratory metrics
    SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.dailySleepDTO.averageRespirationValue') AS FLOAT64) AS average_respiration_rate,
    
    -- Sleep disruption metrics
    SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.dailySleepDTO.awakeCount') AS INT64) AS awake_count,
    SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.dailySleepDTO.avgSleepStress') AS FLOAT64) AS average_sleep_stress,
    
    -- Sleep quality scores (preserve structure as JSON for hub layer)
    JSON_EXTRACT(raw_data, '$.dailySleepDTO.sleepScores') AS sleep_scores_json,
    
    -- Complex nested data preserved for hub layer processing
    JSON_EXTRACT(raw_data, '$.sleepMovement') AS sleep_movement_json,
    JSON_EXTRACT(raw_data, '$.wellnessSpO2SleepSummaryDTO') AS spo2_sleep_summary_json,
    JSON_EXTRACT(raw_data, '$.sleepStress') AS sleep_stress_timeseries_json,
    
    -- Source metadata
    dp_inserted_at,
    source_file

FROM {{ source('garmin', 'staging_garmin_raw') }}
WHERE data_type = 'sleep'
  AND JSON_EXTRACT_SCALAR(raw_data, '$.dailySleepDTO.calendarDate') IS NOT NULL