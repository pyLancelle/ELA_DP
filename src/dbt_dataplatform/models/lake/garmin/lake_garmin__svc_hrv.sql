{{ config(
    dataset=get_schema('lake'),
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['hrv_date', 'sleep_start_gmt']
) }}

-- Pure heart rate variability data extraction from staging_garmin_raw
-- Source: hrv data type from Garmin Connect API
-- Deduplicates by keeping most recent record per date/timestamp combination

WITH hrv_data_with_rank AS (
  SELECT
    -- User and date identifiers
    SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.userProfilePk') AS INT64) AS user_profile_pk,
    DATE(SAFE.PARSE_TIMESTAMP('%Y-%m-%d', JSON_EXTRACT_SCALAR(raw_data, '$.hrvSummary.calendarDate'))) AS hrv_date,
    
    -- HRV summary metrics
    SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.hrvSummary.weeklyAvg') AS INT64) AS weekly_avg_hrv,
    SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.hrvSummary.lastNightAvg') AS INT64) AS last_night_avg_hrv,
    SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.hrvSummary.lastNight5MinHigh') AS INT64) AS last_night_5min_high_hrv,
    
    -- HRV baseline ranges
    SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.hrvSummary.baseline.lowUpper') AS INT64) AS baseline_low_upper,
    SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.hrvSummary.baseline.balancedLow') AS INT64) AS baseline_balanced_low,
    SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.hrvSummary.baseline.balancedUpper') AS INT64) AS baseline_balanced_upper,
    SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.hrvSummary.baseline.markerValue') AS FLOAT64) AS baseline_marker_value,
    
    -- HRV status and feedback
    JSON_EXTRACT_SCALAR(raw_data, '$.hrvSummary.status') AS hrv_status,
    JSON_EXTRACT_SCALAR(raw_data, '$.hrvSummary.feedbackPhrase') AS feedback_phrase,
    SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S', JSON_EXTRACT_SCALAR(raw_data, '$.hrvSummary.createTimeStamp')) AS summary_created_at,
    
    -- Sleep period for context
    SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E1S', JSON_EXTRACT_SCALAR(raw_data, '$.sleepStartTimestampGMT')) AS sleep_start_gmt,
    SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E1S', JSON_EXTRACT_SCALAR(raw_data, '$.sleepEndTimestampGMT')) AS sleep_end_gmt,
    SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E1S', JSON_EXTRACT_SCALAR(raw_data, '$.sleepStartTimestampLocal')) AS sleep_start_local,
    SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E1S', JSON_EXTRACT_SCALAR(raw_data, '$.sleepEndTimestampLocal')) AS sleep_end_local,
    
    -- Measurement period
    SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E1S', JSON_EXTRACT_SCALAR(raw_data, '$.startTimestampGMT')) AS measurement_start_gmt,
    SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E1S', JSON_EXTRACT_SCALAR(raw_data, '$.endTimestampGMT')) AS measurement_end_gmt,
    SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E1S', JSON_EXTRACT_SCALAR(raw_data, '$.startTimestampLocal')) AS measurement_start_local,
    SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E1S', JSON_EXTRACT_SCALAR(raw_data, '$.endTimestampLocal')) AS measurement_end_local,
    
    -- Individual HRV readings preserved as JSON for hub layer processing
    JSON_EXTRACT(raw_data, '$.hrvReadings') AS hrv_readings_timeseries_json,
    
    -- Source metadata
    dp_inserted_at,
    source_file,
    
    -- Add row number to deduplicate by most recent dp_inserted_at
    ROW_NUMBER() OVER (
      PARTITION BY 
        DATE(SAFE.PARSE_TIMESTAMP('%Y-%m-%d', JSON_EXTRACT_SCALAR(raw_data, '$.hrvSummary.calendarDate'))),
        SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E1S', JSON_EXTRACT_SCALAR(raw_data, '$.sleepStartTimestampGMT'))
      ORDER BY dp_inserted_at DESC
    ) AS row_rank

  FROM {{ source('garmin', 'lake_garmin__stg_garmin_raw') }}
  WHERE data_type = 'hrv'
    AND JSON_EXTRACT_SCALAR(raw_data, '$.hrvSummary.calendarDate') IS NOT NULL
    
  {% if is_incremental() %}
    AND dp_inserted_at > (SELECT MAX(dp_inserted_at) FROM {{ this }})
  {% endif %}
)

SELECT
  user_profile_pk,
  hrv_date,
  weekly_avg_hrv,
  last_night_avg_hrv,
  last_night_5min_high_hrv,
  baseline_low_upper,
  baseline_balanced_low,
  baseline_balanced_upper,
  baseline_marker_value,
  hrv_status,
  feedback_phrase,
  summary_created_at,
  sleep_start_gmt,
  sleep_end_gmt,
  sleep_start_local,
  sleep_end_local,
  measurement_start_gmt,
  measurement_end_gmt,
  measurement_start_local,
  measurement_end_local,
  hrv_readings_timeseries_json,
  dp_inserted_at,
  source_file

FROM hrv_data_with_rank
WHERE row_rank = 1