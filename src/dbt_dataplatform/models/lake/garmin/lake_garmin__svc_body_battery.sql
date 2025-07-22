{{ config(
    dataset=get_schema('lake'),
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['body_battery_date', 'start_timestamp_gmt']
) }}

-- Pure body battery data extraction from staging_garmin_raw  
-- Source: body_battery data type from Garmin Connect API
-- Deduplicates by keeping most recent record per date/timestamp combination

WITH body_battery_data_with_rank AS (
  SELECT
    -- Date reference
    DATE(SAFE.PARSE_TIMESTAMP('%Y-%m-%d', JSON_EXTRACT_SCALAR(raw_data, '$.date'))) AS body_battery_date,
    
    -- Daily summary metrics
    SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.charged') AS INT64) AS daily_charged,
    SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.drained') AS INT64) AS daily_drained,
    
    -- Day timestamps
    SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E1S', JSON_EXTRACT_SCALAR(raw_data, '$.startTimestampGMT')) AS start_timestamp_gmt,
    SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E1S', JSON_EXTRACT_SCALAR(raw_data, '$.endTimestampGMT')) AS end_timestamp_gmt,
    SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E1S', JSON_EXTRACT_SCALAR(raw_data, '$.startTimestampLocal')) AS start_timestamp_local,
    SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E1S', JSON_EXTRACT_SCALAR(raw_data, '$.endTimestampLocal')) AS end_timestamp_local,
    
    -- Current body battery status
    JSON_EXTRACT_SCALAR(raw_data, '$.bodyBatteryDynamicFeedbackEvent.bodyBatteryLevel') AS current_body_battery_level,
    JSON_EXTRACT_SCALAR(raw_data, '$.bodyBatteryDynamicFeedbackEvent.feedbackShortType') AS feedback_short_type,
    JSON_EXTRACT_SCALAR(raw_data, '$.bodyBatteryDynamicFeedbackEvent.feedbackLongType') AS feedback_long_type,
    SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E1S', JSON_EXTRACT_SCALAR(raw_data, '$.bodyBatteryDynamicFeedbackEvent.eventTimestampGmt')) AS feedback_timestamp_gmt,
    
    -- End of day status
    JSON_EXTRACT_SCALAR(raw_data, '$.endOfDayBodyBatteryDynamicFeedbackEvent.bodyBatteryLevel') AS end_of_day_level,
    JSON_EXTRACT_SCALAR(raw_data, '$.endOfDayBodyBatteryDynamicFeedbackEvent.feedbackShortType') AS end_of_day_feedback_short,
    JSON_EXTRACT_SCALAR(raw_data, '$.endOfDayBodyBatteryDynamicFeedbackEvent.feedbackLongType') AS end_of_day_feedback_long,
    SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E1S', JSON_EXTRACT_SCALAR(raw_data, '$.endOfDayBodyBatteryDynamicFeedbackEvent.eventTimestampGmt')) AS end_of_day_timestamp_gmt,
    
    -- Complex nested data preserved for hub layer processing
    JSON_EXTRACT(raw_data, '$.bodyBatteryValuesArray') AS body_battery_timeseries_json,
    JSON_EXTRACT(raw_data, '$.bodyBatteryValueDescriptorDTOList') AS timeseries_descriptor_json,
    JSON_EXTRACT(raw_data, '$.bodyBatteryActivityEvent') AS activity_events_json,
    
    -- Source metadata
    dp_inserted_at,
    source_file,
    
    -- Add row number to deduplicate by most recent dp_inserted_at
    ROW_NUMBER() OVER (
      PARTITION BY 
        DATE(SAFE.PARSE_TIMESTAMP('%Y-%m-%d', JSON_EXTRACT_SCALAR(raw_data, '$.date'))),
        SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E1S', JSON_EXTRACT_SCALAR(raw_data, '$.startTimestampGMT'))
      ORDER BY dp_inserted_at DESC
    ) AS row_rank

  FROM {{ source('garmin', 'lake_garmin__stg_garmin_raw') }}
  WHERE data_type = 'body_battery'
    AND JSON_EXTRACT_SCALAR(raw_data, '$.date') IS NOT NULL
    
  {% if is_incremental() %}
    AND dp_inserted_at > (SELECT MAX(dp_inserted_at) FROM {{ this }})
  {% endif %}
)

SELECT
  body_battery_date,
  daily_charged,
  daily_drained,
  start_timestamp_gmt,
  end_timestamp_gmt,
  start_timestamp_local,
  end_timestamp_local,
  current_body_battery_level,
  feedback_short_type,
  feedback_long_type,
  feedback_timestamp_gmt,
  end_of_day_level,
  end_of_day_feedback_short,
  end_of_day_feedback_long,
  end_of_day_timestamp_gmt,
  body_battery_timeseries_json,
  timeseries_descriptor_json,
  activity_events_json,
  dp_inserted_at,
  source_file

FROM body_battery_data_with_rank
WHERE row_rank = 1