{{ config(dataset=get_schema('lake')) }}

-- Pure race predictions data extraction from staging_garmin_raw
-- Source: race_predictions data type from Garmin Connect API

SELECT
    -- User and date identifiers
    SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.userId') AS INT64) AS user_id,
    DATE(SAFE.PARSE_TIMESTAMP('%Y-%m-%d', JSON_EXTRACT_SCALAR(raw_data, '$.calendarDate'))) AS prediction_date,
    
    -- Date range (if applicable)
    SAFE.PARSE_DATE('%Y-%m-%d', JSON_EXTRACT_SCALAR(raw_data, '$.fromCalendarDate')) AS from_calendar_date,
    SAFE.PARSE_DATE('%Y-%m-%d', JSON_EXTRACT_SCALAR(raw_data, '$.toCalendarDate')) AS to_calendar_date,
    
    -- Race time predictions (in seconds)
    SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.time5K') AS INT64) AS predicted_5k_seconds,
    SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.time10K') AS INT64) AS predicted_10k_seconds,
    SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.timeHalfMarathon') AS INT64) AS predicted_half_marathon_seconds,
    SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.timeMarathon') AS INT64) AS predicted_marathon_seconds,
    
    -- Source metadata
    dp_inserted_at,
    source_file

FROM {{ source('garmin', 'staging_garmin_raw') }}
WHERE data_type = 'race_predictions'
  AND JSON_EXTRACT_SCALAR(raw_data, '$.userId') IS NOT NULL