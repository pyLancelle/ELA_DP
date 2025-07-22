{{ config(dataset=get_schema('lake')) }}

-- Pure steps interval data extraction from staging_garmin_raw
-- Source: steps data type from Garmin Connect API
-- Each record represents a 15-minute interval

SELECT
    -- Interval timestamps
    SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E1S', JSON_EXTRACT_SCALAR(raw_data, '$.startGMT')) AS interval_start_gmt,
    SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E1S', JSON_EXTRACT_SCALAR(raw_data, '$.endGMT')) AS interval_end_gmt,
    
    -- Date reference
    DATE(SAFE.PARSE_TIMESTAMP('%Y-%m-%d', JSON_EXTRACT_SCALAR(raw_data, '$.date'))) AS activity_date,
    
    -- Step metrics
    SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.steps') AS INT64) AS steps,
    SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.pushes') AS INT64) AS wheelchair_pushes,
    
    -- Activity level classification
    JSON_EXTRACT_SCALAR(raw_data, '$.primaryActivityLevel') AS primary_activity_level,
    SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.activityLevelConstant') AS BOOL) AS activity_level_constant,
    
    -- Source metadata
    dp_inserted_at,
    source_file

FROM {{ source('garmin', 'staging_garmin_raw') }}
WHERE data_type = 'steps'
  AND JSON_EXTRACT_SCALAR(raw_data, '$.startGMT') IS NOT NULL
  AND JSON_EXTRACT_SCALAR(raw_data, '$.endGMT') IS NOT NULL