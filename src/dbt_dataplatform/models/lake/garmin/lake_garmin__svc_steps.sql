{{ config(
    dataset=get_schema('lake'),
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['interval_start_gmt', 'interval_end_gmt']
) }}

-- Pure steps interval data extraction from staging_garmin_raw
-- Source: steps data type from Garmin Connect API
-- Each record represents a 15-minute interval
-- Deduplicates by keeping most recent record per interval

WITH steps_data_with_rank AS (
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
    source_file,
    
    -- Add row number to deduplicate by most recent dp_inserted_at
    ROW_NUMBER() OVER (
      PARTITION BY 
        SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E1S', JSON_EXTRACT_SCALAR(raw_data, '$.startGMT')),
        SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E1S', JSON_EXTRACT_SCALAR(raw_data, '$.endGMT'))
      ORDER BY dp_inserted_at DESC
    ) AS row_rank

  FROM {{ source('garmin', 'lake_garmin__stg_garmin_raw') }}
  WHERE data_type = 'steps'
    AND JSON_EXTRACT_SCALAR(raw_data, '$.startGMT') IS NOT NULL
    AND JSON_EXTRACT_SCALAR(raw_data, '$.endGMT') IS NOT NULL
    
  {% if is_incremental() %}
    AND dp_inserted_at > (SELECT MAX(dp_inserted_at) FROM {{ this }})
  {% endif %}
)

SELECT
  interval_start_gmt,
  interval_end_gmt,
  activity_date,
  steps,
  wheelchair_pushes,
  primary_activity_level,
  activity_level_constant,
  dp_inserted_at,
  source_file

FROM steps_data_with_rank
WHERE row_rank = 1