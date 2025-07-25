{{ config(
    dataset=get_schema('lake'),
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['body_battery_date', 'start_timestamp_gmt'],
    tags=["lake", "garmin"]
) }}

-- Pure Lake model for Garmin body battery data
-- Stores raw JSON data with basic metadata and deduplication only
-- All field extraction logic will be moved to Hub layer

WITH body_battery_data_with_rank AS (
  SELECT
    -- Unique identifiers for deduplication
    DATE(SAFE.PARSE_TIMESTAMP('%Y-%m-%d', JSON_EXTRACT_SCALAR(raw_data, '$.date'))) AS body_battery_date,
    TIMESTAMP_MILLIS(SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.startTimestampGMT') AS INT64)) AS start_timestamp_gmt,
    
    -- Complete raw JSON data (to be parsed in Hub layer)
    raw_data,
    
    -- Data type for consistency
    data_type,
    
    -- Source metadata
    dp_inserted_at,
    source_file,
    
    -- Add row number to deduplicate by most recent dp_inserted_at
    ROW_NUMBER() OVER (
      PARTITION BY 
        DATE(SAFE.PARSE_TIMESTAMP('%Y-%m-%d', JSON_EXTRACT_SCALAR(raw_data, '$.date'))),
        TIMESTAMP_MILLIS(SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.startTimestampGMT') AS INT64))
      ORDER BY dp_inserted_at DESC
    ) AS row_rank

  FROM {{ source('garmin', 'lake_garmin__stg_garmin_raw') }}
  WHERE data_type = 'body_battery'
    AND JSON_EXTRACT_SCALAR(raw_data, '$.date') IS NOT NULL
    AND JSON_EXTRACT_SCALAR(raw_data, '$.startTimestampGMT') IS NOT NULL
    
  {% if is_incremental() %}
    AND dp_inserted_at > (SELECT MAX(dp_inserted_at) FROM {{ this }})
  {% endif %}
)

SELECT
  body_battery_date,
  start_timestamp_gmt,
  raw_data,
  data_type,
  dp_inserted_at,
  source_file

FROM body_battery_data_with_rank
WHERE row_rank = 1