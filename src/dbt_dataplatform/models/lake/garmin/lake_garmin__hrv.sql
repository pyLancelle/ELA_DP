{{ config(
    dataset=get_schema('lake'),
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['hrv_date', 'sleep_start_gmt'],
    tags=["lake", "garmin"]
) }}

-- Pure Lake model for Garmin HRV data
-- Stores raw JSON data with basic metadata and deduplication only
-- All field extraction logic will be moved to Hub layer

WITH hrv_data_with_rank AS (
  SELECT
    -- Unique identifiers for deduplication
    DATE(SAFE.PARSE_TIMESTAMP('%Y-%m-%d', JSON_EXTRACT_SCALAR(raw_data, '$.hrvSummary.calendarDate'))) AS hrv_date,
    TIMESTAMP_MILLIS(SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.sleepStartTimestampGMT') AS INT64)) AS sleep_start_gmt,
    
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
        DATE(SAFE.PARSE_TIMESTAMP('%Y-%m-%d', JSON_EXTRACT_SCALAR(raw_data, '$.hrvSummary.calendarDate'))),
        TIMESTAMP_MILLIS(SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.sleepStartTimestampGMT') AS INT64))
      ORDER BY dp_inserted_at DESC
    ) AS row_rank

  FROM {{ source('garmin', 'lake_garmin__stg_garmin_raw') }}
  WHERE data_type = 'hrv'
    AND JSON_EXTRACT_SCALAR(raw_data, '$.hrvSummary.calendarDate') IS NOT NULL
    AND JSON_EXTRACT_SCALAR(raw_data, '$.sleepStartTimestampGMT') IS NOT NULL
    
  {% if is_incremental() %}
    AND dp_inserted_at > (SELECT MAX(dp_inserted_at) FROM {{ this }})
  {% endif %}
)

SELECT
  hrv_date,
  sleep_start_gmt,
  raw_data,
  data_type,
  dp_inserted_at,
  source_file

FROM hrv_data_with_rank
WHERE row_rank = 1