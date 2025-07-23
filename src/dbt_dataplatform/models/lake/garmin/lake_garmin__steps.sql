{{ config(
    dataset=get_schema('lake'),
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['interval_start_gmt', 'interval_end_gmt']
) }}

-- Pure Lake model for Garmin steps data
-- Stores raw JSON data with basic metadata and deduplication only
-- All field extraction logic will be moved to Hub layer

WITH steps_data_with_rank AS (
  SELECT
    -- Unique identifiers for deduplication
    TIMESTAMP_MILLIS(SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.startGMT') AS INT64)) AS interval_start_gmt,
    TIMESTAMP_MILLIS(SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.endGMT') AS INT64)) AS interval_end_gmt,
    
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
        TIMESTAMP_MILLIS(SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.startGMT') AS INT64)),
        TIMESTAMP_MILLIS(SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.endGMT') AS INT64))
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
  raw_data,
  data_type,
  dp_inserted_at,
  source_file

FROM steps_data_with_rank
WHERE row_rank = 1