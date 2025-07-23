{{ config(
    dataset=get_schema('lake'),
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='prediction_date',
    tags=["lake", "garmin"]
) }}

-- Pure Lake model for Garmin race predictions data
-- Stores raw JSON data with basic metadata and deduplication only
-- All field extraction logic will be moved to Hub layer

WITH race_predictions_data_with_rank AS (
  SELECT
    -- Unique identifier for deduplication
    DATE(SAFE.PARSE_TIMESTAMP('%Y-%m-%d', JSON_EXTRACT_SCALAR(raw_data, '$.calendarDate'))) AS prediction_date,
    
    -- Complete raw JSON data (to be parsed in Hub layer)
    raw_data,
    
    -- Data type for consistency
    data_type,
    
    -- Source metadata
    dp_inserted_at,
    source_file,
    
    -- Add row number to deduplicate by most recent dp_inserted_at
    ROW_NUMBER() OVER (
      PARTITION BY DATE(SAFE.PARSE_TIMESTAMP('%Y-%m-%d', JSON_EXTRACT_SCALAR(raw_data, '$.calendarDate')))
      ORDER BY dp_inserted_at DESC
    ) AS row_rank

  FROM {{ source('garmin', 'lake_garmin__stg_garmin_raw') }}
  WHERE data_type = 'race_predictions'
    AND JSON_EXTRACT_SCALAR(raw_data, '$.calendarDate') IS NOT NULL
    
  {% if is_incremental() %}
    AND dp_inserted_at > (SELECT MAX(dp_inserted_at) FROM {{ this }})
  {% endif %}
)

SELECT
  prediction_date,
  raw_data,
  data_type,
  dp_inserted_at,
  source_file

FROM race_predictions_data_with_rank
WHERE row_rank = 1