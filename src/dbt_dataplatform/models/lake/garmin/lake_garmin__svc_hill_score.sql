{{ config(
    dataset=get_schema('lake'),
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='score_date',
    tags=["lake", "garmin"]
) }}

-- Pure Lake model for Garmin hill score data
-- Stores raw JSON data with basic metadata and deduplication only
-- All field extraction logic will be moved to Hub layer

WITH hill_score_data_with_rank AS (
  SELECT
    -- Unique identifier for deduplication - use the period end date from the API response
    COALESCE(
      DATE(JSON_EXTRACT_SCALAR(raw_data, '$.endDate')),
      DATE(dp_inserted_at)
    ) AS score_date,
    
    -- Complete raw JSON data (to be parsed in Hub layer)
    raw_data,
    
    -- Data type for consistency
    data_type,
    
    -- Source metadata
    dp_inserted_at,
    source_file,
    
    -- Add row number to deduplicate by most recent dp_inserted_at
    ROW_NUMBER() OVER (
      PARTITION BY COALESCE(
        DATE(JSON_EXTRACT_SCALAR(raw_data, '$.endDate')),
        DATE(dp_inserted_at)
      )
      ORDER BY dp_inserted_at DESC
    ) AS row_rank

  FROM {{ source('garmin', 'lake_garmin__stg_garmin_raw') }}
  WHERE data_type = 'hill_score'
    
  {% if is_incremental() %}
    AND dp_inserted_at > (SELECT MAX(dp_inserted_at) FROM {{ this }})
  {% endif %}
)

SELECT
  score_date,
  raw_data,
  data_type,
  dp_inserted_at,
  source_file

FROM hill_score_data_with_rank
WHERE row_rank = 1