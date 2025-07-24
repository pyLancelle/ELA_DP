{{ config(
    dataset=get_schema('lake'),
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='activity_id',
    tags=["lake", "garmin"]
) }}

-- Pure Lake model for Garmin activities data
-- Stores raw JSON data with basic metadata and deduplication only
-- All field extraction logic will be moved to Hub layer

WITH activities_data_with_rank AS (
  SELECT
    -- Unique identifier for deduplication
    JSON_EXTRACT_SCALAR(raw_data, '$.activityId') AS activity_id,
    
    -- Complete raw JSON data (to be parsed in Hub layer)
    raw_data,
    
    -- Data type for consistency
    data_type,
    
    -- Source metadata
    dp_inserted_at,
    source_file,
    
    -- Add row number to deduplicate by most recent dp_inserted_at
    ROW_NUMBER() OVER (
      PARTITION BY JSON_EXTRACT_SCALAR(raw_data, '$.activityId')
      ORDER BY dp_inserted_at DESC
    ) AS row_rank

  FROM {{ source('garmin', 'lake_garmin__stg_garmin_raw') }}
  WHERE data_type = 'activities'
    AND JSON_EXTRACT_SCALAR(raw_data, '$.activityId') IS NOT NULL
    
  {% if is_incremental() %}
    AND dp_inserted_at > (SELECT MAX(dp_inserted_at) FROM {{ this }})
  {% endif %}
)

SELECT
  activity_id,
  raw_data,
  data_type,
  dp_inserted_at,
  source_file

FROM activities_data_with_rank
WHERE row_rank = 1