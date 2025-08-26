{{ config(
    dataset=get_schema('lake'),
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['club_id'],
    tags=["lake", "chess"]
) }}

-- Pure Lake model for Chess.com clubs data
-- Stores raw JSON data with basic metadata and deduplication only
-- All field extraction logic will be moved to Hub layer

WITH clubs_data_with_rank AS (
  SELECT
    -- Unique identifier for deduplication
    JSON_EXTRACT_SCALAR(raw_data, '$.club_id') AS club_id,
    
    -- Username for partitioning
    username,
    
    -- Complete raw JSON data (to be parsed in Hub layer)
    raw_data,
    
    -- Data type for consistency
    data_type,
    
    -- Source metadata
    dp_inserted_at,
    source_file,
    
    -- Add row number to deduplicate by most recent dp_inserted_at
    ROW_NUMBER() OVER (
      PARTITION BY JSON_EXTRACT_SCALAR(raw_data, '$.club_id'), username
      ORDER BY dp_inserted_at DESC
    ) AS row_rank

  FROM {{ source('chess', 'lake_chess__stg_chess_raw') }}
  WHERE data_type = 'clubs'
    AND JSON_EXTRACT_SCALAR(raw_data, '$.club_id') IS NOT NULL
    
  {% if is_incremental() %}
    AND dp_inserted_at > (SELECT MAX(dp_inserted_at) FROM {{ this }})
  {% endif %}
)

SELECT
  club_id,
  username,
  raw_data,
  data_type,
  dp_inserted_at,
  source_file

FROM clubs_data_with_rank
WHERE row_rank = 1