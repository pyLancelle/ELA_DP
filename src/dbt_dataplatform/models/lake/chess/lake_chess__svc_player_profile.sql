{{ config(
    dataset=get_schema('lake'),
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['username'],
    tags=["lake", "chess"]
) }}

-- Pure Lake model for Chess.com player profile data
-- Stores raw JSON data with basic metadata and deduplication only
-- All field extraction logic will be moved to Hub layer

WITH profile_data_with_rank AS (
  SELECT
    -- Unique identifier for deduplication
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
      PARTITION BY username
      ORDER BY dp_inserted_at DESC
    ) AS row_rank

  FROM {{ source('chess', 'lake_chess__stg_chess_raw') }}
  WHERE data_type = 'player_profile'
    AND username IS NOT NULL
    
  {% if is_incremental() %}
    AND dp_inserted_at > (SELECT MAX(dp_inserted_at) FROM {{ this }})
  {% endif %}
)

SELECT
  username,
  raw_data,
  data_type,
  dp_inserted_at,
  source_file

FROM profile_data_with_rank
WHERE row_rank = 1