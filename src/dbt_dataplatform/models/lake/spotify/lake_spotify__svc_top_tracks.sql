{{ config(
    dataset=get_schema('lake'),
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='track_id',
    tags=["lake", "spotify"]
) }}

-- Pure Lake model for Spotify top tracks data
-- Stores raw JSON data with basic metadata and deduplication only
-- All field extraction logic will be moved to Hub layer

WITH top_tracks_data_with_rank AS (
  SELECT
    -- Unique identifier for deduplication
    JSON_VALUE(raw_data, '$.id') AS track_id,
    
    -- Complete raw JSON data (to be parsed in Hub layer)
    raw_data,
    
    -- Data type for consistency
    data_type,
    
    -- Source metadata
    dp_inserted_at,
    source_file,
    
    -- Add row number to deduplicate by most recent dp_inserted_at
    ROW_NUMBER() OVER (
      PARTITION BY JSON_VALUE(raw_data, '$.id')
      ORDER BY dp_inserted_at DESC
    ) AS row_rank

  FROM {{ source('spotify', 'lake_spotify__stg_spotify_raw') }}
  WHERE data_type = 'top_tracks'
    AND JSON_VALUE(raw_data, '$.id') IS NOT NULL
    
  {% if is_incremental() %}
    AND dp_inserted_at > (SELECT MAX(dp_inserted_at) FROM {{ this }})
  {% endif %}
)

SELECT
  track_id,
  raw_data,
  data_type,
  dp_inserted_at,
  source_file

FROM top_tracks_data_with_rank
WHERE row_rank = 1