{{ config(
    dataset=get_schema('lake'),
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='saved_track_id',
    tags=["lake", "spotify"]
) }}

-- Pure Lake model for Spotify saved tracks data
-- Stores raw JSON data with basic metadata and deduplication only
-- All field extraction logic will be moved to Hub layer

WITH saved_tracks_data_with_rank AS (
  SELECT
    -- Unique identifier for deduplication (added_at + track_id)
    CONCAT(JSON_VALUE(raw_data, '$.added_at'), '_', JSON_VALUE(raw_data, '$.track.id')) AS saved_track_id,
    
    -- Complete raw JSON data (to be parsed in Hub layer)
    raw_data,
    
    -- Data type for consistency
    data_type,
    
    -- Source metadata
    dp_inserted_at,
    source_file,
    
    -- Add row number to deduplicate by most recent dp_inserted_at
    ROW_NUMBER() OVER (
      PARTITION BY CONCAT(JSON_VALUE(raw_data, '$.added_at'), '_', JSON_VALUE(raw_data, '$.track.id'))
      ORDER BY dp_inserted_at DESC
    ) AS row_rank

  FROM {{ source('spotify', 'lake_spotify__stg_spotify_raw') }}
  WHERE data_type = 'saved_tracks'
    AND JSON_VALUE(raw_data, '$.added_at') IS NOT NULL
    AND JSON_VALUE(raw_data, '$.track.id') IS NOT NULL
    
  {% if is_incremental() %}
    AND dp_inserted_at > (SELECT MAX(dp_inserted_at) FROM {{ this }})
  {% endif %}
)

SELECT
  saved_track_id,
  raw_data,
  data_type,
  dp_inserted_at,
  source_file

FROM saved_tracks_data_with_rank
WHERE row_rank = 1