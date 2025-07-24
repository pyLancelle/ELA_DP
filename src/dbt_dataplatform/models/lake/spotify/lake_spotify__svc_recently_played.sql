{{ config(dataset=get_schema('lake'), materialized='incremental', unique_key=['played_at', 'track_id']) }}

-- Spotify recently played tracks service layer
-- Extracts and transforms recently played data from raw JSON

WITH raw_data AS (
    SELECT
        raw_data,
        dp_inserted_at,
        source_file
    FROM {{ source('spotify', 'lake_spotify__stg_spotify_raw') }}
    WHERE data_type = 'recently_played'
    {% if is_incremental() %}
        AND dp_inserted_at > (SELECT MAX(dp_inserted_at) FROM {{ this }})
    {% endif %}
),

transformed AS (
    SELECT
        -- Extract core fields from JSON
        TIMESTAMP(JSON_VALUE(raw_data, '$.played_at')) AS played_at,
        JSON_VALUE(raw_data, '$.track.id') AS track_id,
        
        -- Store complete structures as JSON for flexibility
        JSON_QUERY(raw_data, '$.track') AS track,
        JSON_QUERY(raw_data, '$.context') AS context,
        
        -- Metadata
        dp_inserted_at,
        source_file
    FROM raw_data
    WHERE JSON_VALUE(raw_data, '$.played_at') IS NOT NULL
      AND JSON_VALUE(raw_data, '$.track.id') IS NOT NULL
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY played_at, track_id
            ORDER BY dp_inserted_at DESC
        ) AS row_num
    FROM transformed
)

SELECT * EXCEPT(row_num)
FROM deduplicated
WHERE row_num = 1