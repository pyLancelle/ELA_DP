{{ config(dataset=get_schema('lake'), materialized='incremental', unique_key=['playlist_id']) }}

-- Spotify playlists service layer
-- Extracts and transforms playlists data from raw JSON

WITH raw_data AS (
    SELECT
        raw_data,
        dp_inserted_at,
        source_file
    FROM {{ source('spotify', 'lake_spotify__stg_spotify_raw') }}
    WHERE data_type = 'playlists'
    {% if is_incremental() %}
        AND dp_inserted_at > (SELECT MAX(dp_inserted_at) FROM {{ this }})
    {% endif %}
),

transformed AS (
    SELECT
        -- Extract core fields from JSON
        JSON_VALUE(raw_data, '$.id') AS playlist_id,
        JSON_VALUE(raw_data, '$.name') AS playlist_name,
        CAST(JSON_VALUE(raw_data, '$.public') AS BOOL) AS is_public,
        CAST(JSON_VALUE(raw_data, '$.collaborative') AS BOOL) AS is_collaborative,
        
        -- Store complete playlist data as JSON for detailed analysis
        raw_data AS playlist_data,
        
        -- Metadata
        dp_inserted_at,
        source_file
    FROM raw_data
    WHERE JSON_VALUE(raw_data, '$.id') IS NOT NULL
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY playlist_id
            ORDER BY dp_inserted_at DESC
        ) AS row_num
    FROM transformed
)

SELECT * EXCEPT(row_num)
FROM deduplicated
WHERE row_num = 1