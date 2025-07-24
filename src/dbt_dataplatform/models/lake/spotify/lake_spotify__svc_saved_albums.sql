{{ config(dataset=get_schema('lake'), materialized='incremental', unique_key=['added_at', 'album_id']) }}

-- Spotify saved albums service layer
-- Extracts and transforms saved albums data from raw JSON

WITH raw_data AS (
    SELECT
        raw_data,
        dp_inserted_at,
        source_file
    FROM {{ ref('lake_spotify__stg_spotify_raw') }}
    WHERE data_type = 'saved_albums'
    {% if is_incremental() %}
        AND dp_inserted_at > (SELECT MAX(dp_inserted_at) FROM {{ this }})
    {% endif %}
),

transformed AS (
    SELECT
        -- Extract core fields from JSON
        TIMESTAMP(JSON_VALUE(raw_data, '$.added_at')) AS added_at,
        JSON_VALUE(raw_data, '$.album.id') AS album_id,
        
        -- Store complete album structure as JSON
        JSON_QUERY(raw_data, '$.album') AS album,
        
        -- Metadata
        dp_inserted_at,
        source_file
    FROM raw_data
    WHERE JSON_VALUE(raw_data, '$.added_at') IS NOT NULL
      AND JSON_VALUE(raw_data, '$.album.id') IS NOT NULL
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY added_at, album_id
            ORDER BY dp_inserted_at DESC
        ) AS row_num
    FROM transformed
)

SELECT * EXCEPT(row_num)
FROM deduplicated
WHERE row_num = 1