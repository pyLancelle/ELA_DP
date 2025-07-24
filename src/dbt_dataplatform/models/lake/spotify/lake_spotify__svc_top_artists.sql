{{ config(dataset=get_schema('lake'), materialized='incremental', unique_key=['artist_id']) }}

-- Spotify top artists service layer
-- Extracts and transforms top artists data from raw JSON

WITH raw_data AS (
    SELECT
        raw_data,
        dp_inserted_at,
        source_file
    FROM {{ ref('lake_spotify__stg_spotify_raw') }}
    WHERE data_type = 'top_artists'
    {% if is_incremental() %}
        AND dp_inserted_at > (SELECT MAX(dp_inserted_at) FROM {{ this }})
    {% endif %}
),

transformed AS (
    SELECT
        -- Extract core fields from JSON
        JSON_VALUE(raw_data, '$.id') AS artist_id,
        JSON_VALUE(raw_data, '$.name') AS artist_name,
        CAST(JSON_VALUE(raw_data, '$.popularity') AS INT64) AS popularity,
        JSON_QUERY_ARRAY(raw_data, '$.genres') AS genres,
        
        -- Store complete artist data as JSON for detailed analysis
        raw_data AS artist_data,
        
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
            PARTITION BY artist_id
            ORDER BY dp_inserted_at DESC
        ) AS row_num
    FROM transformed
)

SELECT * EXCEPT(row_num)
FROM deduplicated
WHERE row_num = 1