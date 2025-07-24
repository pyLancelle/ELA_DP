{{ config(dataset=get_schema('lake'), materialized='incremental', unique_key=['track_id']) }}

-- Spotify top tracks service layer
-- Extracts and transforms top tracks data from raw JSON

WITH raw_data AS (
    SELECT
        raw_data,
        dp_inserted_at,
        source_file
    FROM {{ ref('lake_spotify__stg_spotify_raw') }}
    WHERE data_type = 'top_tracks'
    {% if is_incremental() %}
        AND dp_inserted_at > (SELECT MAX(dp_inserted_at) FROM {{ this }})
    {% endif %}
),

transformed AS (
    SELECT
        -- Extract core fields from JSON
        JSON_VALUE(raw_data, '$.id') AS track_id,
        JSON_VALUE(raw_data, '$.name') AS track_name,
        CAST(JSON_VALUE(raw_data, '$.popularity') AS INT64) AS popularity,
        CAST(JSON_VALUE(raw_data, '$.duration_ms') AS INT64) AS duration_ms,
        CAST(JSON_VALUE(raw_data, '$.explicit') AS BOOL) AS explicit,
        
        -- Store complete track data as JSON for detailed analysis
        raw_data AS track_data,
        
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
            PARTITION BY track_id
            ORDER BY dp_inserted_at DESC
        ) AS row_num
    FROM transformed
)

SELECT * EXCEPT(row_num)
FROM deduplicated
WHERE row_num = 1