{{ config(
    dataset=get_schema('lake'),
    materialized='incremental',
    unique_key=['played_at', 'track_id']
) }}

WITH raw_data AS (
    SELECT
        *,
        TIMESTAMP(played_at) AS played_at_ts,
        track.id AS track_id
    FROM {{ source('spotify', 'staging_spotify') }}
    WHERE played_at IS NOT NULL
    {% if is_incremental() %}
        AND dp_inserted_at > (SELECT MAX(dp_inserted_at) FROM {{ this }})
    {% endif %}
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY played_at_ts, track_id
            ORDER BY dp_inserted_at DESC
        ) AS row_num
    FROM raw_data
)

SELECT
    * EXCEPT(played_at, track_id, row_num),
    played_at_ts AS played_at,
    track_id
FROM deduplicated
WHERE row_num = 1
