{{ config(dataset=get_schema('product'), materialized='view') }}

WITH plays_dedup AS (
    SELECT DISTINCT played_at, track_id, track_name
    FROM {{ ref('hub_spotify__plays') }}
),
plays_with_duration AS (
    SELECT p.played_at, p.track_id, p.track_name, t.duration_ms
    FROM plays_dedup p
    LEFT JOIN {{ ref('hub_spotify__tracks') }} t
        ON p.track_id = t.track_id
)
SELECT
    track_id,
    track_name,
    COUNT(*) AS play_count,
    SUM(duration_ms) AS total_play_time_ms
FROM plays_with_duration
GROUP BY 1,2
ORDER BY total_play_time_ms DESC
LIMIT 10
