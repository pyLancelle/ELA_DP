SELECT
    DATE(played_at) AS play_date,
    artist_id,
    artist_name,
    COUNT(*) AS play_count
FROM {{ ref('hub_spotify__plays') }}
GROUP BY 1, 2, 3
