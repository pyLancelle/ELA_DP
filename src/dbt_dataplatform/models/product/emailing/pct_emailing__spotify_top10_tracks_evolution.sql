{{ config(
    dataset=get_schema('product'),
    materialized='view',
    tags=["product", "spotify", "evolution"]
) }}

-- Top 10 Tracks Evolution - Compare current week vs previous week

WITH current_week_stats AS (
    SELECT
        track_name,
        artists[OFFSET(0)].name as artist_name,
        track.external_urls.spotify as track_url,
        track_id,
        -- Get largest album cover
        (
            SELECT url
            FROM UNNEST(album.images) as img
            ORDER BY img.height DESC
            LIMIT 1
        ) as album_cover_url,
        COUNT(*) as total_plays,
        SUM(track.duration_ms) / 1000 / 60 as total_minutes
    FROM {{ ref('hub_spotify__recently_played') }}
    WHERE track_id IS NOT NULL
        AND artists IS NOT NULL
        AND ARRAY_LENGTH(artists) > 0
        AND played_at >= TIMESTAMP(DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 WEEK), WEEK(MONDAY)))
        AND played_at < TIMESTAMP(DATE_TRUNC(CURRENT_DATE(), WEEK(MONDAY)))
    GROUP BY 1, 2, 3, 4, 5
),

previous_week_stats AS (
    SELECT
        track_name,
        artists[OFFSET(0)].name as artist_name,
        track.external_urls.spotify as track_url,
        track_id,
        (
            SELECT url
            FROM UNNEST(album.images) as img
            ORDER BY img.height DESC
            LIMIT 1
        ) as album_cover_url,
        COUNT(*) as total_plays,
        SUM(track.duration_ms) / 1000 / 60 as total_minutes
    FROM {{ ref('hub_spotify__recently_played') }}
    WHERE track_id IS NOT NULL
        AND artists IS NOT NULL
        AND ARRAY_LENGTH(artists) > 0
        AND played_at >= TIMESTAMP(DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 2 WEEK), WEEK(MONDAY)))
        AND played_at < TIMESTAMP(DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 WEEK), WEEK(MONDAY)))
    GROUP BY 1, 2, 3, 4, 5
),

current_week_ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY total_minutes DESC) as current_rank
    FROM current_week_stats
    QUALIFY current_rank <= 10
),

previous_week_ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY total_minutes DESC) as previous_rank
    FROM previous_week_stats
    QUALIFY previous_rank <= 10
)

SELECT
    c.current_rank as rank,
    c.artist_name,
    c.track_name,
    CONCAT(
        CAST(FLOOR(c.total_minutes / 60) AS STRING), 'h',
        LPAD(CAST(MOD(CAST(c.total_minutes AS INT64), 60) AS STRING), 2, '0')
    ) as time_listened,
    c.total_plays,
    c.total_minutes,
    c.track_url,
    c.album_cover_url,

    -- Evolution indicators
    p.previous_rank,
    CASE
        WHEN p.previous_rank IS NULL THEN 'NEW' -- Nouveau titre
        WHEN p.previous_rank > c.current_rank THEN 'UP' -- Montée dans le classement
        WHEN p.previous_rank < c.current_rank THEN 'DOWN' -- Descente dans le classement
        WHEN p.previous_rank = c.current_rank THEN 'SAME' -- Même position
    END as evolution_status,

    CASE
        WHEN p.previous_rank IS NULL THEN CONCAT('🆕 NEW')
        WHEN p.previous_rank > c.current_rank THEN CONCAT('⬆️ +', CAST(p.previous_rank - c.current_rank AS STRING))
        WHEN p.previous_rank < c.current_rank THEN CONCAT('⬇️ -', CAST(c.current_rank - p.previous_rank AS STRING))
        WHEN p.previous_rank = c.current_rank THEN '➖ ='
    END as evolution_display,

    -- Additional metrics for analysis
    ABS(COALESCE(p.previous_rank, 11) - c.current_rank) as rank_change_abs,
    c.total_minutes - COALESCE(p.total_minutes, 0) as minutes_diff,

    -- Track-specific info
    CONCAT(c.artist_name, ' - ', c.track_name) as full_title

FROM current_week_ranked c
LEFT JOIN previous_week_ranked p ON c.track_id = p.track_id
ORDER BY c.current_rank