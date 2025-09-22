{{ config(
    dataset=get_schema('product'),
    materialized='view',
    tags=["product", "spotify", "evolution"]
) }}

-- Top 10 Artists Evolution - Compare current week vs previous week

WITH current_week_stats AS (
    SELECT
        artists[OFFSET(0)].name as artist_name,
        artists[OFFSET(0)].external_urls.spotify as artist_url,
        artists[OFFSET(0)].id as artist_id,
        COUNT(*) as total_plays,
        SUM(track.duration_ms) / 1000 / 60 as total_minutes
    FROM {{ ref('hub_spotify__recently_played') }}
    WHERE artists IS NOT NULL
        AND ARRAY_LENGTH(artists) > 0
        AND played_at >= TIMESTAMP(DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 WEEK), WEEK(MONDAY)))
        AND played_at < TIMESTAMP(DATE_TRUNC(CURRENT_DATE(), WEEK(MONDAY)))
    GROUP BY 1, 2, 3
),

previous_week_stats AS (
    SELECT
        artists[OFFSET(0)].name as artist_name,
        artists[OFFSET(0)].external_urls.spotify as artist_url,
        artists[OFFSET(0)].id as artist_id,
        COUNT(*) as total_plays,
        SUM(track.duration_ms) / 1000 / 60 as total_minutes
    FROM {{ ref('hub_spotify__recently_played') }}
    WHERE artists IS NOT NULL
        AND ARRAY_LENGTH(artists) > 0
        AND played_at >= TIMESTAMP(DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 2 WEEK), WEEK(MONDAY)))
        AND played_at < TIMESTAMP(DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 WEEK), WEEK(MONDAY)))
    GROUP BY 1, 2, 3
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
),

artist_image AS (
    -- Get ONE image per artist (most recent album image)
    SELECT
        artists[OFFSET(0)].id as artist_id,
        (
            SELECT url
            FROM UNNEST(album.images) as img
            ORDER BY img.height DESC
            LIMIT 1
        ) as artist_image_url
    FROM {{ ref('hub_spotify__recently_played') }}
    WHERE artists IS NOT NULL
      AND ARRAY_LENGTH(artists) > 0
      AND album.images IS NOT NULL
      AND ARRAY_LENGTH(album.images) > 0
      AND played_at >= TIMESTAMP(DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 WEEK), WEEK(MONDAY)))
      AND played_at < TIMESTAMP(DATE_TRUNC(CURRENT_DATE(), WEEK(MONDAY)))
    QUALIFY ROW_NUMBER() OVER (PARTITION BY artists[OFFSET(0)].id ORDER BY played_at DESC) = 1
)

SELECT
    c.current_rank as rank,
    c.artist_name,
    CONCAT(
        CAST(FLOOR(c.total_minutes / 60) AS STRING), 'h',
        LPAD(CAST(MOD(CAST(c.total_minutes AS INT64), 60) AS STRING), 2, '0')
    ) as time_listened,
    c.total_plays,
    c.artist_url,
    COALESCE(
        i.artist_image_url,
        CONCAT('https://via.placeholder.com/300x300/1DB954/FFFFFF?text=', SUBSTR(c.artist_name, 1, 1))
    ) as artist_image_url,

    -- Evolution indicators
    p.previous_rank,
    CASE
        WHEN p.previous_rank IS NULL THEN 'NEW' -- Nouvel entrant
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
    c.total_minutes - COALESCE(p.total_minutes, 0) as minutes_diff

FROM current_week_ranked c
LEFT JOIN previous_week_ranked p ON c.artist_id = p.artist_id
LEFT JOIN artist_image i ON c.artist_id = i.artist_id
ORDER BY c.current_rank