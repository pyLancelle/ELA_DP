{{ config(
    dataset=get_schema('product'), 
    materialized='view', 
    tags=["product", "spotify"]
) }}

-- Top 10 Artists All Time - Based on actual listening time

WITH filtered_plays AS (
    SELECT
        *
    FROM {{ ref('hub_spotify__recently_played') }}
    WHERE artists IS NOT NULL
        AND ARRAY_LENGTH(artists) > 0
      AND played_at >= TIMESTAMP(DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 WEEK), WEEK(MONDAY)))
      AND played_at < TIMESTAMP(DATE_TRUNC(CURRENT_DATE(), WEEK(MONDAY)))
),
artist_stats AS (
    SELECT
        artists[OFFSET(0)].name as artist_name,
        artists[OFFSET(0)].external_urls.spotify as artist_url,
        artists[OFFSET(0)].id as artist_id,
        COUNT(*) as total_plays,
        SUM(COALESCE(actual_duration_ms, track.duration_ms, 0)) / 1000.0 / 60 as total_minutes
    FROM filtered_plays
    GROUP BY 1, 2, 3
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
    FROM filtered_plays
    WHERE album.images IS NOT NULL
      AND ARRAY_LENGTH(album.images) > 0
    QUALIFY ROW_NUMBER() OVER (PARTITION BY artists[OFFSET(0)].id ORDER BY played_at DESC) = 1
)

SELECT
    ROW_NUMBER() OVER (ORDER BY s.total_minutes DESC) as rank,
    s.artist_name,
    CONCAT(
        CAST(FLOOR(s.total_minutes / 60) AS STRING), 'h',
        LPAD(CAST(MOD(CAST(s.total_minutes AS INT64), 60) AS STRING), 2, '0')
    ) as time_listened,
    s.total_plays,
    s.artist_url,
    COALESCE(
        i.artist_image_url, 
        CONCAT('https://via.placeholder.com/300x300/1DB954/FFFFFF?text=', SUBSTR(s.artist_name, 1, 1))
    ) as artist_image_url
FROM artist_stats s
LEFT JOIN artist_image i ON s.artist_id = i.artist_id
ORDER BY s.total_minutes DESC
LIMIT 10
