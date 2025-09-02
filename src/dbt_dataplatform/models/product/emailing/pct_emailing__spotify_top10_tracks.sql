{{ config(
    dataset=get_schema('product'), 
    materialized='view', 
    tags=["product", "spotify"]
) }}

-- Top 10 Tracks All Time - Sorted by total listening time

WITH track_stats AS (
    SELECT
        track_name,
        artists[OFFSET(0)].name as artist_name,
        track.external_urls.spotify as track_url,
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
    GROUP BY 1, 2, 3, 4
)

SELECT
    ROW_NUMBER() OVER (ORDER BY total_minutes DESC) as rank,
    artist_name,
    track_name,
    CONCAT(
        CAST(FLOOR(total_minutes / 60) AS STRING), 'h',
        LPAD(CAST(MOD(CAST(total_minutes AS INT64), 60) AS STRING), 2, '0')
    ) as time_listened,
    total_plays,
    total_minutes,
    track_url,
    album_cover_url
FROM track_stats
ORDER BY total_minutes DESC
LIMIT 10