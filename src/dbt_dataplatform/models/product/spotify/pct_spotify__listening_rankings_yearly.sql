{{ config(
    dataset=get_schema('product'),
    materialized='view',
    tags=["product", "spotify", "rankings"]
) }}

-- Annual top rankings for Spotify listening metrics using actual listened duration

WITH plays AS (
    SELECT
        DATE_TRUNC(DATE(played_at), YEAR) AS period_start,
        track_id,
        track_name,
        track.external_urls.spotify AS track_url,
        album.id AS album_id,
        album.name AS album_name,
        album.external_urls.spotify AS album_url,
        artists[SAFE_OFFSET(0)].id AS primary_artist_id,
        artists[SAFE_OFFSET(0)].name AS primary_artist_name,
        artists[SAFE_OFFSET(0)].external_urls.spotify AS primary_artist_url,
        COALESCE(actual_duration_ms, track.duration_ms, 0) AS listened_ms
    FROM {{ ref('hub_spotify__recently_played') }}
    WHERE played_at IS NOT NULL
),
track_rankings AS (
    SELECT
        'top_track' AS perimeter,
        period_start,
        track_id,
        ANY_VALUE(track_name) AS track_name,
        ANY_VALUE(track_url) AS track_url,
        ANY_VALUE(album_id) AS album_id,
        ANY_VALUE(album_name) AS album_name,
        ANY_VALUE(album_url) AS album_url,
        ANY_VALUE(primary_artist_id) AS primary_artist_id,
        ANY_VALUE(primary_artist_name) AS primary_artist_name,
        ANY_VALUE(primary_artist_url) AS primary_artist_url,
        COUNT(*) AS play_count,
        SUM(listened_ms) AS listened_ms
    FROM plays
    WHERE track_id IS NOT NULL
      AND period_start IS NOT NULL
    GROUP BY 1, 2, 3
),
artist_rankings AS (
    SELECT
        'top_artist' AS perimeter,
        period_start,
        primary_artist_id AS artist_id,
        ANY_VALUE(primary_artist_name) AS artist_name,
        ANY_VALUE(primary_artist_url) AS artist_url,
        COUNT(*) AS play_count,
        SUM(listened_ms) AS listened_ms
    FROM plays
    WHERE primary_artist_id IS NOT NULL
      AND period_start IS NOT NULL
    GROUP BY 1, 2, 3
),
album_rankings AS (
    SELECT
        'top_album' AS perimeter,
        period_start,
        album_id,
        ANY_VALUE(album_name) AS album_name,
        ANY_VALUE(album_url) AS album_url,
        ANY_VALUE(primary_artist_id) AS primary_artist_id,
        ANY_VALUE(primary_artist_name) AS primary_artist_name,
        ANY_VALUE(primary_artist_url) AS primary_artist_url,
        COUNT(*) AS play_count,
        SUM(listened_ms) AS listened_ms
    FROM plays
    WHERE album_id IS NOT NULL
      AND period_start IS NOT NULL
    GROUP BY 1, 2, 3
),
combined AS (
    SELECT
        perimeter,
        period_start,
        track_id,
        track_name,
        track_url,
        album_id,
        album_name,
        album_url,
        primary_artist_id,
        primary_artist_name,
        primary_artist_url,
        play_count,
        listened_ms
    FROM track_rankings

    UNION ALL

    SELECT
        perimeter,
        period_start,
        NULL AS track_id,
        NULL AS track_name,
        NULL AS track_url,
        NULL AS album_id,
        NULL AS album_name,
        NULL AS album_url,
        artist_id AS primary_artist_id,
        artist_name AS primary_artist_name,
        artist_url AS primary_artist_url,
        play_count,
        listened_ms
    FROM artist_rankings

    UNION ALL

    SELECT
        perimeter,
        period_start,
        NULL AS track_id,
        NULL AS track_name,
        NULL AS track_url,
        album_id,
        album_name,
        album_url,
        primary_artist_id,
        primary_artist_name,
        primary_artist_url,
        play_count,
        listened_ms
    FROM album_rankings
),
ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY perimeter, period_start
            ORDER BY listened_ms DESC, play_count DESC, COALESCE(track_name, album_name, primary_artist_name)
        ) AS rank
    FROM combined
)
SELECT
    perimeter,
    period_start,
    rank,
    primary_artist_id AS artist_id,
    primary_artist_name AS artist_name,
    primary_artist_url AS artist_url,
    album_id,
    album_name,
    album_url,
    track_id,
    track_name,
    track_url,
    play_count,
    listened_ms,
    SAFE_DIVIDE(listened_ms, 1000.0) AS listened_seconds,
    SAFE_DIVIDE(listened_ms, 1000.0 * 60) AS listened_minutes
FROM ranked
WHERE rank <= 20
ORDER BY period_start DESC, perimeter, rank
