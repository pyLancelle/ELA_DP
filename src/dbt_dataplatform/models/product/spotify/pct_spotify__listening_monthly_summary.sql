{{ config(
    dataset=get_schema('product'),
    materialized='view',
    tags=["product", "spotify"]
) }}

-- Monthly recap of Spotify listening activity using actual listened duration

WITH plays AS (
    SELECT
        DATE_TRUNC(DATE(played_at), MONTH) AS month_start,
        played_at,
        track_id,
        track_name,
        track.external_urls.spotify AS track_url,
        track.duration_ms AS track_duration_ms,
        COALESCE(actual_duration_ms, track.duration_ms, 0) AS listened_ms,
        ARRAY_LENGTH(artists) AS artist_count,
        artists[SAFE_OFFSET(0)].id AS primary_artist_id,
        artists[SAFE_OFFSET(0)].name AS primary_artist_name,
        artists[SAFE_OFFSET(0)].external_urls.spotify AS primary_artist_url
    FROM {{ ref('hub_spotify__recently_played') }}
    WHERE played_at IS NOT NULL
),
monthly_stats AS (
    SELECT
        month_start,
        COUNT(*) AS total_plays,
        SUM(listened_ms) AS total_listened_ms,
        COUNT(DISTINCT track_id) AS distinct_tracks,
        COUNT(DISTINCT primary_artist_id) AS distinct_primary_artists
    FROM plays
    GROUP BY month_start
),
track_by_month AS (
    SELECT
        month_start,
        track_id,
        ANY_VALUE(track_name) AS track_name,
        ANY_VALUE(track_url) AS track_url,
        ANY_VALUE(primary_artist_name) AS primary_artist_name,
        ANY_VALUE(primary_artist_id) AS primary_artist_id,
        ANY_VALUE(primary_artist_url) AS primary_artist_url,
        COUNT(*) AS play_count,
        SUM(listened_ms) AS listened_ms
    FROM plays
    GROUP BY month_start, track_id
),
ranked_tracks AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY month_start
            ORDER BY listened_ms DESC, track_name
        ) AS track_rank
    FROM track_by_month
),
artist_by_month AS (
    SELECT
        month_start,
        primary_artist_id,
        ANY_VALUE(primary_artist_name) AS primary_artist_name,
        ANY_VALUE(primary_artist_url) AS primary_artist_url,
        COUNT(*) AS play_count,
        SUM(listened_ms) AS listened_ms
    FROM plays
    WHERE primary_artist_id IS NOT NULL
    GROUP BY month_start, primary_artist_id
),
ranked_artists AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY month_start
            ORDER BY listened_ms DESC, primary_artist_name
        ) AS artist_rank
    FROM artist_by_month
)
SELECT
    m.month_start,
    FORMAT_DATE('%Y-%m', m.month_start) AS month_label,
    m.total_plays,
    m.distinct_tracks,
    m.distinct_primary_artists,
    m.total_listened_ms,
    SAFE_DIVIDE(m.total_listened_ms, 1000.0) AS total_listened_seconds,
    SAFE_DIVIDE(m.total_listened_ms, 1000.0 * 60) AS total_listened_minutes,
    SAFE_DIVIDE(m.total_listened_ms, 1000.0 * 60 * 60) AS total_listened_hours,
    STRUCT(
        t.track_id,
        t.track_name,
        t.track_url,
        t.primary_artist_id,
        t.primary_artist_name,
        t.primary_artist_url,
        t.play_count,
        t.listened_ms,
        SAFE_DIVIDE(t.listened_ms, 1000.0 * 60) AS listened_minutes
    ) AS top_track,
    STRUCT(
        a.primary_artist_id,
        a.primary_artist_name,
        a.primary_artist_url,
        a.play_count,
        a.listened_ms,
        SAFE_DIVIDE(a.listened_ms, 1000.0 * 60) AS listened_minutes
    ) AS top_artist
FROM monthly_stats AS m
LEFT JOIN ranked_tracks AS t
    ON m.month_start = t.month_start
   AND t.track_rank = 1
LEFT JOIN ranked_artists AS a
    ON m.month_start = a.month_start
   AND a.artist_rank = 1
ORDER BY m.month_start DESC
