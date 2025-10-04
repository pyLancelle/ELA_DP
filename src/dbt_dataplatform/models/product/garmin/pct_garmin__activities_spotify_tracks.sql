{{ config(
    dataset=get_schema('product'),
    materialized='view',
    tags=["product", "garmin", "spotify"]
) }}

WITH activities AS (
    SELECT
        activity_id,
        activity_name,
        activity_type.type_key AS activity_type_key,
        activity_type.type_id AS activity_type_id,
        start_time_gmt,
        start_time_local,
        DATE(start_time_gmt) AS activity_date_utc,
        DATE(start_time_local) AS activity_date_local,
        COALESCE(
            end_time_gmt,
            TIMESTAMP_ADD(
                start_time_gmt,
                INTERVAL COALESCE(SAFE_CAST(duration_seconds AS INT64), 0) SECOND
            )
        ) AS end_time_gmt
    FROM {{ ref('hub_garmin__activities') }}
    WHERE start_time_gmt IS NOT NULL
),
spotify AS (
    SELECT
        play_id,
        played_at,
        expected_end_at,
        next_played_at,
        actual_end_at,
        actual_duration_ms,
        actual_duration_seconds,
        track_id,
        track_name,
        track.duration_ms AS track_duration_ms,
        album.name AS album_name,
        album.uri AS album_uri,
        context.type AS context_type,
        context.uri AS context_uri,
        context.external_urls.spotify AS context_url,
        ARRAY(
            SELECT artist.name
            FROM UNNEST(artists) AS artist
            WHERE artist.name IS NOT NULL
        ) AS artist_names
    FROM {{ ref('hub_spotify__recently_played') }}
    WHERE played_at IS NOT NULL
)
SELECT
    a.activity_id,
    a.activity_name,
    a.activity_type_key,
    a.activity_type_id,
    a.activity_date_utc,
    a.activity_date_local,
    a.start_time_gmt,
    a.end_time_gmt,
    a.start_time_local,
    COUNT(s.play_id) AS tracks_played_count,
    SUM(
        IF(
            s.play_id IS NOT NULL,
            COALESCE(s.actual_duration_ms, s.track_duration_ms, 0),
            0
        )
    ) AS tracks_total_duration_ms,
    ARRAY_AGG(
        IF(
            s.play_id IS NULL,
            NULL,
            STRUCT(
                s.play_id AS play_id,
                s.played_at AS played_at,
                s.expected_end_at AS expected_end_at,
                s.next_played_at AS next_played_at,
                s.actual_end_at AS actual_end_at,
                s.actual_duration_ms AS actual_duration_ms,
                s.actual_duration_seconds AS actual_duration_seconds,
                TIMESTAMP_DIFF(s.played_at, a.start_time_gmt, SECOND) AS seconds_since_activity_start,
                TIMESTAMP_DIFF(a.end_time_gmt, s.played_at, SECOND) AS seconds_until_activity_end,
                s.track_id AS track_id,
                s.track_name AS track_name,
                ARRAY_TO_STRING(s.artist_names, ', ') AS artist_names,
                s.artist_names AS artist_names_array,
                s.album_name AS album_name,
                s.album_uri AS album_uri,
                s.track_duration_ms AS track_duration_ms,
                s.context_type AS context_type,
                s.context_uri AS context_uri,
                s.context_url AS context_url
            )
        ) IGNORE NULLS
        ORDER BY s.played_at
    ) AS played_tracks
FROM activities AS a
LEFT JOIN spotify AS s
  ON s.played_at BETWEEN a.start_time_gmt AND a.end_time_gmt
GROUP BY
    a.activity_id,
    a.activity_name,
    a.activity_type_key,
    a.activity_type_id,
    a.activity_date_utc,
    a.activity_date_local,
    a.start_time_gmt,
    a.end_time_gmt,
    a.start_time_local
ORDER BY a.start_time_gmt
