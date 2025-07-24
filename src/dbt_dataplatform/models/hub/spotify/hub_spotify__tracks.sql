{{ config(dataset=get_schema('hub'), materialized='view', tags=["hub", "spotify"]) }}

-- Hub model for Spotify tracks from multiple sources
-- Combines tracks from recently played, saved tracks, and top tracks

WITH tracks_from_recently_played AS (
    SELECT DISTINCT
        JSON_VALUE(track, '$.id') AS track_id,
        JSON_VALUE(track, '$.name') AS track_name,
        CAST(JSON_VALUE(track, '$.duration_ms') AS INT64) AS duration_ms,
        CAST(JSON_VALUE(track, '$.explicit') AS BOOL) AS explicit,
        JSON_VALUE(track, '$.album.id') AS album_id,
        'recently_played' AS source_type
    FROM {{ ref('lake_spotify__svc_recently_played') }}
    WHERE JSON_VALUE(track, '$.id') IS NOT NULL
),

tracks_from_saved AS (
    SELECT DISTINCT
        JSON_VALUE(track, '$.id') AS track_id,
        JSON_VALUE(track, '$.name') AS track_name,
        CAST(JSON_VALUE(track, '$.duration_ms') AS INT64) AS duration_ms,
        CAST(JSON_VALUE(track, '$.explicit') AS BOOL) AS explicit,
        JSON_VALUE(track, '$.album.id') AS album_id,
        'saved_tracks' AS source_type
    FROM {{ ref('lake_spotify__svc_saved_tracks') }}
    WHERE JSON_VALUE(track, '$.id') IS NOT NULL
),

tracks_from_top AS (
    SELECT DISTINCT
        track_id,
        track_name,
        duration_ms,
        explicit,
        JSON_VALUE(track_data, '$.album.id') AS album_id,
        'top_tracks' AS source_type
    FROM {{ ref('lake_spotify__svc_top_tracks') }}
    WHERE track_id IS NOT NULL
),

all_tracks AS (
    SELECT * FROM tracks_from_recently_played
    UNION ALL
    SELECT * FROM tracks_from_saved
    UNION ALL
    SELECT * FROM tracks_from_top
)

SELECT DISTINCT
    track_id,
    track_name,
    duration_ms,
    explicit,
    album_id,
    ARRAY_AGG(DISTINCT source_type) AS source_types
FROM all_tracks
WHERE track_id IS NOT NULL
GROUP BY track_id, track_name, duration_ms, explicit, album_id
