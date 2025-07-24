{{ config(dataset=get_schema('hub'), materialized='view', tags=["hub", "spotify"]) }}

-- Hub model for Spotify albums from multiple sources
-- Combines albums from tracks, saved tracks, saved albums, and top tracks

WITH albums_from_recently_played AS (
    SELECT DISTINCT
        JSON_VALUE(track, '$.album.id') AS album_id,
        JSON_VALUE(track, '$.album.name') AS album_name,
        JSON_VALUE(track, '$.album.release_date') AS release_date,
        'recently_played' AS source_type
    FROM {{ ref('lake_spotify__svc_recently_played') }}
    WHERE JSON_VALUE(track, '$.album.id') IS NOT NULL
),

albums_from_saved_tracks AS (
    SELECT DISTINCT
        JSON_VALUE(track, '$.album.id') AS album_id,
        JSON_VALUE(track, '$.album.name') AS album_name,
        JSON_VALUE(track, '$.album.release_date') AS release_date,
        'saved_tracks' AS source_type
    FROM {{ ref('lake_spotify__svc_saved_tracks') }}
    WHERE JSON_VALUE(track, '$.album.id') IS NOT NULL
),

albums_from_saved_albums AS (
    SELECT DISTINCT
        JSON_VALUE(album, '$.id') AS album_id,
        JSON_VALUE(album, '$.name') AS album_name,
        JSON_VALUE(album, '$.release_date') AS release_date,
        'saved_albums' AS source_type
    FROM {{ ref('lake_spotify__svc_saved_albums') }}
    WHERE JSON_VALUE(album, '$.id') IS NOT NULL
),

albums_from_top_tracks AS (
    SELECT DISTINCT
        JSON_VALUE(track_data, '$.album.id') AS album_id,
        JSON_VALUE(track_data, '$.album.name') AS album_name,
        JSON_VALUE(track_data, '$.album.release_date') AS release_date,
        'top_tracks' AS source_type
    FROM {{ ref('lake_spotify__svc_top_tracks') }}
    WHERE JSON_VALUE(track_data, '$.album.id') IS NOT NULL
),

all_albums AS (
    SELECT * FROM albums_from_recently_played
    UNION ALL
    SELECT * FROM albums_from_saved_tracks
    UNION ALL
    SELECT * FROM albums_from_saved_albums
    UNION ALL
    SELECT * FROM albums_from_top_tracks
)

SELECT DISTINCT
    album_id,
    album_name,
    release_date,
    ARRAY_AGG(DISTINCT source_type) AS source_types
FROM all_albums
WHERE album_id IS NOT NULL
GROUP BY album_id, album_name, release_date
