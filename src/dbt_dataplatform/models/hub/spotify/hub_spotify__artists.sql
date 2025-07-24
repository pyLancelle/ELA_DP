{{ config(dataset=get_schema('hub'), materialized='view', tags=["hub", "spotify"]) }}

-- Hub model for Spotify artists from multiple sources
-- Combines artists from tracks, saved tracks, and top artists

WITH artists_from_recently_played AS (
    SELECT DISTINCT
        JSON_VALUE(artist, '$.id') AS artist_id,
        JSON_VALUE(artist, '$.name') AS artist_name,
        'recently_played' AS source_type
    FROM {{ ref('lake_spotify__svc_recently_played') }},
    UNNEST(JSON_QUERY_ARRAY(track, '$.artists')) AS artist
    WHERE JSON_VALUE(artist, '$.id') IS NOT NULL
),

artists_from_saved_tracks AS (
    SELECT DISTINCT
        JSON_VALUE(artist, '$.id') AS artist_id,
        JSON_VALUE(artist, '$.name') AS artist_name,
        'saved_tracks' AS source_type
    FROM {{ ref('lake_spotify__svc_saved_tracks') }},
    UNNEST(JSON_QUERY_ARRAY(track, '$.artists')) AS artist
    WHERE JSON_VALUE(artist, '$.id') IS NOT NULL
),

artists_from_top_artists AS (
    SELECT DISTINCT
        artist_id,
        artist_name,
        'top_artists' AS source_type
    FROM {{ ref('lake_spotify__svc_top_artists') }}
    WHERE artist_id IS NOT NULL
),

artists_from_top_tracks AS (
    SELECT DISTINCT
        JSON_VALUE(artist, '$.id') AS artist_id,
        JSON_VALUE(artist, '$.name') AS artist_name,
        'top_tracks' AS source_type
    FROM {{ ref('lake_spotify__svc_top_tracks') }},
    UNNEST(JSON_QUERY_ARRAY(track_data, '$.artists')) AS artist
    WHERE JSON_VALUE(artist, '$.id') IS NOT NULL
),

all_artists AS (
    SELECT * FROM artists_from_recently_played
    UNION ALL
    SELECT * FROM artists_from_saved_tracks
    UNION ALL
    SELECT * FROM artists_from_top_artists
    UNION ALL
    SELECT * FROM artists_from_top_tracks
)

SELECT DISTINCT
    artist_id,
    artist_name,
    ARRAY_AGG(DISTINCT source_type) AS source_types
FROM all_artists
WHERE artist_id IS NOT NULL
GROUP BY artist_id, artist_name
