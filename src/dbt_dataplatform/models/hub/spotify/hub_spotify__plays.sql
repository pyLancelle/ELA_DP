{{ config(dataset=get_schema('hub'), materialized='view', tags=["hub", "spotify"]) }}

-- Hub model for Spotify plays (listening events)
-- Currently only from recently played data

SELECT
    played_at,
    JSON_VALUE(track, '$.id') AS track_id,
    JSON_VALUE(track, '$.name') AS track_name,
    JSON_VALUE(artist, '$.id') AS artist_id,
    JSON_VALUE(artist, '$.name') AS artist_name,
    JSON_VALUE(track, '$.album.id') AS album_id,
    JSON_VALUE(track, '$.album.name') AS album_name,
    JSON_VALUE(track, '$.album.release_date') AS album_release_date,
    JSON_VALUE(context, '$.type') AS context_type,
    JSON_VALUE(context, '$.uri') AS context_uri
FROM {{ ref('lake_spotify__svc_recently_played') }},
UNNEST(JSON_QUERY_ARRAY(track, '$.artists')) AS artist
WHERE JSON_VALUE(track, '$.id') IS NOT NULL
  AND JSON_VALUE(artist, '$.id') IS NOT NULL
