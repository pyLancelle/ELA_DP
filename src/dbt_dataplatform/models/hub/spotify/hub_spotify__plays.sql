{{ config(dataset=get_schema('hub'), materialized='view') }}

SELECT
    played_at,
    track.id AS track_id,
    track.name AS track_name,
    artist.id AS artist_id,
    artist.name AS artist_name,
    track.album.id AS album_id,
    track.album.name AS album_name,
    track.album.release_date AS album_release_date
FROM {{ ref('lake_spotify__svc_tracks') }},
UNNEST(track.artists) AS artist
