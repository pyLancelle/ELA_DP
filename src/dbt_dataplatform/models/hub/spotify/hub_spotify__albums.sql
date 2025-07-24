{{ config(dataset=get_schema('hub'), materialized='view', tags=["hub", "spotify"]) }}

SELECT DISTINCT
    track.album.id AS album_id,
    track.album.name AS album_name,
    track.album.release_date AS release_date
FROM {{ ref('lake_spotify__svc_tracks') }}
WHERE track.album.id IS NOT NULL
