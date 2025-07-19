{{ config(dataset=get_schema('hub'), materialized='view') }}

SELECT DISTINCT
    track.id AS track_id,
    track.name AS track_name,
    track.duration_ms,
    track.explicit,
    track.album.id AS album_id
FROM {{ ref('lake_spotify__svc_tracks') }}
WHERE track.id IS NOT NULL
