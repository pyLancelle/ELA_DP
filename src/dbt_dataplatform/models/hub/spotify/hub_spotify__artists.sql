{{ config(dataset=get_schema('hub'), materialized='view') }}

SELECT DISTINCT
    artist.id AS artist_id,
    artist.name AS artist_name
FROM {{ ref('lake_spotify__svc_tracks') }},
UNNEST(track.artists) AS artist
WHERE artist.id IS NOT NULL
