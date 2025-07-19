{{ config(dataset=get_schema('hub'), materialized='view') }}

SELECT DISTINCT
    REGEXP_EXTRACT(context.uri, r'spotify:playlist:(.+)') AS playlist_id,
    context.href AS playlist_href
FROM {{ ref('lake_spotify__svc_tracks') }}
WHERE context.type = 'playlist'
