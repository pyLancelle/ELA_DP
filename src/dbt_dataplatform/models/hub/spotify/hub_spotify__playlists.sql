{{ config(dataset=get_schema('hub'), materialized='view', tags=["hub", "spotify"]) }}

-- Hub model for Spotify playlists from multiple sources
-- Combines playlists from dedicated playlists data and context from recently played

WITH playlists_from_dedicated AS (
    SELECT DISTINCT
        playlist_id,
        playlist_name,
        is_public,
        is_collaborative,
        'playlists' AS source_type
    FROM {{ ref('lake_spotify__svc_playlists') }}
    WHERE playlist_id IS NOT NULL
),

playlists_from_context AS (
    SELECT DISTINCT
        REGEXP_EXTRACT(JSON_VALUE(context, '$.uri'), r'spotify:playlist:(.+)') AS playlist_id,
        NULL AS playlist_name,
        NULL AS is_public,
        NULL AS is_collaborative,
        'recently_played_context' AS source_type
    FROM {{ ref('lake_spotify__svc_recently_played') }}
    WHERE JSON_VALUE(context, '$.type') = 'playlist'
      AND JSON_VALUE(context, '$.uri') IS NOT NULL
),

all_playlists AS (
    SELECT * FROM playlists_from_dedicated
    UNION ALL
    SELECT * FROM playlists_from_context
)

SELECT DISTINCT
    playlist_id,
    playlist_name,
    is_public,
    is_collaborative,
    ARRAY_AGG(DISTINCT source_type) AS source_types
FROM all_playlists
WHERE playlist_id IS NOT NULL
GROUP BY playlist_id, playlist_name, is_public, is_collaborative
