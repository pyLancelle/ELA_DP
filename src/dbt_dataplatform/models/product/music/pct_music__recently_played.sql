{{
    config(
        materialized='table',
        tags=['music', 'product']
    )
}}

WITH primary_artists AS (
    SELECT
        bridge.trackId,
        bridge.artistId,
        artists.artistName,
        artists.artistExternalUrl
    FROM {{ ref('hub_music__svc_bridge_tracks_artists') }} AS bridge
    INNER JOIN {{ ref('hub_music__svc_dim_artists') }} AS artists
        ON bridge.artistId = artists.artistId
    WHERE bridge.artist_role = 'primary'
)

SELECT
    fact.playedAt AS played_at,
    DATE(fact.playedAt) AS played_date,
    TIME(fact.playedAt) AS played_time,
    tracks.trackId AS track_id,
    tracks.trackUri AS track_uri,
    tracks.trackName AS track_name,
    tracks.trackDurationMs AS track_duration_ms,
    tracks.trackExternalUrl AS track_external_url,
    primary_artists.artistId AS artist_id,
    primary_artists.artistName AS artist_name,
    primary_artists.artistExternalUrl AS artist_external_url,
    albums.albumId AS album_id,
    albums.albumName AS album_name,
    albums.albumImageUrl AS album_image_url,
    albums.albumExternalUrl AS album_external_url
FROM {{ ref('hub_music__svc_fact_played') }} AS fact
INNER JOIN {{ ref('hub_music__svc_dim_tracks') }} AS tracks
    ON fact.trackId = tracks.trackId
INNER JOIN {{ ref('hub_music__svc_dim_albums') }} AS albums
    ON tracks.albumId = albums.albumId
LEFT JOIN primary_artists
    ON tracks.trackId = primary_artists.trackId
ORDER BY fact.playedAt DESC
