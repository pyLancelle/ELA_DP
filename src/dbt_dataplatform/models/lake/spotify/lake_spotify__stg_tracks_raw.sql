SELECT
    context.external_urls.spotify,
    context.href,
    context.type,
    context.uri,
    played_at,
    track.album.album_type,
    track.album.artists
FROM {{ source('spotify', 'staging_spotify_raw') }}