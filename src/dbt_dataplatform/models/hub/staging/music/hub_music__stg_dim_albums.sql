WITH 
stats AS (
    SELECT
        albumId,
        count(*) AS play_count,
        sum(trackDurationMs) AS total_listen_duration,
        min(playedAt) AS first_played_at,
        max(playedAt) AS last_played_at
    FROM {{ ref('lake_spotify__svc_recently_played') }}
    GROUP BY albumId
),
latest_version AS (
    SELECT DISTINCT
        albumId,
        albumName,
        album_type,
        albumReleaseDate,
        albumReleaseDatePrecision,
        albumTotalTracks,
        albumUri,
        albumExternalUrl,
        albumHref,
        albumType,
        albumImageUrl,
        albumImageHeight,
        albumImageWidth,
        albumArtists
    FROM
        {{ ref('lake_spotify__svc_recently_played') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY albumId ORDER BY playedAt DESC) = 1
)
SELECT
    latest_version.albumId,
    latest_version.albumName,
    latest_version.album_type,
    latest_version.albumReleaseDate,
    latest_version.albumReleaseDatePrecision,
    latest_version.albumTotalTracks,
    latest_version.albumUri,
    latest_version.albumExternalUrl,
    latest_version.albumHref,
    latest_version.albumType,
    latest_version.albumImageUrl,
    latest_version.albumImageHeight,
    latest_version.albumImageWidth,
    STRING_AGG(albumArtist.artistName, ', ' ORDER BY artist_offset) as all_artist_names,
    stats.play_count,
    stats.total_listen_duration,
    stats.first_played_at,
    stats.last_played_at
FROM
    latest_version,
    UNNEST(albumArtists) as albumArtist WITH OFFSET as artist_offset
    LEFT JOIN stats USING (albumId)
GROUP BY ALL