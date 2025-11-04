WITH 
    stats AS (
        SELECT
            artist.artistId,
            count(*) AS play_count,
            sum(trackDurationMs) AS total_listen_duration,
            min(playedAt) AS first_played_at,
            max(playedAt) AS last_played_at
        FROM {{ ref('lake_spotify__svc_recently_played') }},
        UNNEST(artists) as artist
        GROUP BY artistId
    ),
    latest_artist AS (
        SELECT DISTINCT
            artist.artistId,
            artist.artistName,
            artist.artistUri,
            artist.artistType,
            artist.artistExternalUrl,
            artist.artistHref
        FROM
            {{ ref('lake_spotify__svc_recently_played') }},
            UNNEST(artists) as artist
        QUALIFY ROW_NUMBER() OVER (PARTITION BY artist.artistId ORDER BY playedAt DESC) = 1
    )
SELECT
    latest_artist.artistId,
    latest_artist.artistName,
    latest_artist.artistUri,
    latest_artist.artistType,
    latest_artist.artistExternalUrl,
    latest_artist.artistHref,
    stats.play_count,
    stats.total_listen_duration,
    stats.first_played_at,
    stats.last_played_at
FROM latest_artist
LEFT JOIN stats USING (artistId)