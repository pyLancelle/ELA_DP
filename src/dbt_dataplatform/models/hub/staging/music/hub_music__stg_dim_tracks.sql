WITH 
stats AS (
    SELECT
        trackId,
        count(*) AS play_count,
        sum(trackDurationMs) AS total_listen_duration,
        min(playedAt) AS first_played_at,
        max(playedAt) AS last_played_at
    FROM {{ ref('lake_spotify__svc_recently_played') }}
    GROUP BY trackId
),
latest_version AS (
	SELECT 
		trackId,
	    trackName,
	    trackUri,
	    trackExternalUrl,
	    trackDurationMs,
	    trackPopularity,
	    trackExplicit,
	    trackHref,
		artists,
	    albumId
	FROM {{ ref('lake_spotify__svc_recently_played') }}
	QUALIFY ROW_NUMBER() OVER (PARTITION BY trackId ORDER BY playedAt DESC) = 1
)
SELECT DISTINCT
    trackId,
    trackName,
    trackUri,
    trackExternalUrl,
    trackDurationMs,
    trackPopularity,
    trackExplicit,
    trackHref,
    STRING_AGG(artist.artistName, ', ' ORDER BY artist_offset) as all_artist_names,
    albumId,
    stats.play_count,
    stats.total_listen_duration,
    stats.first_played_at,
    stats.last_played_at
FROM
    latest_version,
    UNNEST(artists) as artist WITH OFFSET as artist_offset
    LEFT JOIN stats USING (trackId)
GROUP BY ALL