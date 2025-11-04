WITH latest_version AS (
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
    albumId
FROM
    latest_version,
    UNNEST(artists) as artist WITH OFFSET as artist_offset
GROUP BY ALL