SELECT DISTINCT
    trackId,
    artist.artistId,
    artist_offset+1 as artist_position
FROM
    {{ ref('lake_spotify__svc_recently_played') }},
	UNNEST(artists) as artist WITH OFFSET as artist_offset