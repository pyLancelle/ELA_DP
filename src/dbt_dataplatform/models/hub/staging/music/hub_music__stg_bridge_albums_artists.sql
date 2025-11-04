SELECT DISTINCT
    albumId,
    artist.artistId
FROM
    {{ ref('lake_spotify__svc_recently_played') }},
	UNNEST(albumArtists) as artist WITH OFFSET as artist_offset