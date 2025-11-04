SELECT DISTINCT
    trackId,
    artist.artistId,
    artist_offset as artist_position,
    CASE 
        WHEN artist_offset = 0 THEN 'primary'
        ELSE 'featuring'
    END as artist_role
FROM
    {{ ref('lake_spotify__svc_recently_played') }},
	UNNEST(artists) as artist WITH OFFSET as artist_offset