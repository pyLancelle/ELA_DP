{{
  config(
      tags=['music', 'hub']
  )
}}

SELECT DISTINCT
    TRACKID,
    ARTIST.ARTISTID,
    ARTIST_OFFSET AS ARTIST_POSITION,
    CASE
        WHEN ARTIST_OFFSET = 0 THEN 'primary'
        ELSE 'featuring'
    END AS ARTIST_ROLE
FROM
    {{ ref('lake_spotify__svc_recently_played') }},
    UNNEST(ARTISTS) AS ARTIST WITH OFFSET AS ARTIST_OFFSET
