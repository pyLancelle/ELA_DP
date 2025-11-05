{{
  config(
      tags=['music', 'hub']
  )
}}

SELECT DISTINCT
    ALBUMID,
    ARTIST.ARTISTID
FROM
    {{ ref('lake_spotify__svc_recently_played') }},
    UNNEST(ALBUMARTISTS) AS ARTIST WITH OFFSET AS ARTIST_OFFSET
