{{
  config(
      tags=['music', 'hub']
  )
}}

SELECT DISTINCT
    ALBUMID,
    ARTIST.ARTISTID,
    (SELECT MAX(_dp_inserted_at) FROM {{ ref('lake_spotify__svc_recently_played') }}) AS max__dp_inserted_at
FROM
    {{ ref('lake_spotify__svc_recently_played') }},
    UNNEST(ALBUMARTISTS) AS ARTIST WITH OFFSET AS ARTIST_OFFSET
