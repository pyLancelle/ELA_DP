{{
  config(
      tags=['music', 'hub']
  )
}}

SELECT
    PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', PLAYEDAT, 'Europe/Paris')) AS PLAYEDAT,
    TRACKID,
    CONTEXTTYPE,
    CONTEXTURI,
    CONTEXTHREF,
    CONTEXTEXTERNALURL
FROM
    {{ ref('lake_spotify__svc_recently_played') }}
