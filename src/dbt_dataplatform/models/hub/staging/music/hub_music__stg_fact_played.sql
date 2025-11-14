{{
  config(
      tags=['music', 'hub']
  )
}}

SELECT
    PLAYEDAT,
    TRACKID,
    CONTEXTTYPE,
    CONTEXTURI,
    CONTEXTHREF,
    CONTEXTEXTERNALURL
FROM
    {{ ref('lake_spotify__svc_recently_played') }}
