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
    CONTEXTEXTERNALURL,
    _dp_inserted_at
FROM
    {{ ref('lake_spotify__svc_recently_played') }}
