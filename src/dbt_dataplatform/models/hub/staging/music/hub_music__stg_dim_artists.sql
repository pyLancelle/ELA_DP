{{
  config(
      tags=['music', 'hub']
  )
}}

WITH
stats AS (
    SELECT
        artist.artistid,
        count(*) AS play_count,
        sum(trackdurationms) AS total_listen_duration,
        min(playedat) AS first_played_at,
        max(playedat) AS last_played_at
    FROM {{ ref('lake_spotify__svc_recently_played') }},
        unnest(artists) AS artist
    GROUP BY artistid
),

latest_artist AS (
    SELECT DISTINCT
        artist.artistid,
        artist.artistname,
        artist.artisturi,
        artist.artisttype,
        artist.artistexternalurl,
        artist.artisthref
    FROM
        {{ ref('lake_spotify__svc_recently_played') }},
        unnest(artists) AS artist
    QUALIFY
        row_number() OVER (PARTITION BY artist.artistid ORDER BY playedat DESC)
        = 1
)

SELECT
    latest_artist.artistid,
    latest_artist.artistname,
    latest_artist.artisturi,
    latest_artist.artisttype,
    latest_artist.artistexternalurl,
    latest_artist.artisthref,
    stats.play_count,
    stats.total_listen_duration,
    stats.first_played_at,
    stats.last_played_at
FROM latest_artist
LEFT JOIN stats ON latest_artist.artistid = stats.artistid
