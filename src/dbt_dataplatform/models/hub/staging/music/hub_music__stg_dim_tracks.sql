{{
  config(
      tags=['music', 'hub']
  )
}}

WITH
stats AS (
    SELECT
        trackid,
        count(*) AS play_count,
        sum(trackdurationms) AS total_listen_duration,
        min(playedat) AS first_played_at,
        max(playedat) AS last_played_at
    FROM {{ ref('lake_spotify__svc_recently_played') }}
    GROUP BY trackid
),

latest_version AS (
    SELECT
        trackid,
        trackname,
        trackuri,
        trackexternalurl,
        trackdurationms,
        trackpopularity,
        trackexplicit,
        trackhref,
        artists,
        albumid
    FROM {{ ref('lake_spotify__svc_recently_played') }}
    QUALIFY row_number() OVER (PARTITION BY trackid ORDER BY playedat DESC) = 1
)

SELECT DISTINCT
    latest_version.trackid,
    latest_version.trackname,
    latest_version.trackuri,
    latest_version.trackexternalurl,
    latest_version.trackdurationms,
    latest_version.trackpopularity,
    latest_version.trackexplicit,
    latest_version.trackhref,
    latest_version.albumid,
    stats.play_count,
    stats.total_listen_duration,
    stats.first_played_at,
    stats.last_played_at,
    string_agg(artist.artistname, ', ' ORDER BY artist_offset)
        AS all_artist_names
FROM
    latest_version,
    unnest(artists) AS artist WITH OFFSET AS artist_offset
LEFT JOIN stats ON latest_version.trackid = stats.trackid
GROUP BY ALL
