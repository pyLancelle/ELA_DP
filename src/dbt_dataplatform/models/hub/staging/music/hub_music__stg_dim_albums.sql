{{
  config(
      tags=['music', 'hub']
  )
}}

WITH
stats AS (
    SELECT
        albumid,
        count(*) AS play_count,
        sum(trackdurationms) AS total_listen_duration,
        min(playedat) AS first_played_at,
        max(playedat) AS last_played_at
    FROM {{ ref('lake_spotify__svc_recently_played') }}
    GROUP BY albumid
),

latest_version AS (
    SELECT DISTINCT
        albumid,
        albumname,
        album_type,
        albumreleasedate,
        albumreleasedateprecision,
        albumtotaltracks,
        albumuri,
        albumexternalurl,
        albumhref,
        albumtype,
        albumimageurl,
        albumimageheight,
        albumimagewidth,
        albumartists
    FROM
        {{ ref('lake_spotify__svc_recently_played') }}
    QUALIFY row_number() OVER (PARTITION BY albumid ORDER BY playedat DESC) = 1
)

SELECT
    latest_version.albumid,
    latest_version.albumname,
    latest_version.album_type,
    latest_version.albumreleasedate,
    latest_version.albumreleasedateprecision,
    latest_version.albumtotaltracks,
    latest_version.albumuri,
    latest_version.albumexternalurl,
    latest_version.albumhref,
    latest_version.albumtype,
    latest_version.albumimageurl,
    latest_version.albumimageheight,
    latest_version.albumimagewidth,
    stats.play_count,
    stats.total_listen_duration,
    stats.first_played_at,
    stats.last_played_at,
    string_agg(albumartist.artistname, ', ' ORDER BY artist_offset)
        AS all_artist_names,
    (SELECT MAX(_dp_inserted_at) FROM {{ ref('lake_spotify__svc_recently_played') }}) AS max__dp_inserted_at
FROM
    latest_version,
    unnest(albumartists) AS albumartist WITH OFFSET AS artist_offset
LEFT JOIN stats ON latest_version.albumid = stats.albumid
GROUP BY ALL
