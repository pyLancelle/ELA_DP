{{ config(materialized='table', tags=['spotify', 'product']) }}

WITH album_plays AS (
    SELECT
        bridge.artistid,
        tracks.albumid,
        COUNT(DISTINCT fact.trackid)    AS tracks_heard,
        COUNT(DISTINCT fact.playedat)   AS total_plays,
        SUM(tracks.trackdurationms)     AS total_duration_ms,
        MIN(fact.playedat)              AS first_played_at,
        MAX(fact.playedat)              AS last_played_at
    FROM {{ ref('hub_music__svc_fact_played') }} AS fact
    INNER JOIN {{ ref('hub_music__svc_bridge_tracks_artists') }} AS bridge
        ON fact.trackid = bridge.trackid
    INNER JOIN {{ ref('hub_music__svc_dim_tracks') }} AS tracks
        ON fact.trackid = tracks.trackid
    WHERE bridge.artist_role = 'primary'
      AND bridge.artistid IS NOT NULL
    GROUP BY bridge.artistid, tracks.albumid
)

SELECT
    ap.artistid                                                                         AS artist_id,
    ap.albumid                                                                          AS album_id,
    alb.albumname                                                                       AS album_name,
    alb.albumimageurl                                                                   AS album_image_url,
    alb.albumexternalurl                                                                AS album_url,
    alb.album_type,
    CAST(alb.albumreleasedate AS STRING)                                                AS release_date,
    alb.albumtotaltracks                                                                AS total_tracks,
    alb.all_artist_names                                                                AS artist_names,
    ap.tracks_heard,
    ap.total_plays,
    {{ ms_to_hms('ap.total_duration_ms') }}                                            AS total_duration,
    ap.total_duration_ms,
    CAST(ap.first_played_at AS STRING)                                                  AS first_played_at,
    CAST(ap.last_played_at AS STRING)                                                   AS last_played_at,
    ROUND(100.0 * ap.tracks_heard / NULLIF(alb.albumtotaltracks, 0), 1)               AS completion_rate,
    CASE
        WHEN ROUND(100.0 * ap.tracks_heard / NULLIF(alb.albumtotaltracks, 0), 1) >= 90
            THEN 'complete'
        WHEN ROUND(100.0 * ap.tracks_heard / NULLIF(alb.albumtotaltracks, 0), 1) >= 50
            THEN 'partial'
        ELSE 'shallow'
    END                                                                                 AS listen_depth
FROM album_plays AS ap
INNER JOIN {{ ref('hub_music__svc_dim_albums') }} AS alb ON ap.albumid = alb.albumid
ORDER BY ap.artistid, ap.total_duration_ms DESC
