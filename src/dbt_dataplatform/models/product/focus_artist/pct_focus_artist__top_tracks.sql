{{ config(materialized='table', tags=['spotify', 'product']) }}

WITH track_stats AS (
    SELECT
        bridge.artistid,
        fact.trackid,
        tracks.trackname,
        tracks.trackexternalurl,
        albums.albumname,
        albums.albumimageurl,
        COUNT(DISTINCT fact.playedat)   AS play_count,
        SUM(tracks.trackdurationms)     AS total_listen_ms,
        MIN(fact.playedat)              AS first_played_at,
        MAX(fact.playedat)              AS last_played_at
    FROM {{ ref('hub_music__svc_fact_played') }} AS fact
    INNER JOIN {{ ref('hub_music__svc_bridge_tracks_artists') }} AS bridge
        ON fact.trackid = bridge.trackid
    INNER JOIN {{ ref('hub_music__svc_dim_tracks') }} AS tracks
        ON fact.trackid = tracks.trackid
    LEFT JOIN {{ ref('hub_music__svc_dim_albums') }} AS albums
        ON tracks.albumid = albums.albumid
    WHERE bridge.artist_role = 'primary'
      AND bridge.artistid IS NOT NULL
    GROUP BY ALL
),

artist_totals AS (
    SELECT
        artistid,
        SUM(total_listen_ms) AS artist_total_ms
    FROM track_stats
    GROUP BY artistid
),

ranked AS (
    SELECT
        ts.artistid                                                                 AS artist_id,
        ROW_NUMBER() OVER (
            PARTITION BY ts.artistid ORDER BY ts.play_count DESC
        )                                                                           AS track_rank,
        ts.trackid                                                                  AS track_id,
        ts.trackname                                                                AS track_name,
        ts.albumname                                                                AS album_name,
        ts.albumimageurl                                                            AS album_image_url,
        ts.trackexternalurl                                                         AS track_url,
        ts.play_count,
        {{ ms_to_hms('ts.total_listen_ms') }}                                      AS total_duration,
        ts.total_listen_ms                                                          AS total_duration_ms,
        CAST(ts.first_played_at AS STRING)                                          AS first_played_at,
        CAST(ts.last_played_at AS STRING)                                           AS last_played_at,
        ROUND(100.0 * ts.total_listen_ms / NULLIF(at.artist_total_ms, 0), 1)      AS pct_of_artist_time
    FROM track_stats AS ts
    LEFT JOIN artist_totals AS at ON ts.artistid = at.artistid
)

SELECT *
FROM ranked
WHERE track_rank <= 20
ORDER BY artist_id, track_rank
