{{ config(materialized='view', tags=['spotify', 'product']) }}

WITH daily_stats AS (
    SELECT
        bridge.artistid,
        DATE(fact.playedat)             AS listen_date,
        COUNT(DISTINCT fact.playedat)   AS play_count,
        SUM(tracks.trackdurationms)     AS total_duration_ms
    FROM {{ ref('hub_music__svc_fact_played') }} AS fact
    INNER JOIN {{ ref('hub_music__svc_bridge_tracks_artists') }} AS bridge
        ON fact.trackid = bridge.trackid
    INNER JOIN {{ ref('hub_music__svc_dim_tracks') }} AS tracks
        ON fact.trackid = tracks.trackid
    WHERE bridge.artist_role = 'primary'
      AND bridge.artistid IS NOT NULL
    GROUP BY bridge.artistid, DATE(fact.playedat)
)

SELECT
    artistid                                        AS artist_id,
    CAST(listen_date AS STRING)                     AS listen_date,
    play_count,
    total_duration_ms,
    {{ ms_to_hm('total_duration_ms') }}             AS total_duration
FROM daily_stats
ORDER BY artist_id, listen_date
