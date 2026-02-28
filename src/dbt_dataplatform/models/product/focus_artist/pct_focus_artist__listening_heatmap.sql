{{ config(materialized='table', tags=['spotify', 'product']) }}

SELECT
    bridge.artistid                             AS artist_id,
    EXTRACT(HOUR FROM fact.playedat)            AS hour_of_day,
    EXTRACT(DAYOFWEEK FROM fact.playedat)       AS day_of_week,
    FORMAT_TIMESTAMP('%A', fact.playedat)       AS day_name,
    COUNT(DISTINCT fact.playedat)               AS play_count,
    SUM(tracks.trackdurationms)                 AS total_duration_ms
FROM {{ ref('hub_music__svc_fact_played') }} AS fact
INNER JOIN {{ ref('hub_music__svc_bridge_tracks_artists') }} AS bridge
    ON fact.trackid = bridge.trackid
INNER JOIN {{ ref('hub_music__svc_dim_tracks') }} AS tracks
    ON fact.trackid = tracks.trackid
WHERE bridge.artist_role = 'primary'
  AND bridge.artistid IS NOT NULL
GROUP BY
    bridge.artistid,
    EXTRACT(HOUR FROM fact.playedat),
    EXTRACT(DAYOFWEEK FROM fact.playedat),
    FORMAT_TIMESTAMP('%A', fact.playedat)
ORDER BY artist_id, day_of_week, hour_of_day
