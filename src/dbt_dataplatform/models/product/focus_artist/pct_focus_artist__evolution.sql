{{ config(materialized='view', tags=['spotify', 'product']) }}

WITH monthly_stats AS (
    SELECT
        bridge.artistid,
        FORMAT_DATE('%Y-%m', DATE(fact.playedat))   AS year_month,
        COUNT(DISTINCT fact.playedat)               AS play_count,
        COUNT(DISTINCT fact.trackid)                AS unique_tracks,
        SUM(tracks.trackdurationms)                 AS total_duration_ms
    FROM {{ ref('hub_music__svc_fact_played') }} AS fact
    INNER JOIN {{ ref('hub_music__svc_bridge_tracks_artists') }} AS bridge
        ON fact.trackid = bridge.trackid
    INNER JOIN {{ ref('hub_music__svc_dim_tracks') }} AS tracks
        ON fact.trackid = tracks.trackid
    WHERE bridge.artist_role = 'primary'
      AND bridge.artistid IS NOT NULL
    GROUP BY bridge.artistid, FORMAT_DATE('%Y-%m', DATE(fact.playedat))
)

SELECT
    artistid                                    AS artist_id,
    year_month,
    play_count,
    unique_tracks,
    total_duration_ms,
    {{ ms_to_hm('total_duration_ms') }}         AS total_duration
FROM monthly_stats
ORDER BY artist_id, year_month
