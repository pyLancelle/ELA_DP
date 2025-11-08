WITH ranking AS (
    SELECT 
        DATE(playedat) as date,
        sum(dim_tracks.trackDURATIONMS) as total_duration_ms
    FROM {{ ref('hub_music__svc_fact_played')}} AS fact_played
    LEFT JOIN {{ ref('hub_music__svc_dim_tracks')}} AS dim_tracks
        ON fact_played.trackid = dim_tracks.trackid
    WHERE DATE(fact_played.playedat) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
    GROUP BY ALL
    ORDER BY total_duration_ms DESC
)
SELECT
    date,
    total_duration_ms,
    {{ ms_to_hms('total_duration_ms') }} AS total_duration,
FROM ranking