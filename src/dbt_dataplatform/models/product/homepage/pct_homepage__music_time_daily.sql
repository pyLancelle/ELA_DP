WITH date_spine AS (
    SELECT DATE_SUB(CURRENT_DATE(), INTERVAL offset DAY) as date
    FROM UNNEST(GENERATE_ARRAY(0, 13)) AS offset
),
ranking AS (
    SELECT
        DATE(playedat) as date,
        SUM(dim_tracks.trackDURATIONMS) as total_duration_ms
    FROM {{ ref('hub_music__svc_fact_played')}} AS fact_played
    LEFT JOIN {{ ref('hub_music__svc_dim_tracks')}} AS dim_tracks
        ON fact_played.trackid = dim_tracks.trackid
    WHERE DATE(fact_played.playedat) >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)
    GROUP BY ALL
)
SELECT
    date_spine.date,
    COALESCE(ranking.total_duration_ms, 0) as total_duration_ms,
    {{ ms_to_hms('COALESCE(ranking.total_duration_ms, 0)') }} AS total_duration
FROM date_spine
LEFT JOIN ranking
    ON date_spine.date = ranking.date
ORDER BY date_spine.date DESC
LIMIT 10