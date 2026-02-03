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
),
with_stats AS (
    SELECT
        date_spine.date,
        COALESCE(ranking.total_duration_ms, 0) as total_duration_ms,
        MAX(COALESCE(ranking.total_duration_ms, 0)) OVER () as max_duration_ms,
        AVG(COALESCE(ranking.total_duration_ms, 0)) OVER () as avg_duration_ms
    FROM date_spine
    LEFT JOIN ranking
        ON date_spine.date = ranking.date
),
daily_data AS (
    SELECT
        date,
        total_duration_ms,
        CASE
            WHEN max_duration_ms > 0 THEN ROUND((total_duration_ms / max_duration_ms) * 100, 2)
            ELSE 0
        END as bar_height_percent,
        SUBSTR(FORMAT_DATE('%A', date), 1, 1) as day_letter,
        CONCAT(
            CAST(FLOOR(total_duration_ms / 3600000) AS STRING), 'h',
            LPAD(CAST(FLOOR(MOD(total_duration_ms, 3600000) / 60000) AS STRING), 2, '0'), 'm'
        ) as duration_formatted,
        avg_duration_ms
    FROM with_stats
)
SELECT
    ARRAY_AGG(
        STRUCT(
            date,
            total_duration_ms,
            bar_height_percent,
            day_letter,
            duration_formatted
        )
        ORDER BY date DESC
        LIMIT 10
    ) as data,
    CONCAT(
        CAST(FLOOR(ANY_VALUE(avg_duration_ms) / 3600000) AS STRING), 'h ',
        CAST(FLOOR(MOD(CAST(ANY_VALUE(avg_duration_ms) AS INT64), 3600000) / 60000) AS STRING), 'm'
    ) as avg_duration_formatted
FROM daily_data