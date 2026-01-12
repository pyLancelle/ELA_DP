WITH vo2max_data AS (
    SELECT
        DATE(date) as date,
        COALESCE(mostRecentVO2Max.generic.vo2MaxPreciseValue, mostRecentVO2Max.generic.vo2MaxValue) as vo2max
    FROM {{ ref('lake_garmin__svc_training_status') }}
    WHERE mostRecentVO2Max IS NOT NULL
        AND DATE(date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH)
),
current_vo2max AS (
    SELECT
        date,
        vo2max
    FROM vo2max_data
    ORDER BY date DESC
    LIMIT 1
),
six_months_ago_vo2max AS (
    SELECT
        vo2max
    FROM vo2max_data
    WHERE date <= DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH)
    ORDER BY date ASC
    LIMIT 1
),
weekly_averages AS (
    SELECT
        DATE_TRUNC(date, WEEK(MONDAY)) as week_start,
        AVG(vo2max) as avg_vo2max_week
    FROM vo2max_data
    GROUP BY week_start
    ORDER BY week_start
)
SELECT
    current_vo2max.date as current_date,
    current_vo2max.vo2max as current_vo2max,
    ARRAY_AGG(weekly_averages.avg_vo2max_week ORDER BY weekly_averages.week_start) as weekly_vo2max_array,
    current_vo2max.vo2max - COALESCE(six_months_ago_vo2max.vo2max, current_vo2max.vo2max) as vo2max_delta_6_months
FROM current_vo2max
CROSS JOIN weekly_averages
LEFT JOIN six_months_ago_vo2max ON TRUE
GROUP BY ALL
