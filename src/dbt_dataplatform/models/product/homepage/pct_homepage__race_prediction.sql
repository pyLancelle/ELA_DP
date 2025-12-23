{{
  config(
      tags=['garmin', 'product'],
      materialized='view'
  )
}}

WITH latest_predictions AS (
  SELECT
    CAST(time5K AS INT64) AS time5K,
    CAST(time10K AS INT64) AS time10K,
    CAST(timeHalfMarathon AS INT64) AS timeHalfMarathon,
    CAST(timeMarathon AS INT64) AS timeMarathon,
    PARSE_DATE('%Y-%m-%d', calendarDate) AS calendarDate
  FROM {{ ref('lake_garmin__svc_race_predictions') }}
  ORDER BY calendarDate DESC
  LIMIT 1
),

past_predictions AS (
  SELECT
    CAST(time5K AS INT64) AS time5K,
    CAST(time10K AS INT64) AS time10K,
    CAST(timeHalfMarathon AS INT64) AS timeHalfMarathon,
    CAST(timeMarathon AS INT64) AS timeMarathon,
    PARSE_DATE('%Y-%m-%d', calendarDate) AS calendarDate
  FROM {{ ref('lake_garmin__svc_race_predictions') }}
  WHERE PARSE_DATE('%Y-%m-%d', calendarDate) <= DATE_SUB((SELECT calendarDate FROM latest_predictions), INTERVAL 30 DAY)
  ORDER BY calendarDate DESC
  LIMIT 1
),

unpivoted AS (
  SELECT '5K' AS distance, time5K AS current_time, calendarDate AS current_date FROM latest_predictions
  UNION ALL
  SELECT '10K', time10K, calendarDate FROM latest_predictions
  UNION ALL
  SELECT '21K', timeHalfMarathon, calendarDate FROM latest_predictions
  UNION ALL
  SELECT '42K', timeMarathon, calendarDate FROM latest_predictions
)

SELECT
  u.distance,
  u.current_date,
  u.current_time,
  p.calendarDate AS previous_date,
  CASE u.distance
    WHEN '5K' THEN p.time5K
    WHEN '10K' THEN p.time10K
    WHEN '21K' THEN p.timeHalfMarathon
    WHEN '42K' THEN p.timeMarathon
  END AS previous_time,
  u.current_time - CASE u.distance
    WHEN '5K' THEN p.time5K
    WHEN '10K' THEN p.time10K
    WHEN '21K' THEN p.timeHalfMarathon
    WHEN '42K' THEN p.timeMarathon
  END AS diff_seconds
FROM unpivoted u
CROSS JOIN past_predictions p
ORDER BY
  CASE u.distance
    WHEN '5K' THEN 1
    WHEN '10K' THEN 2
    WHEN '21K' THEN 3
    WHEN '42K' THEN 4
  END
