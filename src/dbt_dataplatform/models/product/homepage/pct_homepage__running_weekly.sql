{{
  config(
      tags=['health', 'product'],
      materialized='view'
  )
}}

WITH daily_runs AS (
  SELECT
    DATE(startTimeGMT) AS date,
    CASE EXTRACT(DAYOFWEEK FROM DATE(startTimeGMT))
      WHEN 1 THEN 'S'  -- Dimanche
      WHEN 2 THEN 'M'  -- Lundi
      WHEN 3 THEN 'T'  -- Mardi
      WHEN 4 THEN 'W'  -- Mercredi
      WHEN 5 THEN 'T'  -- Jeudi
      WHEN 6 THEN 'F'  -- Vendredi
      WHEN 7 THEN 'S'  -- Samedi
    END AS day_of_week,
    SUM(distance / 1000) AS total_distance_km,
    SUM(COALESCE(aerobicTrainingEffect, 0)) AS aerobic_score,
    SUM(COALESCE(anaerobicTrainingEffect, 0)) AS anaerobic_score
  FROM {{ ref('hub_health__svc_activities') }}
  WHERE DATE(startTimeGMT) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
  GROUP BY date, day_of_week
)

SELECT
  date,
  day_of_week,
  ROUND(total_distance_km, 2) AS total_distance_km,
  ROUND(aerobic_score, 1) AS aerobic_score,
  ROUND(anaerobic_score, 1) AS anaerobic_score
FROM daily_runs
ORDER BY date DESC
