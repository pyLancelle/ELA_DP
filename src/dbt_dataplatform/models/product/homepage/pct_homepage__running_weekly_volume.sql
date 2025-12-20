/*
Volume hebdomadaire de course sur les 10 dernières semaines.

Agrège les distances, temps et métriques d'entraînement par semaine
pour visualiser la progression du volume d'entraînement.
*/

{{
  config(
      tags=['health', 'homepage'],
      materialized='view'
  )
}}

WITH all_weeks AS (
  -- Génère les 10 dernières semaines (lundi au dimanche)
  SELECT
    DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL week_offset WEEK), WEEK(MONDAY)) AS week_start
  FROM UNNEST(GENERATE_ARRAY(0, 6)) AS week_offset
),

weekly_runs AS (
  SELECT
    DATE_TRUNC(DATE(startTimeGMT), WEEK(MONDAY)) AS week_start,
    COUNT(*) AS number_of_runs,
    SUM(distance / 1000) AS total_distance_km
  FROM {{ ref('hub_health__svc_activities') }}
  WHERE DATE(startTimeGMT) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 WEEK)
  GROUP BY week_start
)

SELECT
  all_weeks.week_start,
  COALESCE(weekly_runs.number_of_runs, 0) AS number_of_runs,
  CAST(ROUND(COALESCE(weekly_runs.total_distance_km, 0), 0) AS INT64) AS total_distance_km
FROM all_weeks
LEFT JOIN weekly_runs ON all_weeks.week_start = weekly_runs.week_start
ORDER BY all_weeks.week_start DESC
