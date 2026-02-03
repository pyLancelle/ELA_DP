/*
Données de Sleep Score pour le graphique SleepScoreChart.

Retourne la moyenne sur 7 jours et le détail quotidien avec :
- Date complète
- Jour de la semaine (première lettre)
- Score de sommeil

Structure optimisée pour affichage frontend direct.
*/

{{
  config(
      tags=['health', 'homepage'],
      materialized='view'
  )
}}

WITH daily_data AS (
  SELECT
    date,
    SUBSTR(FORMAT_DATE('%A', date), 1, 1) as day,
    sleep_score as score
  FROM {{ ref('hub_health__svc_sleep') }}
  WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 6 DAY)
    AND date IS NOT NULL
    AND sleep_score IS NOT NULL
  ORDER BY date DESC
  LIMIT 7
)
SELECT
  ROUND(AVG(score)) as average,
  ARRAY_AGG(
    STRUCT(
      FORMAT_DATE('%Y-%m-%d', date) as date,
      day,
      score
    )
    ORDER BY date ASC
  ) as daily
FROM daily_data
