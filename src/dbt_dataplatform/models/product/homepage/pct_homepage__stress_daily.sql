/*
Données de Stress quotidien pour le graphique StressChart.

Retourne la moyenne sur 7 jours et le détail quotidien avec :
- Date complète
- Jour de la semaine (première lettre)
- Niveau de stress moyen (avgStressLevel)
- Niveau de stress maximum (maxStressLevel)

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
    PARSE_DATE('%Y-%m-%d', calendarDate) as date,
    SUBSTR(FORMAT_DATE('%A', PARSE_DATE('%Y-%m-%d', calendarDate)), 1, 1) as day,
    avgStressLevel as avg_stress,
    maxStressLevel as max_stress
  FROM {{ ref('lake_garmin__svc_stress') }}
  WHERE PARSE_DATE('%Y-%m-%d', calendarDate) >= DATE_SUB(CURRENT_DATE(), INTERVAL 6 DAY)
    AND calendarDate IS NOT NULL
    AND avgStressLevel IS NOT NULL
  ORDER BY PARSE_DATE('%Y-%m-%d', calendarDate) DESC
  LIMIT 7
)
SELECT
  ROUND(AVG(avg_stress)) as average_stress,
  ARRAY_AGG(
    STRUCT(
      FORMAT_DATE('%Y-%m-%d', date) as date,
      day,
      avg_stress,
      max_stress
    )
    ORDER BY date ASC
  ) as daily
FROM daily_data
