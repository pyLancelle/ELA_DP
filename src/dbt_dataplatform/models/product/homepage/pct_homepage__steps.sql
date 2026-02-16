/*
Données de Steps (nombre de pas) pour le graphique StepsChart.

Retourne la moyenne sur 7 jours, l'objectif et le détail quotidien avec :
- Date complète
- Jour de la semaine (première lettre)
- Nombre total de pas

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
    totalSteps as total_steps,
    dailyStepGoal as daily_goal
  FROM {{ ref('lake_garmin__svc_user_summary') }}
  WHERE PARSE_DATE('%Y-%m-%d', calendarDate) >= DATE_SUB(CURRENT_DATE(), INTERVAL 6 DAY)
    AND calendarDate IS NOT NULL
    AND totalSteps IS NOT NULL
  ORDER BY PARSE_DATE('%Y-%m-%d', calendarDate) DESC
  LIMIT 7
)
SELECT
  ROUND(AVG(total_steps)) as average,
  MAX(daily_goal) as goal,  -- Prend l'objectif le plus récent (généralement constant)
  ARRAY_AGG(
    STRUCT(
      FORMAT_DATE('%Y-%m-%d', date) as date,
      day,
      CAST(total_steps AS INT64) as steps
    )
    ORDER BY date ASC
  ) as daily
FROM daily_data
