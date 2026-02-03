/*
Données de HRV (Heart Rate Variability) pour le graphique HrvCard.

Retourne la moyenne, la baseline (médiane) et le détail quotidien avec :
- Date complète
- Jour de la semaine (première lettre)
- Valeur HRV
- Indicateur si au-dessus de la baseline
- Pourcentage pour l'affichage en hauteur (normalisé sur le max)

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
    avg_hrv as value
  FROM {{ ref('hub_health__svc_sleep') }}
  WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 6 DAY)
    AND date IS NOT NULL
    AND avg_hrv IS NOT NULL
),
stats AS (
  SELECT
    ROUND(AVG(value)) as average,
    ROUND(APPROX_QUANTILES(value, 2)[OFFSET(1)]) as baseline,
    MAX(value) as max_value
  FROM daily_data
)
SELECT
  stats.average,
  stats.baseline,
  ARRAY_AGG(
    STRUCT(
      FORMAT_DATE('%Y-%m-%d', daily_data.date) as date,
      daily_data.day,
      daily_data.value,
      daily_data.value > stats.baseline as is_above_baseline,
      ROUND(SAFE_DIVIDE(daily_data.value * 100, stats.max_value), 1) as display_height_percent
    )
    ORDER BY daily_data.date ASC
    LIMIT 7
  ) as daily
FROM daily_data
CROSS JOIN stats
GROUP BY stats.average, stats.baseline, stats.max_value
