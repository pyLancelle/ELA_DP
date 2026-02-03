/*
Données de Resting Heart Rate pour le graphique RestingHrCard.

Retourne la moyenne sur 7 jours et le détail quotidien avec :
- Date complète
- Jour de la semaine (première lettre)
- Valeur de la fréquence cardiaque au repos
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
    resting_heart_rate as value
  FROM {{ ref('hub_health__svc_sleep') }}
  WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 6 DAY)
    AND date IS NOT NULL
    AND resting_heart_rate IS NOT NULL
),
stats AS (
  SELECT MAX(value) as max_value
  FROM daily_data
)
SELECT
  ROUND(AVG(value)) as average,
  ARRAY_AGG(
    STRUCT(
      FORMAT_DATE('%Y-%m-%d', date) as date,
      day,
      value,
      ROUND(SAFE_DIVIDE(value * 100, (SELECT max_value FROM stats)), 1) as display_height_percent
    )
    ORDER BY date DESC
    LIMIT 7
  ) as daily
FROM daily_data
