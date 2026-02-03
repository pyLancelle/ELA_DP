/*
Données de Body Battery pour le graphique BodyBatteryChart.

Retourne le gain moyen sur 7 jours et le détail quotidien avec :
- Date complète
- Jour de la semaine (première lettre)
- Niveau au coucher (bedtime)
- Niveau au lever (waketime)
- Gain pendant la nuit

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
    body_battery.at_bedtime as bedtime,
    body_battery.at_waketime as waketime,
    body_battery.recovery as gain
  FROM {{ ref('hub_health__svc_sleep') }}
  WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 6 DAY)
    AND date IS NOT NULL
    AND body_battery.at_bedtime IS NOT NULL
    AND body_battery.at_waketime IS NOT NULL
  ORDER BY date DESC
  LIMIT 7
)
SELECT
  ROUND(AVG(gain)) as average_gain,
  ARRAY_AGG(
    STRUCT(
      FORMAT_DATE('%Y-%m-%d', date) as date,
      day,
      bedtime,
      waketime,
      gain
    )
    ORDER BY date ASC
  ) as daily
FROM daily_data
