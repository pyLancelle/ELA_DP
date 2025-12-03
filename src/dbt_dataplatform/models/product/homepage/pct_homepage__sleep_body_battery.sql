/*
Données de sommeil, body battery, HRV et fréquence cardiaque au repos pour les 7 derniers jours.

Affiche un résumé quotidien pour la homepage incluant :
- Jour de la semaine en français (première lettre)
- Score de sommeil de la nuit
- Body battery : niveau au coucher, au lever, et gain pendant la nuit
- HRV nocturne moyen
- Fréquence cardiaque au repos
*/

{{
  config(
      tags=['health', 'homepage'],
      materialized='view'
  )
}}

SELECT
    date,
    SUBSTR(FORMAT_DATE('%A', date), 1, 1) AS day_abbr_french,
    sleep_score,
    body_battery.at_bedtime AS battery_at_bedtime,
    body_battery.at_waketime AS battery_at_waketime,
    body_battery.recovery AS battery_gain,
    avg_hrv,
    resting_heart_rate as resting_hr
FROM {{ ref('hub_health__svc_sleep') }}
WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 6 DAY)
  AND date IS NOT NULL
  AND sleep_score IS NOT NULL
ORDER BY date ASC
