/*
Niveaux de sommeil granulaires de la nuit la plus récente.

Affiche les périodes de sommeil détaillées (deep, light, rem, awake)
pour la dernière nuit enregistrée, prêt pour visualisation timeline.
*/

{{
  config(
      tags=['health', 'homepage'],
      materialized='view'
  )
}}

WITH latest_sleep AS (
    SELECT MAX(date) AS latest_date
    FROM {{ ref('hub_health__svc_sleep') }}
    WHERE date IS NOT NULL and sleep_score IS NOT NULL
)

SELECT
    sleep.date,
    sl.start_time,
    sl.end_time,
    sl.level_name
FROM {{ ref('hub_health__stg_sleep') }} AS sleep,
UNNEST(sleep_levels) AS sl
CROSS JOIN latest_sleep
WHERE sleep.date = latest_sleep.latest_date
ORDER BY sl.start_time
