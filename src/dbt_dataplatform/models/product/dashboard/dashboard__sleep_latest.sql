{{ config(
    dataset=get_schema('product'),
    materialized='view',
    description='Vue optimisée des données de sommeil pour le dashboard ELA avec métriques dérivées',
    tags=["product", "dashboard", "garmin"]
) }}

WITH sleep_calculations AS (
    SELECT
      sleep_date,
      sleep_scores.overall.value as sleep_score,

      -- Durée de sommeil (heures décimales + formatée)
      ROUND(total_sleep_seconds / 3600.0, 1) as duration_hours,
      CONCAT(
        CAST(FLOOR(total_sleep_seconds / 3600.0) AS STRING), 'h ',
        CAST(ROUND(MOD(CAST(total_sleep_seconds AS INT64), 3600) / 60.0) AS STRING), 'm'
      ) as duration_formatted,

      -- Heures de coucher et lever (format TIME)
      TIME(sleep_window.start_local) as bedtime,
      TIME(sleep_window.end_local) as wake_time,

      -- Phases de sommeil en minutes
      ROUND(deep_sleep_seconds / 60.0, 0) as deep_minutes,
      ROUND(light_sleep_seconds / 60.0, 0) as light_minutes,
      ROUND(rem_sleep_seconds / 60.0, 0) as rem_minutes,
      ROUND(awake_sleep_seconds / 60.0, 0) as awake_minutes,

      -- Total des phases pour les calculs de pourcentages
      ROUND((deep_sleep_seconds + light_sleep_seconds + rem_sleep_seconds + awake_sleep_seconds) / 60.0, 0) as total_sleep_minutes,

      -- Métriques santé
      resting_heart_rate,
      avg_overnight_hrv as hrv_average,
      health_metrics.avg_spo2 as spo2_average,
      health_metrics.awake_count

    FROM {{ ref('hub_garmin__sleep') }}
    WHERE sleep_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
      AND sleep_date IS NOT NULL
      AND total_sleep_seconds IS NOT NULL
  )

  SELECT
    sleep_date as date,
    sleep_score,
    duration_hours,
    duration_formatted,
    bedtime,
    wake_time,

    -- Phases en minutes (brutes)
    deep_minutes,
    light_minutes,
    rem_minutes,
    awake_minutes,
    total_sleep_minutes,

    -- 🎯 NOUVEAUX CALCULS : Pourcentages des phases
    ROUND((deep_minutes / NULLIF(total_sleep_minutes, 0)) * 100, 0) as deep_percentage,
    ROUND((light_minutes / NULLIF(total_sleep_minutes, 0)) * 100, 0) as light_percentage,
    ROUND((rem_minutes / NULLIF(total_sleep_minutes, 0)) * 100, 0) as rem_percentage,
    ROUND((awake_minutes / NULLIF(total_sleep_minutes, 0)) * 100, 0) as awake_percentage,

    -- 🎯 NOUVEAUX CALCULS : Phases formatées en "1h 45m"
    CASE
      WHEN deep_minutes >= 60 THEN
        CONCAT(CAST(FLOOR(deep_minutes / 60) AS STRING), 'h ', CAST(MOD(CAST(deep_minutes AS INT64), 60) AS STRING), 'm')
      ELSE
        CONCAT(CAST(deep_minutes AS STRING), 'm')
    END as deep_formatted,

    CASE
      WHEN light_minutes >= 60 THEN
        CONCAT(CAST(FLOOR(light_minutes / 60) AS STRING), 'h ', CAST(MOD(CAST(light_minutes AS INT64), 60) AS STRING), 'm')
      ELSE
        CONCAT(CAST(light_minutes AS STRING), 'm')
    END as light_formatted,

    CASE
      WHEN rem_minutes >= 60 THEN
        CONCAT(CAST(FLOOR(rem_minutes / 60) AS STRING), 'h ', CAST(MOD(CAST(rem_minutes AS INT64), 60) AS STRING), 'm')
      ELSE
        CONCAT(CAST(rem_minutes AS STRING), 'm')
    END as rem_formatted,

    CASE
      WHEN awake_minutes >= 60 THEN
        CONCAT(CAST(FLOOR(awake_minutes / 60) AS STRING), 'h ', CAST(MOD(CAST(awake_minutes AS INT64), 60) AS STRING), 'm')
      ELSE
        CONCAT(CAST(awake_minutes AS STRING), 'm')
    END as awake_formatted,

    -- 🎯 NOUVEAU CALCUL : Résumé intelligent basé sur le score
    CASE
      WHEN sleep_score >= 85 THEN 'Excellent sleep quality with optimal deep sleep phases and minimal disruptions. Your recovery metrics are outstanding, setting you up for peak performance today.'
      WHEN sleep_score >= 75 THEN 'Good sleep quality with solid deep sleep phases and manageable interruptions. Your recovery is on track for a productive day ahead.'
      WHEN sleep_score >= 60 THEN "Fair sleep quality with some room for improvement. Consider optimizing your sleep environment or bedtime routine for better recovery."
      ELSE 'Sleep quality below optimal levels with frequent disruptions. Focus on sleep hygiene and consider factors that might be affecting your rest.'
    END as sleep_summary,

    -- 🎯 NOUVEAU CALCUL : Efficacité du sommeil
    ROUND(((deep_minutes + light_minutes + rem_minutes) / NULLIF(total_sleep_minutes, 0)) * 100, 1) as sleep_efficiency_percentage,

    -- 🎯 NOUVEAU CALCUL : Catégorie de qualité
    CASE
      WHEN sleep_score >= 85 THEN 'Excellent'
      WHEN sleep_score >= 75 THEN 'Good'
      WHEN sleep_score >= 60 THEN 'Fair'
      ELSE 'Poor'
    END as quality_category,

    -- Métriques santé (inchangées)
    resting_heart_rate,
    hrv_average,
    spo2_average,
    awake_count

  FROM sleep_calculations
  ORDER BY sleep_date DESC