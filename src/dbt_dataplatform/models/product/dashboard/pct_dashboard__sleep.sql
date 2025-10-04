{{ config(dataset=get_schema('product'), materialized='view', tags=["product", "garmin", "sleep"]) }}

-- Mart view for sleep dashboard - returns complete data for NightSummaryCard component
-- Includes base metrics, 7-day averages, and hourly timeseries for all health metrics

WITH sleep_base AS (
    SELECT
        sleep_date,
        FORMAT_DATE('%d %B %Y', sleep_date) as date_formatted,

        -- Score global
        sleep_scores.overall.value as score,

        -- Heures de coucher et réveil
        FORMAT_TIMESTAMP('%H:%M', sleep_window.start_local) as bedtime,
        FORMAT_TIMESTAMP('%H:%M', sleep_window.end_local) as wakeup,

        -- Durée totale en minutes
        CAST(total_sleep_seconds / 60 AS INT64) as total_minutes,

        -- Phases de sommeil en minutes
        STRUCT(
            CAST(light_sleep_seconds / 60 AS INT64) as light_minutes,
            CAST(deep_sleep_seconds / 60 AS INT64) as deep_minutes,
            CAST(rem_sleep_seconds / 60 AS INT64) as rem_minutes,
            CAST(awake_sleep_seconds / 60 AS INT64) as awake_minutes
        ) as phases_minutes,

        -- Données brutes pour calculs supplémentaires
        light_sleep_seconds,
        deep_sleep_seconds,
        rem_sleep_seconds,
        awake_sleep_seconds,

        -- Métadonnées utiles
        sleep_scores.overall.qualifier as sleep_quality,
        health_metrics.awake_count,

        -- Métriques de santé actuelles (valeurs de la nuit)
        CAST(health_metrics.avg_stress AS INT64) as stress,
        CAST(avg_overnight_hrv AS INT64) as hrv,
        body_battery_change as body_battery,
        CAST(health_metrics.avg_spo2 AS INT64) as spo2,

        -- Pour les timeseries
        sleep_id

    FROM {{ ref('hub_garmin__sleep') }}
    WHERE sleep_date IS NOT NULL
),

sleep_with_averages AS (
    SELECT
        *,
        -- Moyennes sur 7 jours glissants
        AVG(stress) OVER (ORDER BY sleep_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) as stress_avg_7d,
        AVG(hrv) OVER (ORDER BY sleep_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) as hrv_avg_7d,
        AVG(body_battery) OVER (ORDER BY sleep_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) as body_battery_avg_7d,
        AVG(spo2) OVER (ORDER BY sleep_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) as spo2_avg_7d
    FROM sleep_base
),

-- Récupérer les timeseries déjà parsées depuis le hub
sleep_timeseries_parsed AS (
    SELECT
        sleep_id,
        stress_timeseries,
        hrv_timeseries,
        body_battery_timeseries,
        heart_rate_timeseries,
        spo2_timeseries,
        breathing_disruption_timeseries,
        sleep_movement_timeseries,
        sleep_levels_timeseries,
        restless_moments_timeseries,
        respiration_timeseries,
        respiration_averages_timeseries,
        spo2_summary
    FROM {{ ref('hub_garmin__sleep_timeseries') }}
    WHERE sleep_id IS NOT NULL
)

-- Final SELECT combinant tout
SELECT
    s.sleep_date,
    s.date_formatted,
    s.score,
    s.bedtime,
    s.wakeup,
    s.total_minutes,
    s.phases_minutes,

    -- Métriques actuelles
    s.stress,
    s.hrv,
    s.body_battery,
    s.spo2,

    -- Moyennes 7 jours
    s.stress_avg_7d,
    s.hrv_avg_7d,
    s.body_battery_avg_7d,
    s.spo2_avg_7d,

    -- Séries temporelles
    ts.stress_timeseries,
    ts.hrv_timeseries,
    ts.body_battery_timeseries,
    ts.heart_rate_timeseries,
    ts.spo2_timeseries,
    ts.breathing_disruption_timeseries,
    ts.sleep_movement_timeseries,
    ts.sleep_levels_timeseries,
    ts.restless_moments_timeseries,
    ts.respiration_timeseries,
    ts.respiration_averages_timeseries,

    -- SpO2 summary (STRUCT)
    ts.spo2_summary,

    -- Métadonnées additionnelles
    s.sleep_quality,
    s.awake_count,
    s.light_sleep_seconds,
    s.deep_sleep_seconds,
    s.rem_sleep_seconds,
    s.awake_sleep_seconds

FROM sleep_with_averages s
LEFT JOIN sleep_timeseries_parsed ts ON s.sleep_id = ts.sleep_id
ORDER BY s.sleep_date DESC
