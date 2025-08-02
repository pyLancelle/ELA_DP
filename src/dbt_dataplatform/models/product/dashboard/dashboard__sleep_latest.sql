{{ config(dataset=get_schema('product'), materialized='view', tags=["product", "dashboard", "garmin"]) }}

-- Vue dashboard : Données de sommeil de la nuit dernière pour l'API frontend
-- Correspond exactement aux champs requis par le frontend

SELECT 
    -- Date et identifiants
    sleep_date as date,
    
    -- Score de sommeil principal
    sleep_scores.overall.value as sleep_score,
    
    -- Durée de sommeil
    ROUND(total_sleep_seconds / 3600.0, 1) as duration_hours,
    
    -- Heures de coucher et lever (format TIME ou TIMESTAMP selon besoin frontend)
    TIME(sleep_window.start_local) as bedtime,
    TIME(sleep_window.end_local) as wake_time,
    
    -- Body Battery (si disponible, sinon NULL)
    -- Note : Les données body battery sont dans une table séparée, 
    -- ici on utilise le champ body_battery_change du sommeil
    CASE 
        WHEN body_battery_change IS NOT NULL THEN 
            -- Estimation du body battery de départ (assumant qu'il finit plus haut)
            GREATEST(0, 100 - ABS(body_battery_change))
        ELSE NULL 
    END as body_battery_start,
    
    CASE 
        WHEN body_battery_change IS NOT NULL THEN 
            -- Estimation du body battery de fin
            LEAST(100, GREATEST(0, 100 - ABS(body_battery_change)) + body_battery_change)
        ELSE NULL 
    END as body_battery_end,
    
    -- Gain de body battery
    body_battery_change as body_battery_gain,
    
    -- Phases de sommeil en minutes
    ROUND(deep_sleep_seconds / 60.0, 0) as deep_sleep_minutes,
    ROUND(light_sleep_seconds / 60.0, 0) as light_sleep_minutes,
    ROUND(rem_sleep_seconds / 60.0, 0) as rem_sleep_minutes,
    ROUND(awake_sleep_seconds / 60.0, 0) as awake_minutes,
    
    -- Métriques santé
    avg_overnight_hrv as hrv_average,
    health_metrics.avg_spo2 as spo2_average,
    
    -- Champs additionnels utiles pour le debugging/monitoring
    sleep_scores.overall.qualifier as sleep_quality,
    health_metrics.awake_count,
    resting_heart_rate,
    
    -- Métadonnées
    dp_inserted_at as last_updated

FROM {{ ref('hub_garmin__sleep') }}
WHERE sleep_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
  AND sleep_date IS NOT NULL
  AND total_sleep_seconds IS NOT NULL
ORDER BY sleep_date DESC
LIMIT 1