{{ config(dataset=get_schema('product'), materialized='view', tags=["product", "daily_recap"]) }}

-- Daily recap view for yesterday's sleep data
-- Provides a clean summary of sleep metrics from the previous night

SELECT
    -- Core identifiers and date
    main.sleep_id,
    main.sleep_date,
    
    -- Sleep timing (simplified from struct)
    main.sleep_window.start_local as bedtime,
    main.sleep_window.end_local as wake_time,
    EXTRACT(HOUR FROM main.sleep_window.start_local) as bedtime_hour,
    EXTRACT(HOUR FROM main.sleep_window.end_local) as wake_time_hour,
    
    -- Sleep duration metrics
    ROUND(main.total_sleep_seconds / 3600.0, 2) as total_sleep_hours,
    ROUND(main.nap_time_seconds / 3600.0, 2) as nap_time_hours,
    
    -- Sleep phases (in hours for readability)
    ROUND(main.deep_sleep_seconds / 3600.0, 2) as deep_sleep_hours,
    ROUND(main.light_sleep_seconds / 3600.0, 2) as light_sleep_hours,
    ROUND(main.rem_sleep_seconds / 3600.0, 2) as rem_sleep_hours,
    ROUND(main.awake_sleep_seconds / 3600.0, 2) as awake_hours,
    
    -- Sleep phase percentages
    ROUND((main.deep_sleep_seconds * 100.0) / NULLIF(main.total_sleep_seconds, 0), 1) as deep_sleep_percentage,
    ROUND((main.light_sleep_seconds * 100.0) / NULLIF(main.total_sleep_seconds, 0), 1) as light_sleep_percentage,
    ROUND((main.rem_sleep_seconds * 100.0) / NULLIF(main.total_sleep_seconds, 0), 1) as rem_sleep_percentage,
    
    -- Sleep quality metrics
    main.sleep_scores.overall.value as overall_sleep_score,
    main.sleep_scores.overall.qualifier as sleep_quality,
    main.health_metrics.awake_count,
    
    -- Health metrics during sleep
    main.health_metrics.avg_spo2,
    main.health_metrics.avg_respiration,
    main.health_metrics.avg_stress as avg_sleep_stress,
    main.resting_heart_rate,
    main.avg_overnight_hrv,
    main.body_battery_change,
    
    -- Sleep need analysis
    ROUND(main.sleep_need.baseline / 3600.0, 1) as baseline_sleep_need_hours,
    ROUND(main.sleep_need.actual / 3600.0, 1) as actual_sleep_need_hours,
    ROUND((main.total_sleep_seconds - main.sleep_need.baseline) / 3600.0, 1) as sleep_deficit_surplus_hours,
    main.sleep_need.feedback as sleep_need_feedback,
    
    -- Device and data quality flags
    main.device_info.from_device as measured_by_device,
    main.sleep_window.confirmed as sleep_window_confirmed,
    main.device_info.rem_capable as device_rem_capable,
    
    -- Insights and feedback
    main.insights.score_feedback,
    main.insights.score_insight,
    main.insights.personalized_insight,
    
    -- Data freshness
    main.dp_inserted_at as data_inserted_at,
    
    -- TIMESERIES DATA FOR CHARTS
    -- Heart rate evolution during sleep (for line charts)
    ts.sleep_heart_rate as heart_rate_timeseries,
    
    -- HRV data during sleep (for variability analysis)
    ts.hrv_data as hrv_timeseries,
    
    -- Stress level evolution during sleep
    ts.sleep_stress as stress_timeseries,
    
    -- Body battery evolution during sleep
    ts.sleep_body_battery as body_battery_timeseries,
    
    -- SpO2 temporal data (oxygen saturation)
    ts.wellness_spo2_epochs as spo2_timeseries,
    
    -- Respiration rate temporal data
    ts.wellness_respiration_epochs as respiration_timeseries,
    
    -- Sleep movement and restlessness
    ts.sleep_movement as movement_timeseries,
    ts.sleep_levels as sleep_levels_timeseries,
    ts.sleep_restless_moments as restless_moments_timeseries,
    
    -- Breathing disruption events
    ts.breathing_disruption_data as breathing_disruptions
    
FROM {{ ref('hub_garmin__sleep') }} main
LEFT JOIN {{ ref('hub_garmin__sleep_timeseries') }} ts
    ON main.sleep_id = ts.sleep_id 
    AND main.sleep_date = ts.sleep_date
WHERE 
    -- Filter for yesterday's sleep data
    main.sleep_date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    -- Only include complete sleep sessions (exclude naps or incomplete data)
    AND main.total_sleep_seconds >= 3600  -- At least 1 hour of sleep
    AND main.sleep_window.confirmed = true  -- Only confirmed sleep windows
ORDER BY main.sleep_date DESC, bedtime DESC