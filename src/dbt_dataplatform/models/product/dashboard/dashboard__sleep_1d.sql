{{ config(dataset=get_schema('product'), materialized='view', tags=["product", "dashboard"]) }}

-- Daily recap view for yesterday's sleep data
-- Provides a clean summary of sleep metrics from the previous night

SELECT
    -- Core identifiers and date
    main.sleep_date,
    -- Sleep timing (simplified from struct)
    main.sleep_window.start_local as bedtime,
    main.sleep_window.end_local as wake_time,
    EXTRACT(HOUR FROM main.sleep_window.start_local) as bedtime_hour,
    EXTRACT(HOUR FROM main.sleep_window.end_local) as wake_time_hour,
    -- Sleep duration metrics
    ROUND(main.total_sleep_seconds / 3600.0, 2) as duration,
    -- Sleep phases (in hours for readability)
    ROUND(main.deep_sleep_seconds / 3600.0, 2) as deep,
    ROUND(main.light_sleep_seconds / 3600.0, 2) as light,
    ROUND(main.rem_sleep_seconds / 3600.0, 2) as rem,
    ROUND(main.awake_sleep_seconds / 3600.0, 2) as awake,
    -- Sleep quality metrics
    main.sleep_scores.overall.value as score,
    -- Sleep need analysis
    ROUND(main.sleep_need.actual / 60, 1) as recommended,
FROM {{ ref('hub_garmin__sleep') }} main
WHERE 
    -- Filter for yesterday's sleep data
    main.total_sleep_seconds >= 3600
    AND main.sleep_window.confirmed = true
ORDER BY sleep_date DESC
LIMIT 1