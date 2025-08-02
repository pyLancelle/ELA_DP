{{ config(dataset=get_schema('product'), materialized='view', tags=["product", "dashboard"]) }}

-- Daily recap view for yesterday's sleep data
-- Provides a clean summary of sleep metrics from the previous night

SELECT
    -- Core identifiers and date
    main.sleep_date,
    
    ROUND(main.total_sleep_seconds / 3600.0, 2) as total_sleep_hours,
    
    -- Sleep phases (in hours for readability)
    ROUND(main.deep_sleep_seconds / 3600.0, 2) as deep_sleep_hours,
    ROUND(main.light_sleep_seconds / 3600.0, 2) as light_sleep_hours,
    ROUND(main.rem_sleep_seconds / 3600.0, 2) as rem_sleep_hours,
    ROUND(main.awake_sleep_seconds / 3600.0, 2) as awake_hours,
    
    main.sleep_scores.overall.value as overall_sleep_score,    
FROM {{ ref('hub_garmin__sleep') }} main
WHERE 
    -- Filter for yesterday's sleep data
    main.sleep_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
    -- Only include complete sleep sessions (exclude naps or incomplete data)
    AND main.total_sleep_seconds >= 3600  -- At least 1 hour of sleep
    AND main.sleep_window.confirmed = true  -- Only confirmed sleep windows