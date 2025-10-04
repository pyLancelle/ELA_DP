{{ config(dataset=get_schema('product'), materialized='view', tags=["mart", "garmin", "sleep"]) }}

-- Mart view for sleep time series - aggregated by minute for frontend charts
-- Uses structured data from hub_garmin__sleep_timeseries
SELECT
    sleep_date,
    sleep_id,

    -- Extract sleep levels (phases) with timestamps
    PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', level.start_time) as timestamp_gmt,
    CASE level.sleep_level
        WHEN 0 THEN 'awake'
        WHEN 1 THEN 'light'
        WHEN 2 THEN 'deep'
        WHEN 3 THEN 'rem'
        ELSE 'unknown'
    END as sleep_phase,

    -- Heart rate at this point (find closest measurement)
    (
        SELECT hr.value
        FROM UNNEST(heart_rate_timeseries) hr
        WHERE PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', hr.time) <= PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', level.start_time)
        ORDER BY PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', hr.time) DESC
        LIMIT 1
    ) as heart_rate,

    -- Stress level
    (
        SELECT stress.value
        FROM UNNEST(stress_timeseries) stress
        WHERE PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', stress.time) <= PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', level.start_time)
        ORDER BY PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', stress.time) DESC
        LIMIT 1
    ) as stress_level,

    -- Body battery
    (
        SELECT bb.value
        FROM UNNEST(body_battery_timeseries) bb
        WHERE PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', bb.time) <= PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', level.start_time)
        ORDER BY PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', bb.time) DESC
        LIMIT 1
    ) as body_battery,

    -- Respiration
    (
        SELECT resp.respiration_value
        FROM UNNEST(respiration_timeseries) resp
        WHERE PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', resp.time) <= PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', level.start_time)
        ORDER BY PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', resp.time) DESC
        LIMIT 1
    ) as respiration_rate

FROM {{ ref('hub_garmin__sleep_timeseries') }}
CROSS JOIN UNNEST(sleep_levels_timeseries) as level
WHERE sleep_date IS NOT NULL
ORDER BY sleep_date DESC, timestamp_gmt ASC