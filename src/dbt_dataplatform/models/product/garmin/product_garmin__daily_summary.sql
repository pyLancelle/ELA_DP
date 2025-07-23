{{ config(dataset=get_schema('product'), materialized='view', tags=["product", "garmin"]) }}

-- Product model for daily Garmin summary
-- Simple KPIs ready for frontend consumption - one row per day

WITH daily_activities AS (
  SELECT 
    DATE(start_time_local) as activity_date,
    COUNT(*) as activity_count,
    SUM(distance_meters) / 1000.0 as total_distance_km,
    SUM(duration_seconds) / 3600.0 as total_duration_hours,
    SUM(calories) as total_calories,
    AVG(average_heart_rate) as avg_heart_rate
  FROM {{ ref('hub_garmin__activities') }}
  WHERE start_time_local IS NOT NULL
  GROUP BY DATE(start_time_local)
),

daily_sleep AS (
  SELECT 
    DATE(calendar_date) as sleep_date,
    sleep_time_seconds / 3600.0 as sleep_duration_hours,
    sleep_efficiency_percent,
    overall_sleep_score,
    EXTRACT(HOUR FROM TIMESTAMP_MILLIS(CAST(sleep_start_timestamp_local AS INT64))) as bedtime_hour,
    EXTRACT(HOUR FROM TIMESTAMP_MILLIS(CAST(sleep_end_timestamp_local AS INT64))) as waketime_hour
  FROM {{ ref('hub_garmin__sleep') }}
  WHERE calendar_date IS NOT NULL
),

daily_steps AS (
  SELECT 
    EXTRACT(DATE FROM interval_start_gmt) as step_date,
    SUM(steps) as total_steps
  FROM {{ ref('hub_garmin__steps_daily') }}
  WHERE steps IS NOT NULL
  GROUP BY EXTRACT(DATE FROM interval_start_gmt)
),

daily_weight AS (
  SELECT 
    weight_date,
    AVG(weight_kg) as avg_weight_kg,
    AVG(bmi) as avg_bmi,
    AVG(body_fat_percent) as avg_body_fat_percent,
    MAX(is_withings_data) as has_withings_data
  FROM {{ ref('hub_garmin__weight') }}
  WHERE weight_kg IS NOT NULL
  GROUP BY weight_date
),

date_spine AS (
  SELECT DISTINCT date_value as summary_date
  FROM (
    SELECT activity_date as date_value FROM daily_activities
    UNION DISTINCT
    SELECT sleep_date as date_value FROM daily_sleep WHERE sleep_date IS NOT NULL
    UNION DISTINCT 
    SELECT step_date as date_value FROM daily_steps WHERE step_date IS NOT NULL
    UNION DISTINCT
    SELECT weight_date as date_value FROM daily_weight WHERE weight_date IS NOT NULL
  )
  WHERE date_value >= DATE_SUB(CURRENT_DATE(), INTERVAL 365 DAY)
)

SELECT
  ds.summary_date,
  
  -- Sleep metrics
  COALESCE(sl.sleep_duration_hours, 0) as sleep_duration_hours,
  sl.sleep_efficiency_percent,
  sl.overall_sleep_score,
  sl.bedtime_hour,
  sl.waketime_hour,
  
  -- Activity metrics
  COALESCE(da.activity_count, 0) as activity_count,
  COALESCE(da.total_distance_km, 0) as total_distance_km,
  COALESCE(da.total_duration_hours, 0) as total_activity_hours,
  COALESCE(da.total_calories, 0) as total_calories,
  da.avg_heart_rate,
  
  -- Steps
  COALESCE(dst.total_steps, 0) as total_steps,
  
  -- Weight & body composition
  dw.avg_weight_kg,
  dw.avg_bmi,
  dw.avg_body_fat_percent,
  
  -- Data availability flags
  da.activity_count > 0 as has_activity_data,
  sl.sleep_duration_hours > 0 as has_sleep_data,
  dst.total_steps > 0 as has_steps_data,
  dw.avg_weight_kg IS NOT NULL as has_weight_data,
  COALESCE(dw.has_withings_data, FALSE) as has_withings_data,
  
  -- Day of week for patterns
  EXTRACT(DAYOFWEEK FROM ds.summary_date) as day_of_week,
  FORMAT_DATE('%A', ds.summary_date) as day_name,
  
  -- Week and month for aggregations
  DATE_TRUNC(ds.summary_date, WEEK(MONDAY)) as week_start_date,
  DATE_TRUNC(ds.summary_date, MONTH) as month_start_date

FROM date_spine ds
LEFT JOIN daily_sleep sl ON ds.summary_date = sl.sleep_date
LEFT JOIN daily_activities da ON ds.summary_date = da.activity_date  
LEFT JOIN daily_steps dst ON ds.summary_date = dst.step_date
LEFT JOIN daily_weight dw ON ds.summary_date = dw.weight_date

WHERE ds.summary_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 365 DAY)
  AND ds.summary_date <= CURRENT_DATE()

ORDER BY ds.summary_date DESC