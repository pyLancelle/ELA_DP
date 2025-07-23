{{ config(dataset=get_schema('product'), materialized='view', tags=["product", "garmin"]) }}

-- Product model for weekly Garmin summary  
-- Simple weekly KPIs ready for frontend consumption

SELECT
  week_start_date,
  CONCAT(FORMAT_DATE('%b %e', week_start_date), ' - ', FORMAT_DATE('%b %e', DATE_ADD(week_start_date, INTERVAL 6 DAY))) as week_label,
  
  -- Sleep metrics (weekly averages)
  ROUND(AVG(sleep_duration_hours), 1) as avg_sleep_duration_hours,
  ROUND(AVG(sleep_efficiency_percent), 1) as avg_sleep_efficiency_percent,
  ROUND(AVG(overall_sleep_score), 0) as avg_sleep_score,
  COUNT(CASE WHEN has_sleep_data THEN 1 END) as days_with_sleep_data,
  
  -- Activity metrics (weekly totals and averages)
  SUM(activity_count) as total_activities,
  ROUND(SUM(total_distance_km), 1) as total_distance_km,
  ROUND(SUM(total_activity_hours), 1) as total_activity_hours,
  SUM(total_calories) as total_calories,
  ROUND(AVG(avg_heart_rate), 0) as avg_heart_rate,
  COUNT(CASE WHEN has_activity_data THEN 1 END) as days_with_activities,
  
  -- Steps (weekly totals and daily average)
  SUM(total_steps) as total_steps,
  ROUND(AVG(total_steps), 0) as avg_daily_steps,
  COUNT(CASE WHEN has_steps_data THEN 1 END) as days_with_steps_data,
  
  -- Weight (weekly average - latest values)
  AVG(avg_weight_kg) as avg_weight_kg,
  AVG(avg_bmi) as avg_bmi,
  AVG(avg_body_fat_percent) as avg_body_fat_percent,
  COUNT(CASE WHEN has_weight_data THEN 1 END) as days_with_weight_data,
  
  -- Weekly goals tracking (example thresholds)
  SUM(total_steps) >= 70000 as met_weekly_steps_goal, -- 10k steps/day * 7 days
  COUNT(CASE WHEN has_activity_data THEN 1 END) >= 3 as met_weekly_activity_goal, -- 3+ activities/week
  AVG(sleep_duration_hours) >= 7.5 as met_weekly_sleep_goal, -- 7.5h+ average sleep
  
  -- Data completeness
  COUNT(*) as days_in_week,
  ROUND(COUNT(CASE WHEN has_sleep_data THEN 1 END) / COUNT(*) * 100, 0) as sleep_data_completeness_percent,
  ROUND(COUNT(CASE WHEN has_steps_data THEN 1 END) / COUNT(*) * 100, 0) as steps_data_completeness_percent

FROM {{ ref('product_garmin__daily_summary') }}
WHERE week_start_date IS NOT NULL
GROUP BY week_start_date
ORDER BY week_start_date DESC