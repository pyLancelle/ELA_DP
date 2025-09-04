# ne marche pas, il faut utiliser ce qui est stocké dans raw_data
{{ config(
  materialized='table', 
  partition_by={'field': 'activity_date', 'data_type': 'date'},
  cluster_by=['user_id', 'activity_id'],
  dataset=get_schema('hub'),
  tags=["hub", "garmin"])
}}

WITH timeseries_extract AS (
  SELECT 
    activity_id,
    user_id,
    device_id,
    start_time_utc,
    DATE(start_time_utc) as activity_date,
    
    -- Point temporel dans la séquence
    metric_point_index,
    
    -- Extraction des 19 métriques par point depuis detailed_data
    -- Chaque point contient un array de 19 valeurs selon metricDescriptors
    CAST(JSON_EXTRACT_SCALAR(metric_point, '$[0]') AS FLOAT64) as direct_vertical_oscillation_cm,
    CAST(JSON_EXTRACT_SCALAR(metric_point, '$[1]') AS FLOAT64) as direct_available_stamina,
    CAST(JSON_EXTRACT_SCALAR(metric_point, '$[2]') AS FLOAT64) as direct_fractional_cadence,
    CAST(JSON_EXTRACT_SCALAR(metric_point, '$[3]') AS FLOAT64) as direct_double_cadence,
    CAST(JSON_EXTRACT_SCALAR(metric_point, '$[4]') AS FLOAT64) as direct_body_battery,
    CAST(JSON_EXTRACT_SCALAR(metric_point, '$[5]') AS FLOAT64) as sum_moving_duration_ms,
    CAST(JSON_EXTRACT_SCALAR(metric_point, '$[6]') AS FLOAT64) as direct_run_cadence,
    CAST(JSON_EXTRACT_SCALAR(metric_point, '$[7]') AS FLOAT64) as sum_accumulated_power_watts,
    CAST(JSON_EXTRACT_SCALAR(metric_point, '$[8]') AS FLOAT64) as sum_distance_cm,
    CAST(JSON_EXTRACT_SCALAR(metric_point, '$[9]') AS FLOAT64) as direct_potential_stamina,
    CAST(JSON_EXTRACT_SCALAR(metric_point, '$[10]') AS INT64) as direct_timestamp_gmt,
    CAST(JSON_EXTRACT_SCALAR(metric_point, '$[11]') AS FLOAT64) as direct_speed_mps_x10,
    CAST(JSON_EXTRACT_SCALAR(metric_point, '$[12]') AS FLOAT64) as sum_elapsed_duration_ms,
    CAST(JSON_EXTRACT_SCALAR(metric_point, '$[13]') AS FLOAT64) as direct_power_watts,
    CAST(JSON_EXTRACT_SCALAR(metric_point, '$[14]') AS FLOAT64) as direct_heart_rate_bpm,
    CAST(JSON_EXTRACT_SCALAR(metric_point, '$[15]') AS FLOAT64) as sum_duration_ms,
    CAST(JSON_EXTRACT_SCALAR(metric_point, '$[16]') AS FLOAT64) as direct_stride_length_cm,
    CAST(JSON_EXTRACT_SCALAR(metric_point, '$[17]') AS FLOAT64) as direct_vertical_ratio,
    CAST(JSON_EXTRACT_SCALAR(metric_point, '$[18]') AS FLOAT64) as direct_ground_contact_time_ms,
    
    loaded_at

  FROM {{ ref('lake_garmin__svc_activity_details') }},
  UNNEST(JSON_EXTRACT_ARRAY(detailed_data, '$.activityDetailMetrics')) 
    WITH OFFSET AS metric_point_index
  WHERE JSON_EXTRACT_SCALAR(activity_type, '$.typeKey') = 'running'
)

SELECT 
  activity_id,
  user_id,
  device_id,
  activity_date,
  start_time_utc,
  metric_point_index,
  
  -- Struct temporel (timestamps et durées brutes)
  STRUCT(
    direct_timestamp_gmt,
    sum_moving_duration_ms,
    sum_elapsed_duration_ms,
    sum_duration_ms
  ) as temporal_raw,
  
  -- Struct physiologique (valeurs brutes capteurs)
  STRUCT(
    direct_heart_rate_bpm,
    direct_body_battery,
    direct_available_stamina,
    direct_potential_stamina
  ) as physiological_raw,
  
  -- Struct biomécanique running (valeurs brutes capteurs)
  STRUCT(
    direct_run_cadence as cadence_spm,
    direct_fractional_cadence,
    direct_double_cadence,
    direct_stride_length_cm,
    direct_vertical_oscillation_cm,
    direct_vertical_ratio,
    direct_ground_contact_time_ms
  ) as biomechanical_raw,
  
  -- Struct vitesse et distance (valeurs brutes capteurs avec facteurs)
  STRUCT(
    direct_speed_mps_x10, -- Facteur 0.1 selon metricDescriptors
    sum_distance_cm,      -- Facteur 100 selon metricDescriptors  
    sum_accumulated_power_watts
  ) as performance_raw,
  
  -- Struct puissance (valeurs brutes capteurs)
  STRUCT(
    direct_power_watts,
    sum_accumulated_power_watts
  ) as power_raw,
  
  loaded_at

FROM timeseries_extract
WHERE direct_timestamp_gmt IS NOT NULL
ORDER BY activity_id, metric_point_index;