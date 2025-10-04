{{ config(
  materialized='incremental',
  unique_key=['activity_id', 'metric_point_index'],
  partition_by={'field': 'activity_date', 'data_type': 'date'},
  cluster_by=['activity_id', 'metric_point_index'],
  dataset=get_schema('hub'),
  tags=["hub", "garmin"])
}}

-- Hub model for Garmin running activities timeseries data
-- Contains detailed second-by-second metrics for running activities
-- Each row represents one metric point with 19 different measurements

WITH timeseries_extract AS (
  SELECT
    CAST(JSON_VALUE(raw_data, '$.activityId') AS INT64) as activity_id,
    CAST(JSON_VALUE(raw_data, '$.userProfileId') AS INT64) as user_id,
    CAST(JSON_VALUE(raw_data, '$.deviceId') AS INT64) as device_id,
    TIMESTAMP(JSON_VALUE(raw_data, '$.startTimeGMT')) as start_time_utc,
    DATE(TIMESTAMP(JSON_VALUE(raw_data, '$.startTimeGMT'))) as activity_date,

    -- Point temporel dans la séquence
    metric_point_index,

    -- Extraction des 26 métriques par point depuis raw_data
    -- Chaque point contient un object avec metrics array selon metricDescriptors
    CAST(JSON_VALUE(TO_JSON_STRING(metric_point), '$.metrics[0]') AS FLOAT64) as direct_vertical_oscillation_cm,
    CAST(JSON_VALUE(TO_JSON_STRING(metric_point), '$.metrics[1]') AS FLOAT64) as direct_available_stamina,
    CAST(JSON_VALUE(TO_JSON_STRING(metric_point), '$.metrics[2]') AS FLOAT64) as direct_fractional_cadence,
    CAST(JSON_VALUE(TO_JSON_STRING(metric_point), '$.metrics[3]') AS FLOAT64) as direct_double_cadence,
    CAST(JSON_VALUE(TO_JSON_STRING(metric_point), '$.metrics[4]') AS FLOAT64) as direct_body_battery,
    CAST(JSON_VALUE(TO_JSON_STRING(metric_point), '$.metrics[5]') AS FLOAT64) as sum_moving_duration_ms,
    CAST(JSON_VALUE(TO_JSON_STRING(metric_point), '$.metrics[6]') AS FLOAT64) as direct_run_cadence,
    CAST(JSON_VALUE(TO_JSON_STRING(metric_point), '$.metrics[7]') AS FLOAT64) as sum_accumulated_power_watts,
    CAST(JSON_VALUE(TO_JSON_STRING(metric_point), '$.metrics[8]') AS FLOAT64) as sum_distance_cm,
    CAST(JSON_VALUE(TO_JSON_STRING(metric_point), '$.metrics[9]') AS FLOAT64) as direct_potential_stamina,
    SAFE_CAST(JSON_VALUE(TO_JSON_STRING(metric_point), '$.metrics[15]') AS INT64) as direct_timestamp_gmt,
    CAST(JSON_VALUE(TO_JSON_STRING(metric_point), '$.metrics[16]') AS FLOAT64) as direct_speed_mps_x10,
    CAST(JSON_VALUE(TO_JSON_STRING(metric_point), '$.metrics[10]') AS FLOAT64) as sum_elapsed_duration_ms,
    CAST(JSON_VALUE(TO_JSON_STRING(metric_point), '$.metrics[17]') AS FLOAT64) as direct_power_watts,
    CAST(JSON_VALUE(TO_JSON_STRING(metric_point), '$.metrics[14]') AS FLOAT64) as direct_heart_rate_bpm,
    CAST(JSON_VALUE(TO_JSON_STRING(metric_point), '$.metrics[11]') AS FLOAT64) as sum_duration_ms,
    CAST(JSON_VALUE(TO_JSON_STRING(metric_point), '$.metrics[8]') AS FLOAT64) as direct_stride_length_cm,
    CAST(JSON_VALUE(TO_JSON_STRING(metric_point), '$.metrics[18]') AS FLOAT64) as direct_vertical_ratio,
    CAST(JSON_VALUE(TO_JSON_STRING(metric_point), '$.metrics[12]') AS FLOAT64) as direct_ground_contact_time_ms,

    dp_inserted_at

  FROM {{ ref('lake_garmin__svc_activity_details') }},
  UNNEST(JSON_QUERY_ARRAY(JSON_QUERY(raw_data, '$.detailed_data.activityDetailMetrics'))) AS metric_point
    WITH OFFSET AS metric_point_index
  WHERE JSON_VALUE(raw_data, '$.activityType.typeKey') = 'running'
  {% if is_incremental() %}
    AND dp_inserted_at > (SELECT MAX(dp_inserted_at) FROM {{ this }})
  {% endif %}
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

  dp_inserted_at

FROM timeseries_extract
WHERE direct_timestamp_gmt IS NOT NULL