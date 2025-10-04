{{ config(
    materialized='incremental',
    unique_key='activity_id',
    partition_by={'field': 'activity_date', 'data_type': 'date'},
    cluster_by=['activity_id'],
    dataset=get_schema('hub'),
    tags=["hub", "garmin"]
) }}

-- Hub model for Garmin running activities metrics
-- Contains aggregated and summary metrics for each running activity

SELECT
  -- Identifiants source
  JSON_VALUE(raw_data, '$.activityId') as activity_id,

  -- Timestamps source
  DATE(TIMESTAMP(JSON_VALUE(raw_data, '$.startTimeGMT'))) as activity_date,
  TIMESTAMP(JSON_VALUE(raw_data, '$.startTimeGMT')) as start_time_utc,
  TIMESTAMP(JSON_VALUE(raw_data, '$.startTimeLocal')) as start_time_local,

  -- Métadonnées activité
  JSON_VALUE(raw_data, '$.activityName') as activity_name,
  JSON_VALUE(raw_data, '$.activityType.typeKey') as activity_type,
  CAST(JSON_VALUE(raw_data, '$.sportTypeId') AS INT64) as sport_type_id,
  JSON_VALUE(raw_data, '$.manufacturer') as manufacturer,

  -- Struct durées (valeurs brutes secondes)
  STRUCT(
    CAST(JSON_VALUE(raw_data, '$.duration') AS FLOAT64) as duration_seconds,
    CAST(JSON_VALUE(raw_data, '$.elapsedDuration') AS FLOAT64) as elapsed_duration_seconds,
    CAST(JSON_VALUE(raw_data, '$.movingDuration') AS FLOAT64) as moving_duration_seconds
  ) as duration_raw,

  -- Struct distance et vitesse (valeurs brutes sources)
  STRUCT(
    CAST(JSON_VALUE(raw_data, '$.distance') AS FLOAT64) as distance_meters,
    CAST(JSON_VALUE(raw_data, '$.averageSpeed') AS FLOAT64) as average_speed,
    CAST(JSON_VALUE(raw_data, '$.maxSpeed') AS FLOAT64) as max_speed,
    CAST(JSON_VALUE(raw_data, '$.minActivityLapDuration') AS FLOAT64) as min_activity_lap_duration
  ) as distance_speed_raw,

  -- Struct fréquence cardiaque (valeurs brutes sources)
  STRUCT(
    CAST(JSON_VALUE(raw_data, '$.averageHR') AS INT64) as average_hr,
    CAST(JSON_VALUE(raw_data, '$.maxHR') AS INT64) as max_hr,
    CAST(JSON_VALUE(raw_data, '$.timeInHRZone1') AS FLOAT64) as hr_zone_1_time_s,
    CAST(JSON_VALUE(raw_data, '$.timeInHRZone2') AS FLOAT64) as hr_zone_2_time_s,
    CAST(JSON_VALUE(raw_data, '$.timeInHRZone3') AS FLOAT64) as hr_zone_3_time_s,
    CAST(JSON_VALUE(raw_data, '$.timeInHRZone4') AS FLOAT64) as hr_zone_4_time_s,
    CAST(JSON_VALUE(raw_data, '$.timeInHRZone5') AS FLOAT64) as hr_zone_5_time_s
  ) as heart_rate_raw,

  -- Struct running spécifique (valeurs brutes sources)
  STRUCT(
    CAST(JSON_VALUE(raw_data, '$.averageRunningCadenceInStepsPerMinute') AS FLOAT64) as avg_cadence_spm,
    CAST(JSON_VALUE(raw_data, '$.maxRunningCadenceInStepsPerMinute') AS FLOAT64) as max_cadence_spm,
    CAST(JSON_VALUE(raw_data, '$.steps') AS INT64) as steps,
    CAST(JSON_VALUE(raw_data, '$.averageStrideLength') AS FLOAT64) as average_stride_length,
    CAST(JSON_VALUE(raw_data, '$.averageVerticalOscillation') AS FLOAT64) as average_vertical_oscillation,
    CAST(JSON_VALUE(raw_data, '$.averageVerticalRatio') AS FLOAT64) as average_vertical_ratio,
    CAST(JSON_VALUE(raw_data, '$.averageGroundContactTime') AS FLOAT64) as average_ground_contact_time
  ) as running_biomechanics_raw,

  -- Struct entraînement (valeurs brutes sources)
  STRUCT(
    CAST(JSON_VALUE(raw_data, '$.trainingLoad') AS FLOAT64) as training_load,
    CAST(JSON_VALUE(raw_data, '$.trainingStressScore') AS FLOAT64) as training_stress_score,
    CAST(JSON_VALUE(raw_data, '$.aerobicTrainingEffect') AS FLOAT64) as aerobic_training_effect,
    CAST(JSON_VALUE(raw_data, '$.anaerobicTrainingEffect') AS FLOAT64) as anaerobic_training_effect,
    JSON_VALUE(raw_data, '$.trainingEffectLabel') as training_effect_label
  ) as training_metrics_raw,

  -- Struct calories et énergie (valeurs brutes sources)
  STRUCT(
    CAST(JSON_VALUE(raw_data, '$.calories') AS FLOAT64) as calories,
    CAST(JSON_VALUE(raw_data, '$.bmrCalories') AS FLOAT64) as bmr_calories,
    CAST(JSON_VALUE(raw_data, '$.differenceBodyBattery') AS INT64) as difference_body_battery,
    CAST(JSON_VALUE(raw_data, '$.waterConsumed') AS FLOAT64) as water_consumed,
    CAST(JSON_VALUE(raw_data, '$.waterEstimated') AS FLOAT64) as water_estimated
  ) as energy_raw,

  -- Struct intensité (valeurs brutes sources)
  STRUCT(
    CAST(JSON_VALUE(raw_data, '$.moderateIntensityMinutes') AS FLOAT64) as moderate_intensity_minutes,
    CAST(JSON_VALUE(raw_data, '$.vigorousIntensityMinutes') AS FLOAT64) as vigorous_intensity_minutes
  ) as intensity_raw,

  -- Struct puissance (valeurs brutes sources)
  STRUCT(
    CAST(JSON_VALUE(raw_data, '$.averagePower') AS FLOAT64) as average_power,
    CAST(JSON_VALUE(raw_data, '$.maxPower') AS FLOAT64) as max_power,
    CAST(JSON_VALUE(raw_data, '$.normPower') AS FLOAT64) as norm_power,
    CAST(JSON_VALUE(raw_data, '$.timeInPowerZone1') AS FLOAT64) as power_zone_1_time_s,
    CAST(JSON_VALUE(raw_data, '$.timeInPowerZone2') AS FLOAT64) as power_zone_2_time_s,
    CAST(JSON_VALUE(raw_data, '$.timeInPowerZone3') AS FLOAT64) as power_zone_3_time_s,
    CAST(JSON_VALUE(raw_data, '$.timeInPowerZone4') AS FLOAT64) as power_zone_4_time_s,
    CAST(JSON_VALUE(raw_data, '$.timeInPowerZone5') AS FLOAT64) as power_zone_5_time_s
  ) as power_raw,

  -- Struct environnemental (si disponible)
  STRUCT(
    CAST(JSON_VALUE(raw_data, '$.temperature') AS FLOAT64) as temperature
  ) as environmental_raw,

  -- Métadonnées techniques
  STRUCT(
    CAST(JSON_VALUE(raw_data, '$.lapCount') AS INT64) as lap_count,
    CAST(JSON_VALUE(raw_data, '$.hasHeartRateData') AS BOOL) as has_heart_rate_data,
    CAST(JSON_VALUE(raw_data, '$.hasPolyline') AS BOOL) as has_polyline,
    CAST(JSON_VALUE(raw_data, '$.hasSplits') AS BOOL) as has_splits,
    CAST(JSON_VALUE(raw_data, '$.elevationCorrected') AS BOOL) as elevation_corrected,
    CAST(JSON_VALUE(raw_data, '$.manualActivity') AS BOOL) as manual_activity
  ) as technical_metadata,

  dp_inserted_at
FROM {{ ref('lake_garmin__svc_activity_details') }}
WHERE JSON_VALUE(raw_data, '$.activityType.typeKey') like '%running%'
{% if is_incremental() %}
  AND dp_inserted_at > (SELECT MAX(dp_inserted_at) FROM {{ this }})
{% endif %}