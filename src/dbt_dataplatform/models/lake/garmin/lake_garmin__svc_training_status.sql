{{ config(
    dataset=get_schema('lake'),
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='training_status_date'
) }}

-- Pure training status data extraction from staging_garmin_raw
-- Source: training_status data type from Garmin Connect API
-- Deduplicates by keeping most recent record per date

WITH training_status_data_with_rank AS (
  SELECT
    -- User and date identifiers  
    SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.userId') AS INT64) AS user_id,
    DATE(SAFE.PARSE_TIMESTAMP('%Y-%m-%d', JSON_EXTRACT_SCALAR(raw_data, '$.date'))) AS training_status_date,
    SAFE.PARSE_DATE('%Y-%m-%d', JSON_EXTRACT_SCALAR(raw_data, '$.mostRecentTrainingStatus.lastPrimarySyncDate')) AS last_primary_sync_date,
    
    -- VO2 Max data
    SAFE.PARSE_DATE('%Y-%m-%d', JSON_EXTRACT_SCALAR(raw_data, '$.mostRecentVO2Max.generic.calendarDate')) AS vo2_max_date,
    SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.mostRecentVO2Max.generic.vo2MaxValue') AS FLOAT64) AS vo2_max_value,
    SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.mostRecentVO2Max.generic.vo2MaxPreciseValue') AS FLOAT64) AS vo2_max_precise_value,
    SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.mostRecentVO2Max.generic.fitnessAge') AS INT64) AS fitness_age,
    JSON_EXTRACT_SCALAR(raw_data, '$.mostRecentVO2Max.generic.fitnessAgeDescription') AS fitness_age_description,
    
    -- Heat and altitude acclimatization
    SAFE.PARSE_DATE('%Y-%m-%d', JSON_EXTRACT_SCALAR(raw_data, '$.mostRecentVO2Max.heatAltitudeAcclimation.calendarDate')) AS acclimatization_date,
    SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.mostRecentVO2Max.heatAltitudeAcclimation.heatAcclimationPercentage') AS INT64) AS heat_acclimation_percentage,
    SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.mostRecentVO2Max.heatAltitudeAcclimation.altitudeAcclimation') AS INT64) AS altitude_acclimation,
    SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.mostRecentVO2Max.heatAltitudeAcclimation.currentAltitude') AS INT64) AS current_altitude,
    JSON_EXTRACT_SCALAR(raw_data, '$.mostRecentVO2Max.heatAltitudeAcclimation.heatTrend') AS heat_trend,
    JSON_EXTRACT_SCALAR(raw_data, '$.mostRecentVO2Max.heatAltitudeAcclimation.altitudeTrend') AS altitude_trend,
    
    -- Training load balance (for primary device)
    SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.mostRecentTrainingLoadBalance.metricsTrainingLoadBalanceDTOMap.3457686856.monthlyLoadAerobicLow') AS FLOAT64) AS monthly_load_aerobic_low,
    SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.mostRecentTrainingLoadBalance.metricsTrainingLoadBalanceDTOMap.3457686856.monthlyLoadAerobicHigh') AS FLOAT64) AS monthly_load_aerobic_high,
    SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.mostRecentTrainingLoadBalance.metricsTrainingLoadBalanceDTOMap.3457686856.monthlyLoadAnaerobic') AS FLOAT64) AS monthly_load_anaerobic,
    JSON_EXTRACT_SCALAR(raw_data, '$.mostRecentTrainingLoadBalance.metricsTrainingLoadBalanceDTOMap.3457686856.trainingBalanceFeedbackPhrase') AS training_balance_feedback,
    
    -- Current training status (for primary device)
    SAFE.PARSE_DATE('%Y-%m-%d', JSON_EXTRACT_SCALAR(raw_data, '$.mostRecentTrainingStatus.latestTrainingStatusData.3457686856.calendarDate')) AS training_status_calendar_date,
    SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.mostRecentTrainingStatus.latestTrainingStatusData.3457686856.trainingStatus') AS INT64) AS training_status_code,
    SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.mostRecentTrainingStatus.latestTrainingStatusData.3457686856.fitnessTrend') AS INT64) AS fitness_trend_code,
    JSON_EXTRACT_SCALAR(raw_data, '$.mostRecentTrainingStatus.latestTrainingStatusData.3457686856.trainingStatusFeedbackPhrase') AS training_status_feedback,
    JSON_EXTRACT_SCALAR(raw_data, '$.mostRecentTrainingStatus.latestTrainingStatusData.3457686856.sport') AS training_sport,
    JSON_EXTRACT_SCALAR(raw_data, '$.mostRecentTrainingStatus.latestTrainingStatusData.3457686856.subSport') AS training_sub_sport,
    SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.mostRecentTrainingStatus.latestTrainingStatusData.3457686856.trainingPaused') AS BOOL) AS training_paused,
    
    -- Acute/Chronic Workload Ratio
    SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.mostRecentTrainingStatus.latestTrainingStatusData.3457686856.acuteTrainingLoadDTO.acwrPercent') AS INT64) AS acwr_percent,
    JSON_EXTRACT_SCALAR(raw_data, '$.mostRecentTrainingStatus.latestTrainingStatusData.3457686856.acuteTrainingLoadDTO.acwrStatus') AS acwr_status,
    SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.mostRecentTrainingStatus.latestTrainingStatusData.3457686856.acuteTrainingLoadDTO.dailyTrainingLoadAcute') AS FLOAT64) AS daily_training_load_acute,
    SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.mostRecentTrainingStatus.latestTrainingStatusData.3457686856.acuteTrainingLoadDTO.dailyTrainingLoadChronic') AS FLOAT64) AS daily_training_load_chronic,
    SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.mostRecentTrainingStatus.latestTrainingStatusData.3457686856.acuteTrainingLoadDTO.dailyAcuteChronicWorkloadRatio') AS FLOAT64) AS daily_acwr,
    
    -- Complex nested data preserved for hub layer processing
    JSON_EXTRACT(raw_data, '$.mostRecentVO2Max') AS vo2_max_full_json,
    JSON_EXTRACT(raw_data, '$.mostRecentTrainingLoadBalance') AS training_load_balance_full_json,
    JSON_EXTRACT(raw_data, '$.mostRecentTrainingStatus') AS training_status_full_json,
    JSON_EXTRACT(raw_data, '$.mostRecentTrainingStatus.recordedDevices') AS recorded_devices_json,
    
    -- Source metadata
    dp_inserted_at,
    source_file,
    
    -- Add row number to deduplicate by most recent dp_inserted_at
    ROW_NUMBER() OVER (
      PARTITION BY DATE(SAFE.PARSE_TIMESTAMP('%Y-%m-%d', JSON_EXTRACT_SCALAR(raw_data, '$.date')))
      ORDER BY dp_inserted_at DESC
    ) AS row_rank

  FROM {{ source('garmin', 'lake_garmin__stg_garmin_raw') }}
  WHERE data_type = 'training_status'
    AND JSON_EXTRACT_SCALAR(raw_data, '$.userId') IS NOT NULL
    
  {% if is_incremental() %}
    AND dp_inserted_at > (SELECT MAX(dp_inserted_at) FROM {{ this }})
  {% endif %}
)

SELECT
  user_id,
  training_status_date,
  last_primary_sync_date,
  vo2_max_date,
  vo2_max_value,
  vo2_max_precise_value,
  fitness_age,
  fitness_age_description,
  acclimatization_date,
  heat_acclimation_percentage,
  altitude_acclimation,
  current_altitude,
  heat_trend,
  altitude_trend,
  monthly_load_aerobic_low,
  monthly_load_aerobic_high,
  monthly_load_anaerobic,
  training_balance_feedback,
  training_status_calendar_date,
  training_status_code,
  fitness_trend_code,
  training_status_feedback,
  training_sport,
  training_sub_sport,
  training_paused,
  acwr_percent,
  acwr_status,
  daily_training_load_acute,
  daily_training_load_chronic,
  daily_acwr,
  vo2_max_full_json,
  training_load_balance_full_json,
  training_status_full_json,
  recorded_devices_json,
  dp_inserted_at,
  source_file

FROM training_status_data_with_rank
WHERE row_rank = 1