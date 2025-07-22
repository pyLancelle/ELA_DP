{{ config(dataset=get_schema('lake')) }}

-- Pure training status data extraction from staging_garmin_raw
-- Source: training_status data type from Garmin Connect API

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
    source_file

FROM {{ source('garmin', 'staging_garmin_raw') }}
WHERE data_type = 'training_status'
  AND JSON_EXTRACT_SCALAR(raw_data, '$.userId') IS NOT NULL