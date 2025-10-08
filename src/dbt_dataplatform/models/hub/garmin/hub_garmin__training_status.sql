{{ config(
    materialized='incremental',
    unique_key='date',
    partition_by={'field': 'date', 'data_type': 'date'},
    cluster_by=['date'],
    dataset=get_schema('hub'),
    tags=["hub", "garmin"]
) }}

-- Hub model for Garmin training status data
-- Flattens nested device-keyed JSON objects into individual SQL columns
-- Uses hardcoded device ID (3457686856 - Forerunner 965)

SELECT
    DATE(JSON_VALUE(raw_data, '$.date')) as date,
    
    -- VO2Max data grouped in STRUCT
    STRUCT(
        DATE(JSON_VALUE(raw_data, '$.mostRecentVO2Max.generic.calendarDate')) as calendar_date,
        CAST(JSON_VALUE(raw_data, '$.mostRecentVO2Max.generic.vo2MaxPreciseValue') AS FLOAT64) as vo2max_precise_value,
        CAST(JSON_VALUE(raw_data, '$.mostRecentVO2Max.generic.vo2MaxValue') AS FLOAT64) as vo2max_value,
        CAST(JSON_VALUE(raw_data, '$.mostRecentVO2Max.generic.fitnessAge') AS INT64) as fitness_age,
        JSON_VALUE(raw_data, '$.mostRecentVO2Max.generic.fitnessAgeDescription') as fitness_age_description,
        CAST(JSON_VALUE(raw_data, '$.mostRecentVO2Max.generic.maxMetCategory') AS INT64) as max_met_category
    ) as vo2max,
    
    -- Heat/Altitude Acclimation data grouped in STRUCT
    STRUCT(
        DATE(JSON_VALUE(raw_data, '$.mostRecentVO2Max.heatAltitudeAcclimation.calendarDate')) as calendar_date,
        DATE(JSON_VALUE(raw_data, '$.mostRecentVO2Max.heatAltitudeAcclimation.altitudeAcclimationDate')) as altitude_acclimation_date,
        DATE(JSON_VALUE(raw_data, '$.mostRecentVO2Max.heatAltitudeAcclimation.previousAltitudeAcclimationDate')) as previous_altitude_acclimation_date,
        DATE(JSON_VALUE(raw_data, '$.mostRecentVO2Max.heatAltitudeAcclimation.heatAcclimationDate')) as heat_acclimation_date,
        DATE(JSON_VALUE(raw_data, '$.mostRecentVO2Max.heatAltitudeAcclimation.previousHeatAcclimationDate')) as previous_heat_acclimation_date,
        CAST(JSON_VALUE(raw_data, '$.mostRecentVO2Max.heatAltitudeAcclimation.altitudeAcclimation') AS INT64) as altitude_acclimation,
        CAST(JSON_VALUE(raw_data, '$.mostRecentVO2Max.heatAltitudeAcclimation.previousAltitudeAcclimation') AS INT64) as previous_altitude_acclimation,
        CAST(JSON_VALUE(raw_data, '$.mostRecentVO2Max.heatAltitudeAcclimation.heatAcclimationPercentage') AS INT64) as heat_acclimation_percentage,
        CAST(JSON_VALUE(raw_data, '$.mostRecentVO2Max.heatAltitudeAcclimation.previousHeatAcclimationPercentage') AS INT64) as previous_heat_acclimation_percentage,
        JSON_VALUE(raw_data, '$.mostRecentVO2Max.heatAltitudeAcclimation.heatTrend') as heat_trend,
        JSON_VALUE(raw_data, '$.mostRecentVO2Max.heatAltitudeAcclimation.altitudeTrend') as altitude_trend,
        CAST(JSON_VALUE(raw_data, '$.mostRecentVO2Max.heatAltitudeAcclimation.currentAltitude') AS INT64) as current_altitude,
        CAST(JSON_VALUE(raw_data, '$.mostRecentVO2Max.heatAltitudeAcclimation.previousAltitude') AS INT64) as previous_altitude,
        CAST(JSON_VALUE(raw_data, '$.mostRecentVO2Max.heatAltitudeAcclimation.acclimationPercentage') AS INT64) as acclimation_percentage,
        CAST(JSON_VALUE(raw_data, '$.mostRecentVO2Max.heatAltitudeAcclimation.previousAcclimationPercentage') AS INT64) as previous_acclimation_percentage,
        TIMESTAMP(JSON_VALUE(raw_data, '$.mostRecentVO2Max.heatAltitudeAcclimation.altitudeAcclimationLocalTimestamp')) as altitude_acclimation_local_timestamp
    ) as heat_altitude_acclimation,
    
    -- Training Load Balance - flattened fields from device 3457686856
    STRUCT(
        DATE(JSON_EXTRACT_SCALAR(TO_JSON_STRING(raw_data.mostRecentTrainingLoadBalance.metricsTrainingLoadBalanceDTOMap), '$.3457686856.calendarDate')) as calendar_date,
        CAST(JSON_EXTRACT_SCALAR(TO_JSON_STRING(raw_data.mostRecentTrainingLoadBalance.metricsTrainingLoadBalanceDTOMap), '$.3457686856.deviceId') AS INT64) as device_id,
        CAST(JSON_EXTRACT_SCALAR(TO_JSON_STRING(raw_data.mostRecentTrainingLoadBalance.metricsTrainingLoadBalanceDTOMap), '$.3457686856.monthlyLoadAerobicHigh') AS FLOAT64) as monthly_load_aerobic_high,
        CAST(JSON_EXTRACT_SCALAR(TO_JSON_STRING(raw_data.mostRecentTrainingLoadBalance.metricsTrainingLoadBalanceDTOMap), '$.3457686856.monthlyLoadAerobicHighTargetMax') AS INT64) as monthly_load_aerobic_high_target_max,
        CAST(JSON_EXTRACT_SCALAR(TO_JSON_STRING(raw_data.mostRecentTrainingLoadBalance.metricsTrainingLoadBalanceDTOMap), '$.3457686856.monthlyLoadAerobicHighTargetMin') AS INT64) as monthly_load_aerobic_high_target_min,
        CAST(JSON_EXTRACT_SCALAR(TO_JSON_STRING(raw_data.mostRecentTrainingLoadBalance.metricsTrainingLoadBalanceDTOMap), '$.3457686856.monthlyLoadAerobicLow') AS FLOAT64) as monthly_load_aerobic_low,
        CAST(JSON_EXTRACT_SCALAR(TO_JSON_STRING(raw_data.mostRecentTrainingLoadBalance.metricsTrainingLoadBalanceDTOMap), '$.3457686856.monthlyLoadAerobicLowTargetMax') AS INT64) as monthly_load_aerobic_low_target_max,
        CAST(JSON_EXTRACT_SCALAR(TO_JSON_STRING(raw_data.mostRecentTrainingLoadBalance.metricsTrainingLoadBalanceDTOMap), '$.3457686856.monthlyLoadAerobicLowTargetMin') AS INT64) as monthly_load_aerobic_low_target_min,
        CAST(JSON_EXTRACT_SCALAR(TO_JSON_STRING(raw_data.mostRecentTrainingLoadBalance.metricsTrainingLoadBalanceDTOMap), '$.3457686856.monthlyLoadAnaerobic') AS FLOAT64) as monthly_load_anaerobic,
        CAST(JSON_EXTRACT_SCALAR(TO_JSON_STRING(raw_data.mostRecentTrainingLoadBalance.metricsTrainingLoadBalanceDTOMap), '$.3457686856.monthlyLoadAnaerobicTargetMax') AS INT64) as monthly_load_anaerobic_target_max,
        CAST(JSON_EXTRACT_SCALAR(TO_JSON_STRING(raw_data.mostRecentTrainingLoadBalance.metricsTrainingLoadBalanceDTOMap), '$.3457686856.monthlyLoadAnaerobicTargetMin') AS INT64) as monthly_load_anaerobic_target_min,
        CAST(JSON_EXTRACT_SCALAR(TO_JSON_STRING(raw_data.mostRecentTrainingLoadBalance.metricsTrainingLoadBalanceDTOMap), '$.3457686856.primaryTrainingDevice') AS BOOL) as primary_training_device,
        JSON_EXTRACT_SCALAR(TO_JSON_STRING(raw_data.mostRecentTrainingLoadBalance.metricsTrainingLoadBalanceDTOMap), '$.3457686856.trainingBalanceFeedbackPhrase') as training_balance_feedback_phrase,
        raw_data.mostRecentTrainingLoadBalance.recordedDevices as recorded_devices
    ) as training_load_balance,

    -- Training Status - flattened fields from device 3457686856
    STRUCT(
        DATE(JSON_EXTRACT_SCALAR(TO_JSON_STRING(raw_data.mostRecentTrainingStatus.latestTrainingStatusData), '$.3457686856.calendarDate')) as calendar_date,
        CAST(JSON_EXTRACT_SCALAR(TO_JSON_STRING(raw_data.mostRecentTrainingStatus.latestTrainingStatusData), '$.3457686856.deviceId') AS INT64) as device_id,
        CAST(JSON_EXTRACT_SCALAR(TO_JSON_STRING(raw_data.mostRecentTrainingStatus.latestTrainingStatusData), '$.3457686856.fitnessTrend') AS INT64) as fitness_trend,
        JSON_EXTRACT_SCALAR(TO_JSON_STRING(raw_data.mostRecentTrainingStatus.latestTrainingStatusData), '$.3457686856.fitnessTrendSport') as fitness_trend_sport,
        JSON_EXTRACT_SCALAR(TO_JSON_STRING(raw_data.mostRecentTrainingStatus.latestTrainingStatusData), '$.3457686856.sport') as sport,
        JSON_EXTRACT_SCALAR(TO_JSON_STRING(raw_data.mostRecentTrainingStatus.latestTrainingStatusData), '$.3457686856.subSport') as sub_sport,
        DATE(JSON_EXTRACT_SCALAR(TO_JSON_STRING(raw_data.mostRecentTrainingStatus.latestTrainingStatusData), '$.3457686856.sinceDate')) as since_date,
        CAST(JSON_EXTRACT_SCALAR(TO_JSON_STRING(raw_data.mostRecentTrainingStatus.latestTrainingStatusData), '$.3457686856.trainingStatus') AS INT64) as training_status_value,
        JSON_EXTRACT_SCALAR(TO_JSON_STRING(raw_data.mostRecentTrainingStatus.latestTrainingStatusData), '$.3457686856.trainingStatusFeedbackPhrase') as training_status_feedback_phrase,
        CAST(JSON_EXTRACT_SCALAR(TO_JSON_STRING(raw_data.mostRecentTrainingStatus.latestTrainingStatusData), '$.3457686856.trainingPaused') AS BOOL) as training_paused,
        CAST(JSON_EXTRACT_SCALAR(TO_JSON_STRING(raw_data.mostRecentTrainingStatus.latestTrainingStatusData), '$.3457686856.primaryTrainingDevice') AS BOOL) as primary_training_device,
        TIMESTAMP_MILLIS(CAST(JSON_EXTRACT_SCALAR(TO_JSON_STRING(raw_data.mostRecentTrainingStatus.latestTrainingStatusData), '$.3457686856.timestamp') AS INT64)) as timestamp,
        CAST(JSON_EXTRACT_SCALAR(TO_JSON_STRING(raw_data.mostRecentTrainingStatus.latestTrainingStatusData), '$.3457686856.loadLevelTrend') AS INT64) as load_level_trend,
        CAST(JSON_EXTRACT_SCALAR(TO_JSON_STRING(raw_data.mostRecentTrainingStatus.latestTrainingStatusData), '$.3457686856.loadTunnelMax') AS FLOAT64) as load_tunnel_max,
        CAST(JSON_EXTRACT_SCALAR(TO_JSON_STRING(raw_data.mostRecentTrainingStatus.latestTrainingStatusData), '$.3457686856.loadTunnelMin') AS FLOAT64) as load_tunnel_min,
        CAST(JSON_EXTRACT_SCALAR(TO_JSON_STRING(raw_data.mostRecentTrainingStatus.latestTrainingStatusData), '$.3457686856.weeklyTrainingLoad') AS INT64) as weekly_training_load,
        -- Nested Acute Training Load as sub-STRUCT
        STRUCT(
            CAST(JSON_EXTRACT_SCALAR(TO_JSON_STRING(raw_data.mostRecentTrainingStatus.latestTrainingStatusData), '$.3457686856.acuteTrainingLoadDTO.acwrPercent') AS INT64) as acwr_percent,
            JSON_EXTRACT_SCALAR(TO_JSON_STRING(raw_data.mostRecentTrainingStatus.latestTrainingStatusData), '$.3457686856.acuteTrainingLoadDTO.acwrStatus') as acwr_status,
            JSON_EXTRACT_SCALAR(TO_JSON_STRING(raw_data.mostRecentTrainingStatus.latestTrainingStatusData), '$.3457686856.acuteTrainingLoadDTO.acwrStatusFeedback') as acwr_status_feedback,
            CAST(JSON_EXTRACT_SCALAR(TO_JSON_STRING(raw_data.mostRecentTrainingStatus.latestTrainingStatusData), '$.3457686856.acuteTrainingLoadDTO.dailyAcuteChronicWorkloadRatio') AS FLOAT64) as daily_acute_chronic_workload_ratio,
            CAST(JSON_EXTRACT_SCALAR(TO_JSON_STRING(raw_data.mostRecentTrainingStatus.latestTrainingStatusData), '$.3457686856.acuteTrainingLoadDTO.dailyTrainingLoadAcute') AS INT64) as daily_training_load_acute,
            CAST(JSON_EXTRACT_SCALAR(TO_JSON_STRING(raw_data.mostRecentTrainingStatus.latestTrainingStatusData), '$.3457686856.acuteTrainingLoadDTO.dailyTrainingLoadChronic') AS INT64) as daily_training_load_chronic,
            CAST(JSON_EXTRACT_SCALAR(TO_JSON_STRING(raw_data.mostRecentTrainingStatus.latestTrainingStatusData), '$.3457686856.acuteTrainingLoadDTO.maxTrainingLoadChronic') AS FLOAT64) as max_training_load_chronic,
            CAST(JSON_EXTRACT_SCALAR(TO_JSON_STRING(raw_data.mostRecentTrainingStatus.latestTrainingStatusData), '$.3457686856.acuteTrainingLoadDTO.minTrainingLoadChronic') AS FLOAT64) as min_training_load_chronic
        ) as acute_training_load,
        raw_data.mostRecentTrainingStatus.recordedDevices as recorded_devices,
        raw_data.mostRecentTrainingStatus.showSelector as show_selector,
        raw_data.mostRecentTrainingStatus.lastPrimarySyncDate as last_primary_sync_date
    ) as training_status,
    
    -- Metadata
    dp_inserted_at,
    source_file

FROM {{ ref('lake_garmin__svc_training_status') }}

{% if is_incremental() %}
WHERE dp_inserted_at > (SELECT MAX(dp_inserted_at) FROM {{ this }})
{% endif %}