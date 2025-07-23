{{ config(dataset=get_schema('hub'), materialized='view', tags=["hub", "garmin"]) }}

-- Hub model for Garmin training status data - simple field mapping
-- Direct extraction of JSON fields without complex transformations

SELECT
    -- Root level fields
    CAST(JSON_VALUE(raw_data, '$.userId') AS INT64) as user_id,
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
    
    -- Training Load Balance as STRUCT
    STRUCT(
        JSON_QUERY(raw_data, '$.mostRecentTrainingLoadBalance.metricsTrainingLoadBalanceDTOMap') as balance_data_map,
        JSON_QUERY(raw_data, '$.mostRecentTrainingLoadBalance.recordedDevices') as recorded_devices
    ) as training_load_balance,
    
    -- Training Status as STRUCT
    STRUCT(
        JSON_QUERY(raw_data, '$.mostRecentTrainingStatus.latestTrainingStatusData') as status_data_map,
        JSON_QUERY(raw_data, '$.mostRecentTrainingStatus.recordedDevices') as recorded_devices,
        CAST(JSON_VALUE(raw_data, '$.mostRecentTrainingStatus.showSelector') AS BOOLEAN) as show_selector,
        DATE(JSON_VALUE(raw_data, '$.mostRecentTrainingStatus.lastPrimarySyncDate')) as last_primary_sync_date
    ) as training_status,
    
    -- Metadata
    dp_inserted_at,
    source_file
    
FROM {{ ref('lake_garmin__training_status') }}