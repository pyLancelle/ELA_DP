{{ config(dataset=get_schema('hub'), materialized='view', tags=["hub", "garmin"]) }}

-- Hub model for Garmin sleep time series data
-- Contains all detailed temporal data arrays for advanced sleep analysis
-- Use this model for minute-by-minute sleep tracking and detailed metrics

SELECT
    -- Core identifiers for joining with main sleep model
    CAST(JSON_VALUE(raw_data, '$.dailySleepDTO.id') AS INT64) as sleep_id,
    CAST(JSON_VALUE(raw_data, '$.dailySleepDTO.userProfilePK') AS INT64) as user_profile_pk,
    DATE(JSON_VALUE(raw_data, '$.dailySleepDTO.calendarDate')) as sleep_date,
    
    -- Movement and activity patterns
    JSON_QUERY(raw_data, '$.sleepMovement') as sleep_movement,
    JSON_QUERY(raw_data, '$.sleepLevels') as sleep_levels,
    JSON_QUERY(raw_data, '$.sleepRestlessMoments') as sleep_restless_moments,
    
    -- Wellness and health monitoring
    JSON_QUERY(raw_data, '$.wellnessSpO2SleepSummaryDTO') as wellness_spo2_summary,
    JSON_QUERY(raw_data, '$.wellnessEpochSPO2DataDTOList') as wellness_spo2_epochs,
    JSON_QUERY(raw_data, '$.wellnessEpochRespirationDataDTOList') as wellness_respiration_epochs,
    JSON_QUERY(raw_data, '$.wellnessEpochRespirationAveragesList') as wellness_respiration_averages,
    
    -- Physiological time series
    JSON_QUERY(raw_data, '$.sleepHeartRate') as sleep_heart_rate,
    JSON_QUERY(raw_data, '$.sleepStress') as sleep_stress,
    JSON_QUERY(raw_data, '$.sleepBodyBattery') as sleep_body_battery,
    JSON_QUERY(raw_data, '$.hrvData') as hrv_data,
    JSON_QUERY(raw_data, '$.breathingDisruptionData') as breathing_disruption_data,
    
    -- Metadata
    dp_inserted_at,
    source_file
    
FROM {{ ref('lake_garmin__sleep') }}