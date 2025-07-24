{{ config(dataset=get_schema('hub'), materialized='view', tags=["hub", "garmin"]) }}

-- Hub model for Garmin stress data with STRUCT organization
-- Transforms raw stress JSON into structured format with logical groupings

SELECT
    -- Primary identifiers
    DATE(JSON_VALUE(raw_data, '$.date')) as stress_date,
    CAST(JSON_VALUE(raw_data, '$.userId') AS INT64) as user_profile_pk,
    
    -- Summary metrics
    CAST(JSON_VALUE(raw_data, '$.avgStressLevel') AS INT64) as avg_stress_level,
    
    -- Body Battery descriptors
    JSON_QUERY(raw_data, '$.bodyBatteryValueDescriptorsDTOList') as body_battery_descriptors,
    
    -- Body Battery timeseries as JSON array (for detailed analysis)
    JSON_QUERY(raw_data, '$.bodyBatteryValuesArray') as body_battery_timeseries,
    
    -- Stress timeseries as JSON array (if available)
    JSON_QUERY(raw_data, '$.stressValuesArray') as stress_timeseries,
    
    -- Metadata
    dp_inserted_at,
    source_file

FROM {{ ref('lake_garmin__svc_stress') }}