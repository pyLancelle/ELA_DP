{{ config(dataset=get_schema('hub'), materialized='view', tags=["hub", "garmin"]) }}

-- Hub model for Garmin body battery time series data
-- Contains detailed temporal data for advanced energy analysis and activity correlation

SELECT
    -- Core identifiers for joining with main body battery model
    DATE(JSON_VALUE(raw_data, '$.date')) as battery_date,
    TIMESTAMP(JSON_VALUE(raw_data, '$.startTimestampGMT')) as start_timestamp_gmt,
    TIMESTAMP(JSON_VALUE(raw_data, '$.endTimestampGMT')) as end_timestamp_gmt,
    
    -- Time series values (timestamps + battery levels)
    JSON_QUERY(raw_data, '$.bodyBatteryValuesArray') as battery_values_array,
    
    -- Column descriptors for the values array
    JSON_QUERY(raw_data, '$.bodyBatteryValueDescriptorDTOList') as value_descriptors,
    
    -- Activity events that impact body battery (sleep, stress, exercise, etc.)
    JSON_QUERY(raw_data, '$.bodyBatteryActivityEvent') as activity_events,
    
    -- Metadata
    dp_inserted_at,
    source_file
    
FROM {{ ref('lake_garmin__svc_body_battery') }}