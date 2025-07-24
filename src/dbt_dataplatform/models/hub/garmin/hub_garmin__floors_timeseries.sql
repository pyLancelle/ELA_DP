{{ config(dataset=get_schema('hub'), materialized='view', tags=["hub", "garmin"]) }}

-- Hub model for Garmin floors time series data
-- Contains detailed 15-minute interval data for floors climbed analysis

SELECT
    -- Core identifiers for joining with main floors model
    DATE(JSON_VALUE(raw_data, '$.date')) as floors_date,
    TIMESTAMP(JSON_VALUE(raw_data, '$.startTimestampGMT')) as start_timestamp_gmt,
    TIMESTAMP(JSON_VALUE(raw_data, '$.endTimestampGMT')) as end_timestamp_gmt,
    
    -- Time series values in 15-minute intervals
    -- Array structure: [startTimeGMT, endTimeGMT, floorsAscended, floorsDescended]
    JSON_QUERY(raw_data, '$.floorValuesArray') as floor_values_array,
    
    -- Column descriptors for the values array
    JSON_QUERY(raw_data, '$.floorsValueDescriptorDTOList') as value_descriptors,
    
    -- Metadata
    dp_inserted_at,
    source_file
    
FROM {{ ref('lake_garmin__svc_floors') }}