{{ config(
    materialized='incremental',
    unique_key='floors_date',
    partition_by={'field': 'floors_date', 'data_type': 'date'},
    cluster_by=['floors_date'],
    dataset=get_schema('hub'),
    tags=["hub", "garmin"]
) }}

-- Hub model for Garmin floors data with structured objects
-- Simple model as floors data is primarily time series based

SELECT
    -- Core identifiers and basic info (kept flat for easy filtering/joining)
    DATE(JSON_VALUE(raw_data, '$.date')) as floors_date,
    
    -- Time window information
    STRUCT(
        TIMESTAMP(JSON_VALUE(raw_data, '$.startTimestampGMT')) as start_gmt,
        TIMESTAMP(JSON_VALUE(raw_data, '$.endTimestampGMT')) as end_gmt,
        TIMESTAMP(JSON_VALUE(raw_data, '$.startTimestampLocal')) as start_local,
        TIMESTAMP(JSON_VALUE(raw_data, '$.endTimestampLocal')) as end_local
    ) as time_window,
    
    -- Time series data as JSON arrays (for detailed analysis)
    JSON_QUERY(raw_data, '$.floorValuesArray') as floor_values_timeseries,
    JSON_QUERY(raw_data, '$.floorsValueDescriptorDTOList') as value_descriptors,
    
    -- Metadata
    dp_inserted_at,
    source_file

FROM {{ ref('lake_garmin__svc_floors') }}

{% if is_incremental() %}
WHERE dp_inserted_at > (SELECT MAX(dp_inserted_at) FROM {{ this }})
{% endif %}