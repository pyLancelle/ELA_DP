{{ config(dataset=get_schema('hub'), materialized='view', tags=["hub", "garmin"]) }}

-- Hub model for Garmin heart rate data with structured organization
-- Transforms raw heart rate JSON into structured format with logical groupings

SELECT
    -- Primary identifiers
    heart_rate_date,
    
    -- Time window information as STRUCT
    STRUCT(
        TIMESTAMP(JSON_VALUE(raw_data, '$.startTimestampGMT')) as start_gmt,
        TIMESTAMP(JSON_VALUE(raw_data, '$.endTimestampGMT')) as end_gmt,
        TIMESTAMP(JSON_VALUE(raw_data, '$.startTimestampLocal')) as start_local,
        TIMESTAMP(JSON_VALUE(raw_data, '$.endTimestampLocal')) as end_local
    ) as time_window,
    
    -- Summary heart rate metrics as STRUCT
    STRUCT(
        CAST(JSON_VALUE(raw_data, '$.restingHeartRate') AS INT64) as resting_heart_rate,
        CAST(JSON_VALUE(raw_data, '$.maxHeartRate') AS INT64) as max_heart_rate,
        CAST(JSON_VALUE(raw_data, '$.minHeartRate') AS INT64) as min_heart_rate,
        CAST(JSON_VALUE(raw_data, '$.lastSevenDaysAvgRestingHeartRate') AS INT64) as last_seven_days_avg_resting_heart_rate
    ) as heart_rate_metrics,
    
    -- Heart rate value descriptors (metadata for timeseries)
    JSON_QUERY(raw_data, '$.heartRateValueDescriptors') as heart_rate_descriptors,
    
    -- Heart rate timeseries as JSON array (for detailed analysis)
    JSON_QUERY(raw_data, '$.heartRateValues') as heart_rate_timeseries,
    
    -- Metadata
    dp_inserted_at,
    source_file

FROM {{ ref('lake_garmin__svc_heart_rate') }}