{{ config(dataset=get_schema('hub'), materialized='view', tags=["hub", "garmin"]) }}

-- Hub model for Garmin heart rate timeseries data
-- Expands heart rate timeseries into structured ARRAY format for detailed analysis

SELECT
    -- Primary identifiers
    heart_rate_date,
    
    -- Time window context
    TIMESTAMP(JSON_VALUE(raw_data, '$.startTimestampGMT')) as start_timestamp_gmt,
    TIMESTAMP(JSON_VALUE(raw_data, '$.endTimestampGMT')) as end_timestamp_gmt,
    
    -- Heart rate timeseries as ARRAY of STRUCT for easy querying
    ARRAY(
        SELECT AS STRUCT
            TIMESTAMP_MILLIS(CAST(JSON_VALUE(hr_values, '$[0]') AS INT64)) as timestamp_gmt,
            CAST(JSON_VALUE(hr_values, '$[1]') AS INT64) as heart_rate_bpm
        FROM UNNEST(JSON_QUERY_ARRAY(raw_data, '$.heartRateValues')) as hr_values
        WHERE JSON_VALUE(hr_values, '$[1]') IS NOT NULL  -- Filter out null heart rate values
        ORDER BY TIMESTAMP_MILLIS(CAST(JSON_VALUE(hr_values, '$[0]') AS INT64))
    ) as heart_rate_measurements,
    
    -- Summary statistics for quick access
    CAST(JSON_VALUE(raw_data, '$.restingHeartRate') AS INT64) as resting_heart_rate,
    CAST(JSON_VALUE(raw_data, '$.maxHeartRate') AS INT64) as max_heart_rate,
    CAST(JSON_VALUE(raw_data, '$.minHeartRate') AS INT64) as min_heart_rate,
    
    -- Metadata
    dp_inserted_at,
    source_file

FROM {{ ref('lake_garmin__svc_heart_rate') }}