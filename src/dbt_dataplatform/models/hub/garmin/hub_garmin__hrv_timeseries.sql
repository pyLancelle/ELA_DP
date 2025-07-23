{{ config(dataset=get_schema('hub'), materialized='view', tags=["hub", "garmin"]) }}

-- Hub model for Garmin HRV time series data
-- Contains detailed 5-minute interval HRV readings during sleep for advanced analysis

SELECT
    -- Core identifiers for joining with main HRV model
    DATE(JSON_VALUE(raw_data, '$.date')) as hrv_date,
    TIMESTAMP(JSON_VALUE(raw_data, '$.sleepStartTimestampGMT')) as sleep_start_timestamp_gmt,
    TIMESTAMP(JSON_VALUE(raw_data, '$.sleepEndTimestampGMT')) as sleep_end_timestamp_gmt,
    
    -- HRV readings array with 5-minute intervals during sleep
    -- Each reading contains: hrvValue, readingTimeGMT, readingTimeLocal
    JSON_QUERY(raw_data, '$.hrvReadings') as hrv_readings_array,
    
    -- Metadata
    dp_inserted_at,
    source_file
    
FROM {{ ref('lake_garmin__hrv') }}