{{ config(dataset=get_schema('hub'), materialized='view', tags=["hub", "garmin"]) }}

-- Hub model for Garmin HRV data with structured objects
-- Uses STRUCT to preserve logical groupings for heart rate variability analysis

SELECT
    -- Core identifiers and basic info (kept flat for easy filtering/joining)
    DATE(JSON_VALUE(raw_data, '$.date')) as hrv_date,
    
    -- HRV Summary with nested baseline information
    STRUCT(
        DATE(JSON_VALUE(raw_data, '$.hrvSummary.calendarDate')) as calendar_date,
        CAST(JSON_VALUE(raw_data, '$.hrvSummary.weeklyAvg') AS INT64) as weekly_avg,
        CAST(JSON_VALUE(raw_data, '$.hrvSummary.lastNightAvg') AS INT64) as last_night_avg,
        CAST(JSON_VALUE(raw_data, '$.hrvSummary.lastNight5MinHigh') AS INT64) as last_night_5min_high,
        
        -- Baseline thresholds as nested STRUCT
        STRUCT(
            CAST(JSON_VALUE(raw_data, '$.hrvSummary.baseline.lowUpper') AS INT64) as low_upper,
            CAST(JSON_VALUE(raw_data, '$.hrvSummary.baseline.balancedLow') AS INT64) as balanced_low,
            CAST(JSON_VALUE(raw_data, '$.hrvSummary.baseline.balancedUpper') AS INT64) as balanced_upper,
            CAST(JSON_VALUE(raw_data, '$.hrvSummary.baseline.markerValue') AS FLOAT64) as marker_value
        ) as baseline,
        
        JSON_VALUE(raw_data, '$.hrvSummary.status') as status,
        JSON_VALUE(raw_data, '$.hrvSummary.feedbackPhrase') as feedback_phrase,
        TIMESTAMP(JSON_VALUE(raw_data, '$.hrvSummary.createTimeStamp')) as create_timestamp
    ) as hrv_summary,
    
    -- Session timing information
    STRUCT(
        TIMESTAMP(JSON_VALUE(raw_data, '$.startTimestampGMT')) as start_gmt,
        TIMESTAMP(JSON_VALUE(raw_data, '$.endTimestampGMT')) as end_gmt,
        TIMESTAMP(JSON_VALUE(raw_data, '$.startTimestampLocal')) as start_local,
        TIMESTAMP(JSON_VALUE(raw_data, '$.endTimestampLocal')) as end_local
    ) as session_time_window,
    
    -- Sleep timing information (HRV is primarily collected during sleep)
    STRUCT(
        TIMESTAMP(JSON_VALUE(raw_data, '$.sleepStartTimestampGMT')) as sleep_start_gmt,
        TIMESTAMP(JSON_VALUE(raw_data, '$.sleepEndTimestampGMT')) as sleep_end_gmt,
        TIMESTAMP(JSON_VALUE(raw_data, '$.sleepStartTimestampLocal')) as sleep_start_local,
        TIMESTAMP(JSON_VALUE(raw_data, '$.sleepEndTimestampLocal')) as sleep_end_local
    ) as sleep_time_window,
    
    -- HRV readings time series (for detailed analysis)
    JSON_QUERY(raw_data, '$.hrvReadings') as hrv_readings_timeseries,
    
    -- Metadata
    dp_inserted_at,
    source_file
    
FROM {{ ref('lake_garmin__hrv') }}