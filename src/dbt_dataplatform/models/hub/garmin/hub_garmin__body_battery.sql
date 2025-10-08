{{ config(
    materialized='incremental',
    unique_key='battery_date',
    partition_by={'field': 'battery_date', 'data_type': 'date'},
    cluster_by=['battery_date'],
    dataset=get_schema('hub'),
    tags=["hub", "garmin"]
) }}

-- Hub model for Garmin body battery data with structured objects
-- Uses STRUCT to preserve logical groupings for daily energy metrics

SELECT
    -- Core identifiers and basic metrics (kept flat for easy filtering/joining)
    DATE(JSON_VALUE(raw_data, '$.date')) as battery_date,
    CAST(JSON_VALUE(raw_data, '$.charged') AS INT64) as energy_charged,
    CAST(JSON_VALUE(raw_data, '$.drained') AS INT64) as energy_drained,
    
    -- Time window information
    STRUCT(
        TIMESTAMP(JSON_VALUE(raw_data, '$.startTimestampGMT')) as start_gmt,
        TIMESTAMP(JSON_VALUE(raw_data, '$.endTimestampGMT')) as end_gmt,
        TIMESTAMP(JSON_VALUE(raw_data, '$.startTimestampLocal')) as start_local,
        TIMESTAMP(JSON_VALUE(raw_data, '$.endTimestampLocal')) as end_local
    ) as time_window,
    
    -- Dynamic feedback event (during the day)
    STRUCT(
        TIMESTAMP(JSON_VALUE(raw_data, '$.bodyBatteryDynamicFeedbackEvent.eventTimestampGmt')) as event_timestamp_gmt,
        JSON_VALUE(raw_data, '$.bodyBatteryDynamicFeedbackEvent.bodyBatteryLevel') as battery_level,
        JSON_VALUE(raw_data, '$.bodyBatteryDynamicFeedbackEvent.feedbackShortType') as feedback_short_type,
        JSON_VALUE(raw_data, '$.bodyBatteryDynamicFeedbackEvent.feedbackLongType') as feedback_long_type
    ) as dynamic_feedback_event,
    
    -- End of day feedback event
    STRUCT(
        TIMESTAMP(JSON_VALUE(raw_data, '$.endOfDayBodyBatteryDynamicFeedbackEvent.eventTimestampGmt')) as event_timestamp_gmt,
        JSON_VALUE(raw_data, '$.endOfDayBodyBatteryDynamicFeedbackEvent.bodyBatteryLevel') as battery_level,
        JSON_VALUE(raw_data, '$.endOfDayBodyBatteryDynamicFeedbackEvent.feedbackShortType') as feedback_short_type,
        JSON_VALUE(raw_data, '$.endOfDayBodyBatteryDynamicFeedbackEvent.feedbackLongType') as feedback_long_type
    ) as end_of_day_feedback_event,
    
    -- Time series data as JSON arrays (for detailed analysis)
    JSON_QUERY(raw_data, '$.bodyBatteryValuesArray') as battery_values_timeseries,
    JSON_QUERY(raw_data, '$.bodyBatteryValueDescriptorDTOList') as value_descriptors,
    JSON_QUERY(raw_data, '$.bodyBatteryActivityEvent') as activity_events,
    
    -- Metadata
    dp_inserted_at,
    source_file

FROM {{ ref('lake_garmin__svc_body_battery') }}

QUALIFY ROW_NUMBER() OVER (PARTITION BY DATE(JSON_VALUE(raw_data, '$.date')) ORDER BY dp_inserted_at DESC) = 1

{% if is_incremental() %}
  AND dp_inserted_at > (SELECT MAX(dp_inserted_at) FROM {{ this }})
{% endif %}