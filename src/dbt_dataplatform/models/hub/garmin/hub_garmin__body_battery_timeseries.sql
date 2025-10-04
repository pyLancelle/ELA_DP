{{ config(
    materialized='incremental',
    unique_key='battery_date',
    partition_by={'field': 'battery_date', 'data_type': 'date'},
    cluster_by=['battery_date'],
    dataset=get_schema('hub'),
    tags=["hub", "garmin"]
) }}

-- Hub model for Garmin body battery time series data
-- Contains parsed and structured temporal data for advanced energy analysis and activity correlation

SELECT
    -- Core identifiers for joining with main body battery model
    DATE(JSON_VALUE(raw_data, '$.date')) as battery_date,
    TIMESTAMP(JSON_VALUE(raw_data, '$.startTimestampGMT')) as start_timestamp_gmt,
    TIMESTAMP(JSON_VALUE(raw_data, '$.endTimestampGMT')) as end_timestamp_gmt,

    -- Time series values parsed (timestamps + battery levels)
    ARRAY(
        SELECT AS STRUCT
            FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', TIMESTAMP_MILLIS(CAST(JSON_VALUE(arr_item, '$[0]') AS INT64))) as time,
            CAST(JSON_VALUE(arr_item, '$[1]') AS INT64) as battery_level
        FROM UNNEST(JSON_QUERY_ARRAY(JSON_QUERY(raw_data, '$.bodyBatteryValuesArray'))) AS arr_item
        WHERE JSON_VALUE(arr_item, '$[1]') IS NOT NULL
        ORDER BY CAST(JSON_VALUE(arr_item, '$[0]') AS INT64)
    ) as battery_timeseries,

    -- Column descriptors parsed
    ARRAY(
        SELECT AS STRUCT
            CAST(JSON_VALUE(TO_JSON_STRING(value), '$.bodyBatteryValueDescriptorIndex') AS INT64) as descriptor_index,
            JSON_VALUE(TO_JSON_STRING(value), '$.bodyBatteryValueDescriptorKey') as descriptor_key
        FROM UNNEST(JSON_QUERY_ARRAY(JSON_QUERY(raw_data, '$.bodyBatteryValueDescriptorDTOList'))) AS value
        ORDER BY CAST(JSON_VALUE(TO_JSON_STRING(value), '$.bodyBatteryValueDescriptorIndex') AS INT64)
    ) as value_descriptors,

    -- Activity events that impact body battery parsed (sleep, stress, exercise, etc.)
    ARRAY(
        SELECT AS STRUCT
            CAST(JSON_VALUE(TO_JSON_STRING(value), '$.bodyBatteryImpact') AS INT64) as battery_impact,
            CAST(JSON_VALUE(TO_JSON_STRING(value), '$.durationInMilliseconds') AS INT64) as duration_ms,
            FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', TIMESTAMP(JSON_VALUE(TO_JSON_STRING(value), '$.eventStartTimeGmt'))) as event_start_time,
            JSON_VALUE(TO_JSON_STRING(value), '$.eventType') as event_type,
            JSON_VALUE(TO_JSON_STRING(value), '$.feedbackType') as feedback_type,
            JSON_VALUE(TO_JSON_STRING(value), '$.shortFeedback') as short_feedback,
            CAST(JSON_VALUE(TO_JSON_STRING(value), '$.timezoneOffset') AS INT64) as timezone_offset_ms
        FROM UNNEST(JSON_QUERY_ARRAY(JSON_QUERY(raw_data, '$.bodyBatteryActivityEvent'))) AS value
        ORDER BY JSON_VALUE(TO_JSON_STRING(value), '$.eventStartTimeGmt')
    ) as activity_events,

    -- Metadata
    dp_inserted_at,
    source_file

FROM {{ ref('lake_garmin__svc_body_battery') }}

{% if is_incremental() %}
WHERE dp_inserted_at > (SELECT MAX(dp_inserted_at) FROM {{ this }})
{% endif %}