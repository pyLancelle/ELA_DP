{{ config(
    materialized='incremental',
    unique_key='hrv_date',
    partition_by={'field': 'hrv_date', 'data_type': 'date'},
    cluster_by=['hrv_date'],
    dataset=get_schema('hub'),
    tags=["hub", "garmin"]
) }}

-- Hub model for Garmin HRV time series data
-- Contains parsed 5-minute interval HRV readings during sleep for advanced analysis

SELECT
    -- Core identifiers
    DATE(JSON_VALUE(raw_data, '$.date')) as hrv_date,
    CAST(JSON_VALUE(raw_data, '$.userProfilePk') AS INT64) as user_profile_pk,

    -- Time window context
    FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', TIMESTAMP(JSON_VALUE(raw_data, '$.startTimestampGMT'))) as start_timestamp_gmt,
    FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', TIMESTAMP(JSON_VALUE(raw_data, '$.endTimestampGMT'))) as end_timestamp_gmt,
    FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', TIMESTAMP(JSON_VALUE(raw_data, '$.startTimestampLocal'))) as start_timestamp_local,
    FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', TIMESTAMP(JSON_VALUE(raw_data, '$.endTimestampLocal'))) as end_timestamp_local,

    -- Sleep window context
    FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', TIMESTAMP(JSON_VALUE(raw_data, '$.sleepStartTimestampGMT'))) as sleep_start_timestamp_gmt,
    FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', TIMESTAMP(JSON_VALUE(raw_data, '$.sleepEndTimestampGMT'))) as sleep_end_timestamp_gmt,
    FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', TIMESTAMP(JSON_VALUE(raw_data, '$.sleepStartTimestampLocal'))) as sleep_start_timestamp_local,
    FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', TIMESTAMP(JSON_VALUE(raw_data, '$.sleepEndTimestampLocal'))) as sleep_end_timestamp_local,

    -- HRV readings timeseries parsed (5-minute intervals)
    ARRAY(
        SELECT AS STRUCT
            CAST(JSON_VALUE(TO_JSON_STRING(value), '$.hrvValue') AS INT64) as hrv_value,
            FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', TIMESTAMP(JSON_VALUE(TO_JSON_STRING(value), '$.readingTimeGMT'))) as reading_time_gmt,
            FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', TIMESTAMP(JSON_VALUE(TO_JSON_STRING(value), '$.readingTimeLocal'))) as reading_time_local
        FROM UNNEST(JSON_QUERY_ARRAY(JSON_QUERY(raw_data, '$.hrvReadings'))) AS value
        WHERE JSON_VALUE(TO_JSON_STRING(value), '$.hrvValue') IS NOT NULL
        ORDER BY JSON_VALUE(TO_JSON_STRING(value), '$.readingTimeGMT')
    ) as hrv_timeseries,

    -- HRV summary parsed
    STRUCT(
        STRUCT(
            CAST(JSON_VALUE(raw_data, '$.hrvSummary.baseline.balancedLow') AS INT64) as balanced_low,
            CAST(JSON_VALUE(raw_data, '$.hrvSummary.baseline.balancedUpper') AS INT64) as balanced_upper,
            CAST(JSON_VALUE(raw_data, '$.hrvSummary.baseline.lowUpper') AS INT64) as low_upper,
            CAST(JSON_VALUE(raw_data, '$.hrvSummary.baseline.markerValue') AS FLOAT64) as marker_value
        ) as baseline,
        DATE(JSON_VALUE(raw_data, '$.hrvSummary.calendarDate')) as calendar_date,
        TIMESTAMP(JSON_VALUE(raw_data, '$.hrvSummary.createTimeStamp')) as create_timestamp,
        JSON_VALUE(raw_data, '$.hrvSummary.feedbackPhrase') as feedback_phrase,
        CAST(JSON_VALUE(raw_data, '$.hrvSummary.lastNight5MinHigh') AS INT64) as last_night_5min_high,
        CAST(JSON_VALUE(raw_data, '$.hrvSummary.lastNightAvg') AS INT64) as last_night_avg,
        JSON_VALUE(raw_data, '$.hrvSummary.status') as status,
        CAST(JSON_VALUE(raw_data, '$.hrvSummary.weeklyAvg') AS INT64) as weekly_avg
    ) as hrv_summary,

    -- Metadata
    dp_inserted_at,
    source_file

FROM {{ ref('lake_garmin__svc_hrv') }}

{% if is_incremental() %}
WHERE dp_inserted_at > (SELECT MAX(dp_inserted_at) FROM {{ this }})
{% endif %}