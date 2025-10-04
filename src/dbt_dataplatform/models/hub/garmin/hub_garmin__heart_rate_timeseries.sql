{{ config(
    materialized='incremental',
    unique_key='heart_rate_date',
    partition_by={'field': 'heart_rate_date', 'data_type': 'date'},
    cluster_by=['heart_rate_date'],
    dataset=get_schema('hub'),
    tags=["hub", "garmin"]
) }}

-- Hub model for Garmin heart rate timeseries data
-- Contains parsed and structured heart rate measurements for detailed analysis

SELECT
    -- Primary identifiers
    heart_rate_date,
    CAST(JSON_VALUE(raw_data, '$.userProfilePK') AS INT64) as user_profile_pk,
    DATE(JSON_VALUE(raw_data, '$.calendarDate')) as calendar_date,

    -- Time window context
    FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', TIMESTAMP(JSON_VALUE(raw_data, '$.startTimestampGMT'))) as start_timestamp_gmt,
    FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', TIMESTAMP(JSON_VALUE(raw_data, '$.endTimestampGMT'))) as end_timestamp_gmt,
    FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', TIMESTAMP(JSON_VALUE(raw_data, '$.startTimestampLocal'))) as start_timestamp_local,
    FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', TIMESTAMP(JSON_VALUE(raw_data, '$.endTimestampLocal'))) as end_timestamp_local,

    -- Heart rate timeseries parsed
    ARRAY(
        SELECT AS STRUCT
            FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', TIMESTAMP_MILLIS(CAST(JSON_VALUE(hr_values, '$[0]') AS INT64))) as time,
            CAST(JSON_VALUE(hr_values, '$[1]') AS INT64) as heart_rate_bpm
        FROM UNNEST(JSON_QUERY_ARRAY(JSON_QUERY(raw_data, '$.heartRateValues'))) as hr_values
        WHERE JSON_VALUE(hr_values, '$[1]') IS NOT NULL
        ORDER BY CAST(JSON_VALUE(hr_values, '$[0]') AS INT64)
    ) as heart_rate_timeseries,

    -- Heart rate value descriptors (similar to body battery)
    ARRAY(
        SELECT AS STRUCT
            CAST(JSON_VALUE(TO_JSON_STRING(value), '$.heartRateValueDescriptorIndex') AS INT64) as descriptor_index,
            JSON_VALUE(TO_JSON_STRING(value), '$.heartRateValueDescriptorKey') as descriptor_key
        FROM UNNEST(JSON_QUERY_ARRAY(JSON_QUERY(raw_data, '$.heartRateValueDescriptors'))) AS value
        ORDER BY CAST(JSON_VALUE(TO_JSON_STRING(value), '$.heartRateValueDescriptorIndex') AS INT64)
    ) as value_descriptors,

    -- Summary statistics
    CAST(JSON_VALUE(raw_data, '$.restingHeartRate') AS INT64) as resting_heart_rate,
    CAST(JSON_VALUE(raw_data, '$.maxHeartRate') AS INT64) as max_heart_rate,
    CAST(JSON_VALUE(raw_data, '$.minHeartRate') AS INT64) as min_heart_rate,
    CAST(JSON_VALUE(raw_data, '$.lastSevenDaysAvgRestingHeartRate') AS INT64) as avg_7d_resting_heart_rate,

    -- Metadata
    dp_inserted_at,
    source_file

FROM {{ ref('lake_garmin__svc_heart_rate') }}

{% if is_incremental() %}
WHERE dp_inserted_at > (SELECT MAX(dp_inserted_at) FROM {{ this }})
{% endif %}