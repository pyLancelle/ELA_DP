{{ config(
    materialized='incremental',
    unique_key='stress_date',
    partition_by={'field': 'stress_date', 'data_type': 'date'},
    cluster_by=['stress_date'],
    dataset=get_schema('hub'),
    tags=["hub", "garmin"]
) }}

-- Hub model for Garmin stress timeseries data
-- Contains parsed stress and body battery time series for detailed daily analysis

SELECT
    -- Core identifiers
    DATE(JSON_VALUE(raw_data, '$.date')) as stress_date,
    CAST(JSON_VALUE(raw_data, '$.userId') AS INT64) as user_profile_pk,

    -- Time window context
    FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', TIMESTAMP(JSON_VALUE(raw_data, '$.startTimestampGMT'))) as start_timestamp_gmt,
    FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', TIMESTAMP(JSON_VALUE(raw_data, '$.endTimestampGMT'))) as end_timestamp_gmt,
    FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', TIMESTAMP(JSON_VALUE(raw_data, '$.startTimestampLocal'))) as start_timestamp_local,
    FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', TIMESTAMP(JSON_VALUE(raw_data, '$.endTimestampLocal'))) as end_timestamp_local,

    -- Stress timeseries parsed
    ARRAY(
        SELECT AS STRUCT
            FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', TIMESTAMP_MILLIS(SAFE_CAST(JSON_VALUE(stress_point, '$[0]') AS INT64))) as time,
            SAFE_CAST(JSON_VALUE(stress_point, '$[1]') AS INT64) as stress_level
        FROM UNNEST(JSON_QUERY_ARRAY(JSON_QUERY(raw_data, '$.stressValuesArray'))) AS stress_point
        WHERE JSON_VALUE(stress_point, '$[1]') IS NOT NULL
            AND SAFE_CAST(JSON_VALUE(stress_point, '$[0]') AS INT64) IS NOT NULL
            AND SAFE_CAST(JSON_VALUE(stress_point, '$[1]') AS INT64) IS NOT NULL
        ORDER BY SAFE_CAST(JSON_VALUE(stress_point, '$[0]') AS INT64)
    ) as stress_timeseries,

    -- Body Battery timeseries parsed (from stress endpoint)
    ARRAY(
        SELECT AS STRUCT
            FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', TIMESTAMP_MILLIS(SAFE_CAST(JSON_VALUE(bb_point, '$[0]') AS INT64))) as time,
            SAFE_CAST(JSON_VALUE(bb_point, '$[1]') AS INT64) as battery_level
        FROM UNNEST(JSON_QUERY_ARRAY(JSON_QUERY(raw_data, '$.bodyBatteryValuesArray'))) AS bb_point
        WHERE JSON_VALUE(bb_point, '$[1]') IS NOT NULL
            AND SAFE_CAST(JSON_VALUE(bb_point, '$[0]') AS INT64) IS NOT NULL
            AND SAFE_CAST(JSON_VALUE(bb_point, '$[1]') AS INT64) IS NOT NULL
        ORDER BY SAFE_CAST(JSON_VALUE(bb_point, '$[0]') AS INT64)
    ) as body_battery_timeseries,

    -- Body Battery value descriptors parsed
    ARRAY(
        SELECT AS STRUCT
            CAST(JSON_VALUE(TO_JSON_STRING(value), '$.bodyBatteryValueDescriptorIndex') AS INT64) as descriptor_index,
            JSON_VALUE(TO_JSON_STRING(value), '$.bodyBatteryValueDescriptorKey') as descriptor_key
        FROM UNNEST(JSON_QUERY_ARRAY(JSON_QUERY(raw_data, '$.bodyBatteryValueDescriptorsDTOList'))) AS value
        ORDER BY CAST(JSON_VALUE(TO_JSON_STRING(value), '$.bodyBatteryValueDescriptorIndex') AS INT64)
    ) as body_battery_descriptors,

    -- Metadata
    dp_inserted_at,
    source_file

FROM {{ ref('lake_garmin__svc_stress') }}

{% if is_incremental() %}
WHERE dp_inserted_at > (SELECT MAX(dp_inserted_at) FROM {{ this }})
{% endif %}
