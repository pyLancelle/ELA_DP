{{ config(
    materialized='incremental',
    unique_key='floors_date',
    partition_by={'field': 'floors_date', 'data_type': 'date'},
    cluster_by=['floors_date'],
    dataset=get_schema('hub'),
    tags=["hub", "garmin"]
) }}

-- Hub model for Garmin floors time series data
-- Contains detailed 15-minute interval data for floors climbed analysis

SELECT
    -- Core identifiers
    DATE(JSON_VALUE(raw_data, '$.date')) as floors_date,
    CAST(JSON_VALUE(raw_data, '$.userProfilePK') AS INT64) as user_profile_pk,

    -- Time window context
    FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', TIMESTAMP(JSON_VALUE(raw_data, '$.startTimestampGMT'))) as start_timestamp_gmt,
    FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', TIMESTAMP(JSON_VALUE(raw_data, '$.endTimestampGMT'))) as end_timestamp_gmt,
    FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', TIMESTAMP(JSON_VALUE(raw_data, '$.startTimestampLocal'))) as start_timestamp_local,
    FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', TIMESTAMP(JSON_VALUE(raw_data, '$.endTimestampLocal'))) as end_timestamp_local,

    -- Floors timeseries parsed (15-minute intervals)
    ARRAY(
        SELECT AS STRUCT
            FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', TIMESTAMP(JSON_VALUE(floor_interval, '$[0]'))) as start_time,
            FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', TIMESTAMP(JSON_VALUE(floor_interval, '$[1]'))) as end_time,
            CAST(JSON_VALUE(floor_interval, '$[2]') AS INT64) as floors_ascended,
            CAST(JSON_VALUE(floor_interval, '$[3]') AS INT64) as floors_descended
        FROM UNNEST(JSON_QUERY_ARRAY(JSON_QUERY(raw_data, '$.floorValuesArray'))) AS floor_interval
        WHERE JSON_VALUE(floor_interval, '$[2]') IS NOT NULL OR JSON_VALUE(floor_interval, '$[3]') IS NOT NULL
        ORDER BY JSON_VALUE(floor_interval, '$[0]')
    ) as floors_timeseries,

    -- Value descriptors parsed
    ARRAY(
        SELECT AS STRUCT
            CAST(JSON_VALUE(TO_JSON_STRING(value), '$.floorsValueDescriptorIndex') AS INT64) as descriptor_index,
            JSON_VALUE(TO_JSON_STRING(value), '$.floorsValueDescriptorKey') as descriptor_key
        FROM UNNEST(JSON_QUERY_ARRAY(JSON_QUERY(raw_data, '$.floorsValueDescriptorDTOList'))) AS value
        ORDER BY CAST(JSON_VALUE(TO_JSON_STRING(value), '$.floorsValueDescriptorIndex') AS INT64)
    ) as value_descriptors,

    -- Metadata
    dp_inserted_at,
    source_file

FROM {{ ref('lake_garmin__svc_floors') }}

QUALIFY ROW_NUMBER() OVER (PARTITION BY DATE(JSON_VALUE(raw_data, '$.date')) ORDER BY dp_inserted_at DESC) = 1

{% if is_incremental() %}
  WHERE dp_inserted_at > (SELECT MAX(dp_inserted_at) FROM {{ this }})
{% endif %}