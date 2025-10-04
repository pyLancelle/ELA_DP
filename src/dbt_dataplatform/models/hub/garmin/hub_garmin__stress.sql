{{ config(
    materialized='incremental',
    unique_key='stress_date',
    partition_by={'field': 'stress_date', 'data_type': 'date'},
    cluster_by=['stress_date', 'user_profile_pk'],
    dataset=get_schema('hub'),
    tags=["hub", "garmin"]
) }}

-- Hub model for Garmin stress data with STRUCT organization
-- Transforms raw stress JSON into structured format with logical groupings

SELECT
    -- Primary identifiers
    DATE(JSON_VALUE(raw_data, '$.date')) as stress_date,
    CAST(JSON_VALUE(raw_data, '$.userId') AS INT64) as user_profile_pk,

    -- Time window information
    STRUCT(
        TIMESTAMP(JSON_VALUE(raw_data, '$.startTimestampGMT')) as start_gmt,
        TIMESTAMP(JSON_VALUE(raw_data, '$.endTimestampGMT')) as end_gmt,
        TIMESTAMP(JSON_VALUE(raw_data, '$.startTimestampLocal')) as start_local,
        TIMESTAMP(JSON_VALUE(raw_data, '$.endTimestampLocal')) as end_local
    ) as time_window,

    -- Summary metrics
    CAST(JSON_VALUE(raw_data, '$.avgStressLevel') AS INT64) as avg_stress_level,
    CAST(JSON_VALUE(raw_data, '$.maxStressLevel') AS INT64) as max_stress_level,
    CAST(JSON_VALUE(raw_data, '$.stressChartValueOffset') AS INT64) as stress_chart_value_offset,
    CAST(JSON_VALUE(raw_data, '$.stressChartYAxisOrigin') AS INT64) as stress_chart_y_axis_origin,

    -- Metadata
    dp_inserted_at,
    source_file

FROM {{ ref('lake_garmin__svc_stress') }}

{% if is_incremental() %}
WHERE dp_inserted_at > (SELECT MAX(dp_inserted_at) FROM {{ this }})
{% endif %}