{{ config(
    materialized='incremental',
    unique_key='score_date',
    partition_by={'field': 'score_date', 'data_type': 'date'},
    cluster_by=['score_date'],
    dataset=get_schema('hub'),
    tags=["hub", "garmin"]
) }}

-- Hub model for Garmin endurance score data
-- Clean structure based on actual API response with STRUCT grouping

SELECT
    -- Primary date identifier
    DATE(JSON_EXTRACT_SCALAR(raw_data, '$.enduranceScoreDTO.calendarDate')) as score_date,

    -- Endurance score metrics
    STRUCT(
        CAST(JSON_EXTRACT_SCALAR(raw_data, '$.enduranceScoreDTO.overallScore') AS INT64) as overall_score,
        CAST(JSON_EXTRACT_SCALAR(raw_data, '$.enduranceScoreDTO.classification') AS INT64) as classification,
        CAST(JSON_EXTRACT_SCALAR(raw_data, '$.enduranceScoreDTO.feedbackPhrase') AS INT64) as feedback_phrase_id
    ) as endurance_score,

    -- Classification thresholds
    STRUCT(
        CAST(JSON_EXTRACT_SCALAR(raw_data, '$.enduranceScoreDTO.classificationLowerLimitElite') AS INT64) as elite,
        CAST(JSON_EXTRACT_SCALAR(raw_data, '$.enduranceScoreDTO.classificationLowerLimitExpert') AS INT64) as expert,
        CAST(JSON_EXTRACT_SCALAR(raw_data, '$.enduranceScoreDTO.classificationLowerLimitSuperior') AS INT64) as superior,
        CAST(JSON_EXTRACT_SCALAR(raw_data, '$.enduranceScoreDTO.classificationLowerLimitWellTrained') AS INT64) as well_trained,
        CAST(JSON_EXTRACT_SCALAR(raw_data, '$.enduranceScoreDTO.classificationLowerLimitTrained') AS INT64) as trained,
        CAST(JSON_EXTRACT_SCALAR(raw_data, '$.enduranceScoreDTO.classificationLowerLimitIntermediate') AS INT64) as intermediate
    ) as classification_thresholds,

    -- Gauge range
    STRUCT(
        CAST(JSON_EXTRACT_SCALAR(raw_data, '$.enduranceScoreDTO.gaugeLowerLimit') AS INT64) as lower_limit,
        CAST(JSON_EXTRACT_SCALAR(raw_data, '$.enduranceScoreDTO.gaugeUpperLimit') AS INT64) as upper_limit
    ) as gauge_range,

    -- Contributors array (activity type contributions)
    ARRAY(
        SELECT AS STRUCT
            CAST(JSON_EXTRACT_SCALAR(contributor, '$.activityTypeId') AS INT64) as activity_type_id,
            CAST(JSON_EXTRACT_SCALAR(contributor, '$.contribution') AS FLOAT64) as contribution_percent,
            CAST(JSON_EXTRACT_SCALAR(contributor, '$.group') AS INT64) as group_id
        FROM UNNEST(JSON_EXTRACT_ARRAY(raw_data, '$.enduranceScoreDTO.contributors')) as contributor
    ) as contributors,

    -- Period stats
    STRUCT(
        DATE(JSON_EXTRACT_SCALAR(raw_data, '$.startDate')) as start_date,
        DATE(JSON_EXTRACT_SCALAR(raw_data, '$.endDate')) as end_date,
        CAST(JSON_EXTRACT_SCALAR(raw_data, '$.avg') AS INT64) as avg_score,
        CAST(JSON_EXTRACT_SCALAR(raw_data, '$.max') AS INT64) as max_score
    ) as period,

    -- Device information
    STRUCT(
        CAST(JSON_EXTRACT_SCALAR(raw_data, '$.enduranceScoreDTO.deviceId') AS INT64) as device_id,
        CAST(JSON_EXTRACT_SCALAR(raw_data, '$.enduranceScoreDTO.primaryTrainingDevice') AS BOOL) as primary_training_device
    ) as device,

    -- Metadata
    dp_inserted_at,
    source_file

FROM {{ ref('lake_garmin__svc_endurance_score') }}

{% if is_incremental() %}
WHERE dp_inserted_at > (SELECT MAX(dp_inserted_at) FROM {{ this }})
{% endif %}
