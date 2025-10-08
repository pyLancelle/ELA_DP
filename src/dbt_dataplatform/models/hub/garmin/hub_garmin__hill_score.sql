{{ config(
    materialized='incremental',
    unique_key='score_date',
    partition_by={'field': 'score_date', 'data_type': 'date'},
    cluster_by=['score_date'],
    dataset=get_schema('hub'),
    tags=["hub", "garmin"]
) }}

-- Hub model for Garmin hill score data
-- Unnests hillScoreDTOList to create one row per daily score
-- Clean structure with STRUCT grouping for logical separation

SELECT
    -- Primary date identifier
    DATE(JSON_EXTRACT_SCALAR(daily_record, '$.calendarDate')) as score_date,

    -- Hill score metrics
    STRUCT(
        CAST(JSON_EXTRACT_SCALAR(daily_record, '$.strengthScore') AS INT64) as strength_score,
        CAST(JSON_EXTRACT_SCALAR(daily_record, '$.enduranceScore') AS INT64) as endurance_score,
        CAST(JSON_EXTRACT_SCALAR(daily_record, '$.overallScore') AS INT64) as overall_score,
        CAST(JSON_EXTRACT_SCALAR(daily_record, '$.hillScoreClassificationId') AS INT64) as classification_id,
        CAST(JSON_EXTRACT_SCALAR(daily_record, '$.hillScoreFeedbackPhraseId') AS INT64) as feedback_phrase_id
    ) as hill_score,

    -- VO2 Max metrics (often null)
    STRUCT(
        CAST(JSON_EXTRACT_SCALAR(daily_record, '$.vo2Max') AS FLOAT64) as vo2_max,
        CAST(JSON_EXTRACT_SCALAR(daily_record, '$.vo2MaxPreciseValue') AS FLOAT64) as vo2_max_precise_value
    ) as vo2_max_data,

    -- Period context
    STRUCT(
        DATE(JSON_EXTRACT_SCALAR(raw_data, '$.startDate')) as start_date,
        DATE(JSON_EXTRACT_SCALAR(raw_data, '$.endDate')) as end_date,
        CAST(JSON_EXTRACT_SCALAR(raw_data, '$.maxScore') AS INT64) as max_score
    ) as period,

    -- Device information
    STRUCT(
        CAST(JSON_EXTRACT_SCALAR(daily_record, '$.deviceId') AS INT64) as device_id,
        CAST(JSON_EXTRACT_SCALAR(daily_record, '$.primaryTrainingDevice') AS BOOL) as primary_training_device
    ) as device,

    -- Metadata
    dp_inserted_at,
    source_file

FROM {{ ref('lake_garmin__svc_hill_score') }},
    UNNEST(JSON_EXTRACT_ARRAY(raw_data, '$.hillScoreDTOList')) as daily_record

WHERE JSON_EXTRACT_SCALAR(daily_record, '$.calendarDate') IS NOT NULL

{% if is_incremental() %}
  AND dp_inserted_at > (SELECT MAX(dp_inserted_at) FROM {{ this }})
{% endif %}
