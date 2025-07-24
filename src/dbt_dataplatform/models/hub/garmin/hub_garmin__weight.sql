{{ config(dataset=get_schema('hub'), materialized='view', tags=["hub", "garmin"]) }}

-- Hub model for Garmin weight data with logical STRUCT groupings
-- Direct extraction of JSON fields organized by measurement type

SELECT
    -- Core identifiers
    DATE(JSON_VALUE(raw_data, '$.calendarDate')) as calendar_date,
    DATE(JSON_VALUE(raw_data, '$.summaryDate')) as summary_date,
    TIMESTAMP_MILLIS(CAST(JSON_VALUE(raw_data, '$.date') AS INT64)) as date_timestamp,
    TIMESTAMP_MILLIS(CAST(JSON_VALUE(raw_data, '$.timestampGMT') AS INT64)) as timestamp_gmt,
    JSON_VALUE(raw_data, '$.sourceType') as source_type,
    
    -- Weight measurements grouped in STRUCT
    STRUCT(
        CAST(JSON_VALUE(raw_data, '$.weight') AS FLOAT64) as weight,
        CAST(JSON_VALUE(raw_data, '$.weightDelta') AS FLOAT64) as weight_delta,
        CAST(JSON_VALUE(raw_data, '$.bmi') AS FLOAT64) as bmi
    ) as weight_metrics,
    
    -- Body composition measurements grouped in STRUCT
    STRUCT(
        CAST(JSON_VALUE(raw_data, '$.bodyFat') AS FLOAT64) as body_fat,
        CAST(JSON_VALUE(raw_data, '$.bodyWater') AS FLOAT64) as body_water,
        CAST(JSON_VALUE(raw_data, '$.boneMass') AS INT64) as bone_mass,
        CAST(JSON_VALUE(raw_data, '$.muscleMass') AS INT64) as muscle_mass,
        CAST(JSON_VALUE(raw_data, '$.physiqueRating') AS INT64) as physique_rating,
        CAST(JSON_VALUE(raw_data, '$.visceralFat') AS FLOAT64) as visceral_fat,
        CAST(JSON_VALUE(raw_data, '$.metabolicAge') AS INT64) as metabolic_age
    ) as body_composition,
    
    -- Metadata
    dp_inserted_at,
    source_file
    
FROM {{ ref('lake_garmin__svc_weight') }}