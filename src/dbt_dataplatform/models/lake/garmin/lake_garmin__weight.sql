{{ config(dataset=get_schema('lake')) }}

-- Pure weight/body composition data extraction from staging_garmin_raw
-- Source: weight data type from Garmin Connect API

SELECT
    -- Date and timestamp
    DATE(SAFE.PARSE_TIMESTAMP('%Y-%m-%d', JSON_EXTRACT_SCALAR(raw_data, '$.date'))) AS weight_date,
    SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S', JSON_EXTRACT_SCALAR(raw_data, '$.timestampGMT')) AS timestamp_gmt,
    SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S', JSON_EXTRACT_SCALAR(raw_data, '$.timestampLocal')) AS timestamp_local,
    
    -- Weight and body composition metrics
    SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.weight') AS FLOAT64) AS weight_kg,
    SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.bodyFat') AS FLOAT64) AS body_fat_percentage,
    SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.bodyWater') AS FLOAT64) AS body_water_percentage,
    SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.muscleMass') AS FLOAT64) AS muscle_mass_kg,
    SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.boneMass') AS FLOAT64) AS bone_mass_kg,
    SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.bmi') AS FLOAT64) AS bmi,
    SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.bodyAge') AS INT64) AS body_age,
    SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.visceralFatRating') AS FLOAT64) AS visceral_fat_rating,
    SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.metabolicAge') AS INT64) AS metabolic_age,
    
    -- Measurement source/device
    JSON_EXTRACT_SCALAR(raw_data, '$.sourceType') AS source_type,
    JSON_EXTRACT_SCALAR(raw_data, '$.deviceId') AS device_id,
    
    -- Source metadata
    dp_inserted_at,
    source_file

FROM {{ source('garmin', 'staging_garmin_raw') }}
WHERE data_type = 'weight'
  AND JSON_EXTRACT_SCALAR(raw_data, '$.weight') IS NOT NULL