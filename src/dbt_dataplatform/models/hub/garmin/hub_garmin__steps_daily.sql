{{ config(dataset=get_schema('hub'), materialized='view', tags=["hub", "garmin"]) }}

-- Hub model for Garmin steps daily aggregation
-- One line per day with 15-minute intervals as array

SELECT
    -- Primary date identifier
    DATE(JSON_VALUE(raw_data, '$.date')) as step_date,
    
    -- Daily aggregations
    SUM(CAST(JSON_VALUE(raw_data, '$.steps') AS INT64)) as total_steps,
    SUM(CAST(JSON_VALUE(raw_data, '$.pushes') AS INT64)) as total_pushes,
    
    -- Array of 15-minute intervals for the day
    ARRAY_AGG(
        STRUCT(
            TIMESTAMP(JSON_VALUE(raw_data, '$.startGMT')) as start_gmt,
            TIMESTAMP(JSON_VALUE(raw_data, '$.endGMT')) as end_gmt,
            CAST(JSON_VALUE(raw_data, '$.steps') AS INT64) as steps,
            CAST(JSON_VALUE(raw_data, '$.pushes') AS INT64) as pushes,
            JSON_VALUE(raw_data, '$.primaryActivityLevel') as primary_activity_level,
            CAST(JSON_VALUE(raw_data, '$.activityLevelConstant') AS BOOLEAN) as activity_level_constant
        ) 
        ORDER BY TIMESTAMP(JSON_VALUE(raw_data, '$.startGMT'))
    ) as intervals_array,
    
    -- Metadata
    MIN(dp_inserted_at) as dp_inserted_at,
    STRING_AGG(DISTINCT source_file) as source_files
    
FROM {{ ref('lake_garmin__svc_steps') }}
WHERE DATE(JSON_VALUE(raw_data, '$.date')) >= '2025-03-01'
GROUP BY DATE(JSON_VALUE(raw_data, '$.date'))