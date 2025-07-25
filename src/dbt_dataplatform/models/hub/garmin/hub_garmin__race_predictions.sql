{{ config(dataset=get_schema('hub'), materialized='view', tags=["hub", "garmin"]) }}

-- Hub model for Garmin race predictions data - simple field mapping
-- Direct extraction of JSON fields without complex transformations

SELECT
    -- Direct field mapping from JSON
    DATE(JSON_VALUE(raw_data, '$.fromCalendarDate')) as from_calendar_date,
    DATE(JSON_VALUE(raw_data, '$.toCalendarDate')) as to_calendar_date,
    DATE(JSON_VALUE(raw_data, '$.calendarDate')) as calendar_date,
    CAST(JSON_VALUE(raw_data, '$.time5K') AS INT64) as time_5k,
    CAST(JSON_VALUE(raw_data, '$.time10K') AS INT64) as time_10k,
    CAST(JSON_VALUE(raw_data, '$.timeHalfMarathon') AS INT64) as time_half_marathon,
    CAST(JSON_VALUE(raw_data, '$.timeMarathon') AS INT64) as time_marathon,
    
    -- Metadata
    dp_inserted_at,
    source_file
    
FROM {{ ref('lake_garmin__svc_race_predictions') }}