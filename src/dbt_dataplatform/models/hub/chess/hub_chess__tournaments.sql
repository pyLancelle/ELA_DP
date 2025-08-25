{{ config(dataset=get_schema('hub'), materialized='view', tags=["hub", "chess"]) }}

-- Hub model for Chess.com tournaments data with structured objects

SELECT
    tournament_id,
    username,
    
    -- Basic tournament information
    JSON_VALUE(raw_data, '$.name') as tournament_name,
    JSON_VALUE(raw_data, '$.description') as description,
    JSON_VALUE(raw_data, '$.url') as tournament_url,
    JSON_VALUE(raw_data, '$.creator') as creator_url,
    JSON_VALUE(raw_data, '$.status') as status,
    
    -- Tournament settings
    STRUCT(
        CAST(JSON_VALUE(raw_data, '$.total_players') AS INT64) as total_players,
        JSON_VALUE(raw_data, '$.time_class') as time_class,
        JSON_VALUE(raw_data, '$.time_control') as time_control,
        CAST(JSON_VALUE(raw_data, '$.rated') AS BOOL) as is_rated,
        CAST(JSON_VALUE(raw_data, '$.max_players') AS INT64) as max_players
    ) as settings,
    
    -- Tournament timing
    STRUCT(
        TIMESTAMP_SECONDS(CAST(JSON_VALUE(raw_data, '$.start_time') AS INT64)) as start_time,
        TIMESTAMP_SECONDS(CAST(JSON_VALUE(raw_data, '$.finish_time') AS INT64)) as finish_time,
        TIMESTAMP_SECONDS(CAST(JSON_VALUE(raw_data, '$.registration_open') AS INT64)) as registration_open_time
    ) as timing,
    
    -- Tournament format
    STRUCT(
        JSON_VALUE(raw_data, '$.type') as tournament_type,
        CAST(JSON_VALUE(raw_data, '$.swiss_initial_setup') AS BOOL) as is_swiss_initial,
        CAST(JSON_VALUE(raw_data, '$.initial_group_size') AS INT64) as initial_group_size
    ) as format,
    
    -- Prize information
    STRUCT(
        JSON_VALUE(raw_data, '$.prize') as prize_description,
        CAST(JSON_VALUE(raw_data, '$.is_prize_tournament') AS BOOL) as has_prize
    ) as prize_info,
    
    -- Entry requirements
    STRUCT(
        CAST(JSON_VALUE(raw_data, '$.min_rating') AS INT64) as min_rating,
        CAST(JSON_VALUE(raw_data, '$.max_rating') AS INT64) as max_rating,
        CAST(JSON_VALUE(raw_data, '$.is_invite_only') AS BOOL) as is_invite_only
    ) as entry_requirements,
    
    -- Tournament URLs
    STRUCT(
        JSON_VALUE(raw_data, '$.players') as players_url,
        JSON_VALUE(raw_data, '$.rounds') as rounds_url
    ) as urls,
    
    -- Metadata
    dp_inserted_at,
    source_file
    
FROM {{ ref('lake_chess__svc_tournaments') }}