{{ config(dataset=get_schema('hub'), materialized='view', tags=["hub", "chess"]) }}

-- Hub model for Chess.com games with structured objects
-- Uses STRUCT to preserve logical groupings for game information

SELECT
    -- Core identifiers (kept flat for easy filtering/joining)
    game_id,
    username,
    JSON_VALUE(raw_data, '$.url') as game_url,
    JSON_VALUE(raw_data, '$.time_class') as time_class,
    JSON_VALUE(raw_data, '$.rules') as rules,
    CASE 
        WHEN JSON_VALUE(raw_data, '$.end_time') IS NOT NULL 
        THEN TIMESTAMP_SECONDS(CAST(JSON_VALUE(raw_data, '$.end_time') AS INT64)) 
        ELSE NULL 
    END as end_time,
    
    -- Game metadata as STRUCT
    STRUCT(
        JSON_VALUE(raw_data, '$.uuid') as uuid,
        JSON_VALUE(raw_data, '$.initial_setup') as initial_setup,
        JSON_VALUE(raw_data, '$.fen') as final_position,
        JSON_VALUE(raw_data, '$.pgn') as pgn,
        JSON_VALUE(raw_data, '$.time_control') as time_control,
        CASE 
            WHEN JSON_VALUE(raw_data, '$.end_time') IS NOT NULL 
            THEN TIMESTAMP_SECONDS(CAST(JSON_VALUE(raw_data, '$.end_time') AS INT64)) 
            ELSE NULL 
        END as end_time
    ) as game_info,
    
    -- White player information as STRUCT
    STRUCT(
        JSON_VALUE(raw_data, '$.white.rating') as rating,
        JSON_VALUE(raw_data, '$.white.result') as result,
        JSON_VALUE(raw_data, '$.white.username') as username,
        JSON_VALUE(raw_data, '$.white.uuid') as uuid
    ) as white_player,
    
    -- Black player information as STRUCT
    STRUCT(
        JSON_VALUE(raw_data, '$.black.rating') as rating,
        JSON_VALUE(raw_data, '$.black.result') as result,
        JSON_VALUE(raw_data, '$.black.username') as username,
        JSON_VALUE(raw_data, '$.black.uuid') as uuid
    ) as black_player,
    
    -- Tournament information as STRUCT (fields don't exist in current data structure)
    STRUCT(
        CAST(NULL AS STRING) as tournament_url,
        CAST(NULL AS STRING) as match_url
    ) as tournament_info,
    
    -- Game result analysis as STRUCT
    STRUCT(
        -- Determine user's color and result
        CASE 
            WHEN JSON_VALUE(raw_data, '$.white.username') = username THEN 'white'
            WHEN JSON_VALUE(raw_data, '$.black.username') = username THEN 'black'
            ELSE NULL
        END as user_color,
        CASE 
            WHEN JSON_VALUE(raw_data, '$.white.username') = username THEN JSON_VALUE(raw_data, '$.white.result')
            WHEN JSON_VALUE(raw_data, '$.black.username') = username THEN JSON_VALUE(raw_data, '$.black.result')
            ELSE NULL
        END as user_result,
        CASE 
            WHEN JSON_VALUE(raw_data, '$.white.username') = username THEN CAST(JSON_VALUE(raw_data, '$.white.rating') AS INT64)
            WHEN JSON_VALUE(raw_data, '$.black.username') = username THEN CAST(JSON_VALUE(raw_data, '$.black.rating') AS INT64)
            ELSE NULL
        END as user_rating
    ) as user_game_info,
    
    -- Metadata
    dp_inserted_at,
    source_file

FROM {{ ref('lake_chess__svc_games') }}
WHERE game_id IS NOT NULL
  AND username IS NOT NULL