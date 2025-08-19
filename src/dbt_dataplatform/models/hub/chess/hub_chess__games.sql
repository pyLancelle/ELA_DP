{{ config(dataset=get_schema('hub'), materialized='view', tags=["hub", "chess"]) }}

-- Hub model for Chess.com games data with structured objects
-- Organizes game data for efficient analysis

SELECT
    game_id,
    username,
    
    -- Game basic information
    JSON_VALUE(raw_data, '$.url') as game_url,
    JSON_VALUE(raw_data, '$.pgn') as pgn,
    JSON_VALUE(raw_data, '$.event') as event_type,
    JSON_VALUE(raw_data, '$.time_class') as time_class,
    JSON_VALUE(raw_data, '$.rules') as rules,
    CAST(JSON_VALUE(raw_data, '$.rated') AS BOOL) as is_rated,
    
    -- Time control information
    STRUCT(
        JSON_VALUE(raw_data, '$.time_control') as time_control,
        TIMESTAMP_SECONDS(CAST(JSON_VALUE(raw_data, '$.start_time') AS INT64)) as start_time,
        TIMESTAMP_SECONDS(CAST(JSON_VALUE(raw_data, '$.end_time') AS INT64)) as end_time
    ) as timing,
    
    -- White player information
    STRUCT(
        JSON_VALUE(raw_data, '$.white.username') as username,
        CAST(JSON_VALUE(raw_data, '$.white.rating') AS INT64) as rating,
        JSON_VALUE(raw_data, '$.white.result') as result,
        JSON_VALUE(raw_data, '$.white.@id') as api_url,
        JSON_VALUE(raw_data, '$.white.uuid') as uuid
    ) as white_player,
    
    -- Black player information
    STRUCT(
        JSON_VALUE(raw_data, '$.black.username') as username,
        CAST(JSON_VALUE(raw_data, '$.black.rating') AS INT64) as rating,
        JSON_VALUE(raw_data, '$.black.result') as result,
        JSON_VALUE(raw_data, '$.black.@id') as api_url,
        JSON_VALUE(raw_data, '$.black.uuid') as uuid
    ) as black_player,
    
    -- Opening information
    STRUCT(
        JSON_VALUE(raw_data, '$.eco') as eco_code,
        JSON_VALUE(raw_data, '$.opening') as opening_name
    ) as opening,
    
    -- Board positions
    STRUCT(
        JSON_VALUE(raw_data, '$.initial_setup') as initial_fen,
        JSON_VALUE(raw_data, '$.fen') as final_fen
    ) as board_positions,
    
    -- Player accuracies
    STRUCT(
        CAST(JSON_VALUE(raw_data, '$.accuracies.white') AS FLOAT64) as white_accuracy,
        CAST(JSON_VALUE(raw_data, '$.accuracies.black') AS FLOAT64) as black_accuracy
    ) as accuracies,
    
    -- Match information (for matches with multiple games)
    JSON_VALUE(raw_data, '$.match') as match_url,
    
    -- Tournament information
    JSON_VALUE(raw_data, '$.tournament') as tournament_url,
    
    -- TCN (if available)
    JSON_VALUE(raw_data, '$.tcn') as tcn,
    
    -- Metadata
    dp_inserted_at,
    source_file
    
FROM {{ ref('lake_chess__svc_games') }}