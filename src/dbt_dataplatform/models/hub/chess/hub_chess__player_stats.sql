{{ config(dataset=get_schema('hub'), materialized='view', tags=["hub", "chess"]) }}

-- Hub model for Chess.com player statistics with structured objects
-- Uses STRUCT to preserve logical groupings for different game modes

SELECT
    -- Core identifiers (kept flat for easy filtering/joining)
    username,
    
    -- Chess Rapid stats as STRUCT
    STRUCT(
        CAST(JSON_VALUE(raw_data, '$.chess_rapid.last.rating') AS INT64) as current_rating,
        TIMESTAMP_SECONDS(CAST(JSON_VALUE(raw_data, '$.chess_rapid.last.date') AS INT64)) as last_game_date,
        CAST(JSON_VALUE(raw_data, '$.chess_rapid.last.rd') AS INT64) as rating_deviation,
        CAST(JSON_VALUE(raw_data, '$.chess_rapid.best.rating') AS INT64) as best_rating,
        TIMESTAMP_SECONDS(CAST(JSON_VALUE(raw_data, '$.chess_rapid.best.date') AS INT64)) as best_rating_date,
        CAST(JSON_VALUE(raw_data, '$.chess_rapid.record.win') AS INT64) as wins,
        CAST(JSON_VALUE(raw_data, '$.chess_rapid.record.loss') AS INT64) as losses,
        CAST(JSON_VALUE(raw_data, '$.chess_rapid.record.draw') AS INT64) as draws
    ) as rapid,
    
    -- Chess Blitz stats as STRUCT
    STRUCT(
        CAST(JSON_VALUE(raw_data, '$.chess_blitz.last.rating') AS INT64) as current_rating,
        TIMESTAMP_SECONDS(CAST(JSON_VALUE(raw_data, '$.chess_blitz.last.date') AS INT64)) as last_game_date,
        CAST(JSON_VALUE(raw_data, '$.chess_blitz.last.rd') AS INT64) as rating_deviation,
        CAST(JSON_VALUE(raw_data, '$.chess_blitz.best.rating') AS INT64) as best_rating,
        TIMESTAMP_SECONDS(CAST(JSON_VALUE(raw_data, '$.chess_blitz.best.date') AS INT64)) as best_rating_date,
        CAST(JSON_VALUE(raw_data, '$.chess_blitz.record.win') AS INT64) as wins,
        CAST(JSON_VALUE(raw_data, '$.chess_blitz.record.loss') AS INT64) as losses,
        CAST(JSON_VALUE(raw_data, '$.chess_blitz.record.draw') AS INT64) as draws
    ) as blitz,
    
    -- Chess Bullet stats as STRUCT
    STRUCT(
        CAST(JSON_VALUE(raw_data, '$.chess_bullet.last.rating') AS INT64) as current_rating,
        TIMESTAMP_SECONDS(CAST(JSON_VALUE(raw_data, '$.chess_bullet.last.date') AS INT64)) as last_game_date,
        CAST(JSON_VALUE(raw_data, '$.chess_bullet.last.rd') AS INT64) as rating_deviation,
        CAST(JSON_VALUE(raw_data, '$.chess_bullet.best.rating') AS INT64) as best_rating,
        TIMESTAMP_SECONDS(CAST(JSON_VALUE(raw_data, '$.chess_bullet.best.date') AS INT64)) as best_rating_date,
        CAST(JSON_VALUE(raw_data, '$.chess_bullet.record.win') AS INT64) as wins,
        CAST(JSON_VALUE(raw_data, '$.chess_bullet.record.loss') AS INT64) as losses,
        CAST(JSON_VALUE(raw_data, '$.chess_bullet.record.draw') AS INT64) as draws
    ) as bullet,
    
    -- Chess Daily stats as STRUCT
    STRUCT(
        CAST(JSON_VALUE(raw_data, '$.chess_daily.last.rating') AS INT64) as current_rating,
        TIMESTAMP_SECONDS(CAST(JSON_VALUE(raw_data, '$.chess_daily.last.date') AS INT64)) as last_game_date,
        CAST(JSON_VALUE(raw_data, '$.chess_daily.last.rd') AS INT64) as rating_deviation,
        CAST(JSON_VALUE(raw_data, '$.chess_daily.best.rating') AS INT64) as best_rating,
        TIMESTAMP_SECONDS(CAST(JSON_VALUE(raw_data, '$.chess_daily.best.date') AS INT64)) as best_rating_date,
        CAST(JSON_VALUE(raw_data, '$.chess_daily.record.win') AS INT64) as wins,
        CAST(JSON_VALUE(raw_data, '$.chess_daily.record.loss') AS INT64) as losses,
        CAST(JSON_VALUE(raw_data, '$.chess_daily.record.draw') AS INT64) as draws
    ) as daily,
    
    -- Puzzle stats as STRUCT
    STRUCT(
        CAST(JSON_VALUE(raw_data, '$.puzzle_rush.best.total_attempts') AS INT64) as rush_best_attempts,
        CAST(JSON_VALUE(raw_data, '$.puzzle_rush.best.score') AS INT64) as rush_best_score,
        CAST(JSON_VALUE(raw_data, '$.tactics.highest.rating') AS INT64) as tactics_highest_rating,
        CAST(JSON_VALUE(raw_data, '$.tactics.lowest.rating') AS INT64) as tactics_lowest_rating
    ) as puzzles,
    
    -- Metadata
    dp_inserted_at,
    source_file

FROM {{ ref('lake_chess__svc_player_stats') }}
WHERE username IS NOT NULL