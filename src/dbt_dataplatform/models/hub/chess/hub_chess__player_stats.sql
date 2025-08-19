{{ config(dataset=get_schema('hub'), materialized='view', tags=["hub", "chess"]) }}

-- Hub model for Chess.com player statistics with structured objects
-- Organizes rating data and game statistics by time control

SELECT
    username,
    
    -- FIDE rating
    CAST(JSON_VALUE(raw_data, '$.fide') AS INT64) as fide_rating,
    
    -- Daily Chess statistics
    STRUCT(
        CAST(JSON_VALUE(raw_data, '$.chess_daily.last.rating') AS INT64) as last_rating,
        CAST(JSON_VALUE(raw_data, '$.chess_daily.best.rating') AS INT64) as best_rating,
        TIMESTAMP_SECONDS(CAST(JSON_VALUE(raw_data, '$.chess_daily.best.date') AS INT64)) as best_rating_date,
        JSON_VALUE(raw_data, '$.chess_daily.best.game') as best_game_url,
        STRUCT(
            CAST(JSON_VALUE(raw_data, '$.chess_daily.record.win') AS INT64) as wins,
            CAST(JSON_VALUE(raw_data, '$.chess_daily.record.loss') AS INT64) as losses,
            CAST(JSON_VALUE(raw_data, '$.chess_daily.record.draw') AS INT64) as draws
        ) as record
    ) as chess_daily,
    
    -- Chess960 Daily statistics
    STRUCT(
        CAST(JSON_VALUE(raw_data, '$.chess960_daily.last.rating') AS INT64) as last_rating,
        CAST(JSON_VALUE(raw_data, '$.chess960_daily.best.rating') AS INT64) as best_rating,
        TIMESTAMP_SECONDS(CAST(JSON_VALUE(raw_data, '$.chess960_daily.best.date') AS INT64)) as best_rating_date,
        JSON_VALUE(raw_data, '$.chess960_daily.best.game') as best_game_url,
        STRUCT(
            CAST(JSON_VALUE(raw_data, '$.chess960_daily.record.win') AS INT64) as wins,
            CAST(JSON_VALUE(raw_data, '$.chess960_daily.record.loss') AS INT64) as losses,
            CAST(JSON_VALUE(raw_data, '$.chess960_daily.record.draw') AS INT64) as draws
        ) as record
    ) as chess960_daily,
    
    -- Rapid Chess statistics
    STRUCT(
        CAST(JSON_VALUE(raw_data, '$.chess_rapid.last.rating') AS INT64) as last_rating,
        CAST(JSON_VALUE(raw_data, '$.chess_rapid.best.rating') AS INT64) as best_rating,
        TIMESTAMP_SECONDS(CAST(JSON_VALUE(raw_data, '$.chess_rapid.best.date') AS INT64)) as best_rating_date,
        JSON_VALUE(raw_data, '$.chess_rapid.best.game') as best_game_url,
        STRUCT(
            CAST(JSON_VALUE(raw_data, '$.chess_rapid.record.win') AS INT64) as wins,
            CAST(JSON_VALUE(raw_data, '$.chess_rapid.record.loss') AS INT64) as losses,
            CAST(JSON_VALUE(raw_data, '$.chess_rapid.record.draw') AS INT64) as draws
        ) as record
    ) as chess_rapid,
    
    -- Bullet Chess statistics
    STRUCT(
        CAST(JSON_VALUE(raw_data, '$.chess_bullet.last.rating') AS INT64) as last_rating,
        CAST(JSON_VALUE(raw_data, '$.chess_bullet.best.rating') AS INT64) as best_rating,
        TIMESTAMP_SECONDS(CAST(JSON_VALUE(raw_data, '$.chess_bullet.best.date') AS INT64)) as best_rating_date,
        JSON_VALUE(raw_data, '$.chess_bullet.best.game') as best_game_url,
        STRUCT(
            CAST(JSON_VALUE(raw_data, '$.chess_bullet.record.win') AS INT64) as wins,
            CAST(JSON_VALUE(raw_data, '$.chess_bullet.record.loss') AS INT64) as losses,
            CAST(JSON_VALUE(raw_data, '$.chess_bullet.record.draw') AS INT64) as draws
        ) as record
    ) as chess_bullet,
    
    -- Blitz Chess statistics
    STRUCT(
        CAST(JSON_VALUE(raw_data, '$.chess_blitz.last.rating') AS INT64) as last_rating,
        CAST(JSON_VALUE(raw_data, '$.chess_blitz.best.rating') AS INT64) as best_rating,
        TIMESTAMP_SECONDS(CAST(JSON_VALUE(raw_data, '$.chess_blitz.best.date') AS INT64)) as best_rating_date,
        JSON_VALUE(raw_data, '$.chess_blitz.best.game') as best_game_url,
        STRUCT(
            CAST(JSON_VALUE(raw_data, '$.chess_blitz.record.win') AS INT64) as wins,
            CAST(JSON_VALUE(raw_data, '$.chess_blitz.record.loss') AS INT64) as losses,
            CAST(JSON_VALUE(raw_data, '$.chess_blitz.record.draw') AS INT64) as draws
        ) as record
    ) as chess_blitz,
    
    -- Tactics statistics
    STRUCT(
        CAST(JSON_VALUE(raw_data, '$.tactics.highest.rating') AS INT64) as highest_rating,
        TIMESTAMP_SECONDS(CAST(JSON_VALUE(raw_data, '$.tactics.highest.date') AS INT64)) as highest_rating_date,
        CAST(JSON_VALUE(raw_data, '$.tactics.lowest.rating') AS INT64) as lowest_rating,
        TIMESTAMP_SECONDS(CAST(JSON_VALUE(raw_data, '$.tactics.lowest.date') AS INT64)) as lowest_rating_date
    ) as tactics,
    
    -- Puzzle Rush statistics
    STRUCT(
        CAST(JSON_VALUE(raw_data, '$.puzzle_rush.best.total_attempts') AS INT64) as best_total_attempts,
        CAST(JSON_VALUE(raw_data, '$.puzzle_rush.best.score') AS INT64) as best_score
    ) as puzzle_rush,
    
    -- Metadata
    dp_inserted_at,
    source_file
    
FROM {{ ref('lake_chess__svc_player_stats') }}