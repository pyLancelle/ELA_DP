{{ config(dataset=get_schema('hub'), materialized='view', tags=["hub", "chess"]) }}

-- Hub model for Chess.com player profile data with structured objects
-- Uses STRUCT to preserve logical groupings for efficient analysis

SELECT
    -- Core identifiers and basic info
    username,
    JSON_VALUE(raw_data, '$.name') as full_name,
    JSON_VALUE(raw_data, '$.title') as chess_title,
    CAST(JSON_VALUE(raw_data, '$.player_id') AS INT64) as player_id,
    JSON_VALUE(raw_data, '$.status') as account_status,
    CAST(JSON_VALUE(raw_data, '$.is_streamer') AS BOOL) as is_streamer,
    CAST(JSON_VALUE(raw_data, '$.verified') AS BOOL) as is_verified,
    
    -- Profile images and social
    STRUCT(
        JSON_VALUE(raw_data, '$.avatar') as avatar_url,
        JSON_VALUE(raw_data, '$.twitch_url') as twitch_url,
        JSON_QUERY_ARRAY(raw_data, '$.streaming_platforms') as streaming_platforms
    ) as media,
    
    -- Location information
    STRUCT(
        JSON_VALUE(raw_data, '$.location') as location,
        JSON_VALUE(raw_data, '$.country') as country_url
    ) as location_info,
    
    -- Activity timestamps
    STRUCT(
        TIMESTAMP_SECONDS(CAST(JSON_VALUE(raw_data, '$.joined') AS INT64)) as account_created_at,
        TIMESTAMP_SECONDS(CAST(JSON_VALUE(raw_data, '$.last_online') AS INT64)) as last_online_at
    ) as activity,
    
    -- Social metrics
    STRUCT(
        CAST(JSON_VALUE(raw_data, '$.followers') AS INT64) as followers_count
    ) as social,
    
    -- League and competitive info
    STRUCT(
        JSON_VALUE(raw_data, '$.league') as league_url
    ) as competitive,
    
    -- Profile URLs
    STRUCT(
        JSON_VALUE(raw_data, '$.@id') as api_url,
        JSON_VALUE(raw_data, '$.url') as profile_url
    ) as urls,
    
    -- Metadata
    dp_inserted_at,
    source_file
    
FROM {{ ref('lake_chess__svc_player_profile') }}