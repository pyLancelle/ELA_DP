{{ config(dataset=get_schema('hub'), materialized='view', tags=["hub", "chess"]) }}

-- Hub model for Chess.com clubs data with structured objects

SELECT
    club_id,
    username,
    
    -- Basic club information
    JSON_VALUE(raw_data, '$.name') as club_name,
    JSON_VALUE(raw_data, '$.description') as description,
    JSON_VALUE(raw_data, '$.club_id') as club_slug,
    JSON_VALUE(raw_data, '$.url') as club_url,
    JSON_VALUE(raw_data, '$.icon') as icon_url,
    JSON_VALUE(raw_data, '$.country') as country,
    
    -- Membership information
    STRUCT(
        CAST(JSON_VALUE(raw_data, '$.members_count') AS INT64) as total_members,
        TIMESTAMP_SECONDS(CAST(JSON_VALUE(raw_data, '$.created') AS INT64)) as created_at,
        TIMESTAMP_SECONDS(CAST(JSON_VALUE(raw_data, '$.last_activity') AS INT64)) as last_activity_at
    ) as membership,
    
    -- Club settings
    STRUCT(
        CAST(JSON_VALUE(raw_data, '$.join_request') AS BOOL) as requires_join_request,
        CAST(JSON_VALUE(raw_data, '$.visibility') AS STRING) as visibility
    ) as settings,
    
    -- Activity metrics
    STRUCT(
        JSON_VALUE(raw_data, '$.admin') as admin_usernames,
        CAST(JSON_VALUE(raw_data, '$.average_daily_rating') AS INT64) as avg_daily_rating
    ) as activity_metrics,
    
    -- Metadata
    dp_inserted_at,
    source_file
    
FROM {{ ref('lake_chess__svc_clubs') }}