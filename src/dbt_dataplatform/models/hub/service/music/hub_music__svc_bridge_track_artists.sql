{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['track_key', 'artist_key']
    )
}}
SELECT
    *,
    CASE 
        WHEN bta.artist_position = 1 THEN 'primary'
        ELSE 'featuring'
    END as artist_role,
    CURRENT_TIMESTAMP() as dp_created_at,
    CURRENT_TIMESTAMP() as dp_updated_at
FROM
    {{ ref('hub_music__stg_bridge_track_artists') }} AS bta