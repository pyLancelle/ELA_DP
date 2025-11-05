{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['albumId', 'artistId'],
        tags=['music', 'hub']
    )
}}
SELECT
    *,
    CURRENT_TIMESTAMP() AS dp_updated_at
FROM
    {{ ref('hub_music__stg_bridge_albums_artists') }}
