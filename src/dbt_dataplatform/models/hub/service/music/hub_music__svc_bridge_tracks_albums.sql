{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['albumId', 'artistId']
    )
}}
SELECT
    *,
    CURRENT_TIMESTAMP() as dp_updated_at
FROM
    {{ ref('hub_music__stg_bridge_albums_artists') }} AS bta