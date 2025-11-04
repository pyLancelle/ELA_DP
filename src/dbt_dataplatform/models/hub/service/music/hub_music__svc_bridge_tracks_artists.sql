{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['trackId', 'artistId']
    )
}}
SELECT
    *,
    CURRENT_TIMESTAMP() as dp_updated_at
FROM
    {{ ref('hub_music__stg_bridge_tracks_artists') }} AS bta