{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['trackId']
    )
}}
SELECT
    *,
    CURRENT_TIMESTAMP() as dp_updated_at
FROM
    {{ ref('hub_music__stg_dim_tracks') }} AS bta