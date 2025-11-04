{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['artistId']
    )
}}
SELECT
    *,
    CURRENT_TIMESTAMP() as dp_updated_at
FROM
    {{ ref('hub_music__stg_dim_artists') }} AS bta