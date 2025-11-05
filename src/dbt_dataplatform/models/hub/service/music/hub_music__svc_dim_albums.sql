{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['albumId'],
        tags=['music', 'hub']
    )
}}
SELECT
    *,
    CURRENT_TIMESTAMP() AS dp_updated_at
FROM
    {{ ref('hub_music__stg_dim_albums') }}
