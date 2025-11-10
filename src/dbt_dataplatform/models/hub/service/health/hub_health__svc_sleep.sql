{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['date'],
        tags=['health', 'hub'],
    )
}}
SELECT
    *,
    CURRENT_TIMESTAMP() AS dp_updated_at
FROM
    {{ ref('hub_health__stg_sleep') }}
