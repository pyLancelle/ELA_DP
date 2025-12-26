{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['playedAt'],
        partition_by={
            'field': 'playedAt',
            'data_type': 'timestamp',
            'granularity': 'day'
        },
        tags=['spotify', 'hub']
    )
}}
SELECT
    * EXCEPT(_dp_inserted_at),
    CURRENT_TIMESTAMP() AS _dp_updated_at
FROM
    {{ ref('hub_music__stg_fact_played') }}
{% if is_incremental() %}
WHERE _dp_inserted_at > (SELECT MAX(_dp_updated_at) FROM {{ this }})
{% endif %}
