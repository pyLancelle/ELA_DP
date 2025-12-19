{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['artistId'],
        tags=['music', 'hub']
    )
}}
SELECT
    * EXCEPT(max__dp_inserted_at),
    CURRENT_TIMESTAMP() AS _dp_updated_at
FROM
    {{ ref('hub_music__stg_dim_artists') }}
{% if is_incremental() %}
WHERE max__dp_inserted_at > (SELECT MAX(_dp_updated_at) FROM {{ this }})
{% endif %}
