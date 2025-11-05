{{
  config(
      materialized='incremental',
      incremental_strategy='merge',
      unique_key='playedAt',
      tags=['spotify', 'lake']
  )
}}

SELECT
    *,
    CURRENT_TIMESTAMP() AS dp_updated_at
FROM {{ ref('lake_spotify__stg_recently_played') }}
{% if is_incremental() %}
    WHERE dp_inserted_at > (SELECT MAX(dp_inserted_at) FROM {{ this }})
{% endif %}
