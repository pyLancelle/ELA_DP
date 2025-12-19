{{
  config(
      materialized='incremental',
      incremental_strategy='merge',
      unique_key='addedAt',
      tags=['spotify', 'lake']
  )
}}

SELECT
    *,
    CURRENT_TIMESTAMP() AS _dp_updated_at
FROM {{ ref('lake_spotify__stg_saved_albums') }}
{% if is_incremental() %}
    WHERE _dp_inserted_at > (SELECT MAX(_dp_updated_at) FROM {{ this }})
{% endif %}
