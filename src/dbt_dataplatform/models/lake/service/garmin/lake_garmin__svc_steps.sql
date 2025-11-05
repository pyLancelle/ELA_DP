{{
  config(
      materialized='incremental',
      incremental_strategy='merge',
      unique_key='startGMT',
      tags=['garmin', 'lake']
  )
}}

SELECT
    *,
    CURRENT_TIMESTAMP() AS dp_updated_at
FROM {{ ref('lake_garmin__stg_steps') }}
{% if is_incremental() %}
    WHERE dp_inserted_at > (SELECT MAX(dp_inserted_at) FROM {{ this }})
{% endif %}