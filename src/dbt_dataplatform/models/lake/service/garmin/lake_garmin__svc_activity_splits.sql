{{
  config(
      materialized='incremental',
      incremental_strategy='merge',
      unique_key='activityId',
      tags=['garmin', 'lake']
  )
}}

SELECT
    *,
    CURRENT_TIMESTAMP() AS _dp_updated_at
FROM {{ ref('lake_garmin__stg_activity_splits') }}
{% if is_incremental() %}
    WHERE _dp_inserted_at > (SELECT MAX(_dp_updated_at) FROM {{ this }})
{% endif %}
