{{
  config(
      materialized='incremental',
      incremental_strategy='merge',
      unique_key='calendardate'
  )
}}

SELECT
    *,
    CURRENT_TIMESTAMP() AS dp_updated_at
FROM {{ ref('lake_garmin__stg_sleep') }}
WHERE
    id IS NOT null
    {% if is_incremental() %}
        AND dp_inserted_at > (SELECT MAX(dp_inserted_at) FROM {{ this }})
    {% endif %}