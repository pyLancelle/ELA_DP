{{
  config(
      materialized='incremental',
      incremental_strategy='merge',
      unique_key='activityId'
  )
}}

SELECT
	*,
	CURRENT_TIMESTAMP() AS dp_updated_at
FROM {{ ref('lake_garmin__stg_activities')}}
{% if is_incremental() %}
  WHERE dp_inserted_at > (SELECT MAX(dp_inserted_at) FROM {{ this }})
{% endif %}
QUALIFY ROW_NUMBER() OVER (PARTITION BY activityId ORDER BY dp_inserted_at DESC) = 1