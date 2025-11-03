{{
  config(
      materialized='incremental',
      incremental_strategy='merge',
      unique_key='calendarDate'
  )
}}

SELECT
	*,
	CURRENT_TIMESTAMP() AS dp_updated_at
FROM {{ ref('lake_garmin__stg_weight')}}
{% if is_incremental() %}
  WHERE dp_inserted_at > (SELECT MAX(dp_inserted_at) FROM {{ this }})
{% endif %}
QUALIFY ROW_NUMBER() OVER (PARTITION BY calendarDate ORDER BY dp_inserted_at DESC, timestampGMT DESC) = 1