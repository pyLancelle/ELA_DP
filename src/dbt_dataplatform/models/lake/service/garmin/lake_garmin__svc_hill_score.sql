{{
  config(
      materialized='incremental',
      incremental_strategy='merge',
      unique_key='startDate'
  )
}}

SELECT
	*,
	CURRENT_TIMESTAMP() AS dp_updated_at
FROM {{ ref('lake_garmin__stg_hill_score')}}
{% if is_incremental() %}
  WHERE dp_inserted_at > (SELECT MAX(dp_inserted_at) FROM {{ this }})
{% endif %}
QUALIFY ROW_NUMBER() OVER (PARTITION BY startDate ORDER BY endDate DESC, dp_inserted_at DESC) = 1