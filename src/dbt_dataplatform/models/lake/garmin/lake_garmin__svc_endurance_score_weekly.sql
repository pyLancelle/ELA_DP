{{ config(
    dataset=get_schema('lake'),
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['user_profile_pk', 'week_date'],
    tags=["lake", "garmin"]
) }}

-- Pure Lake model for Garmin endurance score weekly data
-- Explodes groupMap to extract weekly endurance metrics
-- Monthly data is handled in separate lake_garmin__svc_endurance_score model

WITH endurance_weekly_exploded AS (
  SELECT
    -- Extract user profile PK
    CAST(JSON_EXTRACT_SCALAR(raw_data, '$.userProfilePK') AS INT64) AS user_profile_pk,
    
    -- Extract week date from groupMap keys
    DATE(REPLACE(REPLACE(week_key, '"', ''), '\\', '')) AS week_date,
    
    -- Extract weekly metrics from groupMap values
    JSON_EXTRACT(raw_data, CONCAT('$.groupMap.', week_key)) AS week_data,
    
    -- Source metadata
    raw_data,
    data_type,
    dp_inserted_at,
    source_file
    
  FROM {{ source('garmin', 'lake_garmin__stg_garmin_raw') }},
  UNNEST(JSON_KEYS(JSON_EXTRACT(raw_data, '$.groupMap'))) AS week_key
  WHERE data_type = 'endurance_score'
    AND JSON_EXTRACT(raw_data, '$.groupMap') IS NOT NULL
    
  {% if is_incremental() %}
    AND dp_inserted_at > (SELECT MAX(dp_inserted_at) FROM {{ this }})
  {% endif %}
),

endurance_weekly_with_rank AS (
  SELECT
    user_profile_pk,
    week_date,
    week_data,
    raw_data,
    data_type,
    dp_inserted_at,
    source_file,
    
    -- Add row number to deduplicate by most recent dp_inserted_at
    ROW_NUMBER() OVER (
      PARTITION BY user_profile_pk, week_date
      ORDER BY dp_inserted_at DESC
    ) AS row_rank
    
  FROM endurance_weekly_exploded
  WHERE user_profile_pk IS NOT NULL
    AND week_date IS NOT NULL
    -- Filter out weeks with null/empty data
    AND JSON_EXTRACT_SCALAR(week_data, '$.groupAverage') IS NOT NULL
)

SELECT
  user_profile_pk,
  week_date,
  week_data,
  raw_data,
  data_type,
  dp_inserted_at,
  source_file

FROM endurance_weekly_with_rank
WHERE row_rank = 1