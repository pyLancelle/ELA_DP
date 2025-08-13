{{ config(
    dataset=get_schema('lake'),
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['user_profile_pk', 'period_start_date', 'period_end_date'],
    tags=["lake", "garmin"]
) }}

-- Lake model for Garmin endurance score weekly groupMap data
-- Stores the complete groupMap for hub layer to parse daily data
-- Simplified approach due to BigQuery JSON path limitations with dynamic keys

WITH endurance_score_weekly_with_rank AS (
  SELECT
    CAST(JSON_EXTRACT_SCALAR(raw_data, '$.userProfilePK') AS INT64) AS user_profile_pk,
    
    -- Extract period dates for unique key
    DATE(JSON_EXTRACT_SCALAR(raw_data, '$.startDate')) AS period_start_date,
    DATE(JSON_EXTRACT_SCALAR(raw_data, '$.endDate')) AS period_end_date,
    
    -- Store complete groupMap for hub layer processing
    JSON_EXTRACT(raw_data, '$.groupMap') AS group_map,
    
    -- Metadata
    raw_data,
    data_type,
    dp_inserted_at,
    source_file,
    
    -- Add row number to deduplicate by most recent dp_inserted_at
    ROW_NUMBER() OVER (
      PARTITION BY CAST(JSON_EXTRACT_SCALAR(raw_data, '$.userProfilePK') AS INT64),
                   DATE(JSON_EXTRACT_SCALAR(raw_data, '$.startDate')),
                   DATE(JSON_EXTRACT_SCALAR(raw_data, '$.endDate'))
      ORDER BY dp_inserted_at DESC
    ) AS row_rank
    
  FROM {{ source('garmin', 'lake_garmin__stg_garmin_raw') }}
  WHERE data_type = 'endurance_score'
    AND JSON_EXTRACT(raw_data, '$.groupMap') IS NOT NULL
    AND JSON_EXTRACT_SCALAR(raw_data, '$.userProfilePK') IS NOT NULL
    
  {% if is_incremental() %}
    AND dp_inserted_at > (SELECT MAX(dp_inserted_at) FROM {{ this }})
  {% endif %}
)

SELECT
  user_profile_pk,
  period_start_date,
  period_end_date,
  group_map,
  raw_data,
  data_type,
  dp_inserted_at,
  source_file

FROM endurance_score_weekly_with_rank
WHERE row_rank = 1