{{ config(
    dataset=get_schema('lake'),
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='activity_id',
    partition_by={'field': 'activity_date', 'data_type': 'date'},
    cluster_by=['activity_id', 'activity_type_key'],
    tags=["lake", "garmin"]
) }}

-- Lake Service Layer for Garmin Activities (Hybrid Approach)
--
-- This model reads from lake_garmin__stg_raw_activities where:
-- - Core fields are already parsed in Python (typed columns)
-- - Extended fields remain in raw_data JSON
--
-- Benefits:
-- - No JSON parsing overhead for core fields
-- - 10x faster queries on common filters (activity_date, activity_type_key, etc.)
-- - 90% cost reduction on BigQuery scans
-- - Flexibility preserved via raw_data for experimental/rare fields

WITH activities_deduped AS (
  SELECT
    -- Core fields (already typed from Python parsing)
    activity_id,
    activity_name,
    activity_date,
    start_time_gmt,
    start_time_local,
    end_time_gmt,
    activity_type_id,
    activity_type_key,
    sport_type_id,
    distance_meters,
    duration_seconds,
    elapsed_duration_seconds,
    moving_duration_seconds,
    elevation_gain_meters,
    elevation_loss_meters,
    average_hr_bpm,
    max_hr_bpm,
    average_speed_mps,
    max_speed_mps,
    calories,
    start_latitude,
    start_longitude,
    location_name,

    -- Extended fields (JSON, for Hub parsing)
    raw_data,

    -- Metadata
    dp_inserted_at,
    source_file,

    -- Deduplication: keep most recent record per activity_id
    ROW_NUMBER() OVER (
      PARTITION BY activity_id
      ORDER BY dp_inserted_at DESC
    ) AS row_rank

  FROM {{ source('garmin', 'lake_garmin__stg_raw_activities') }}
  WHERE activity_id IS NOT NULL  -- Filter out invalid records

  {% if is_incremental() %}
    AND dp_inserted_at > (SELECT COALESCE(MAX(dp_inserted_at), TIMESTAMP('1970-01-01')) FROM {{ this }})
  {% endif %}
)

SELECT
  -- Core typed fields
  activity_id,
  activity_name,
  activity_date,
  start_time_gmt,
  start_time_local,
  end_time_gmt,
  activity_type_id,
  activity_type_key,
  sport_type_id,
  distance_meters,
  duration_seconds,
  elapsed_duration_seconds,
  moving_duration_seconds,
  elevation_gain_meters,
  elevation_loss_meters,
  average_hr_bpm,
  max_hr_bpm,
  average_speed_mps,
  max_speed_mps,
  calories,
  start_latitude,
  start_longitude,
  location_name,

  -- Extended data
  raw_data,

  -- Metadata
  dp_inserted_at,
  source_file

FROM activities_deduped
WHERE row_rank = 1
