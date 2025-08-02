{{ config(dataset=get_schema('hub'), materialized='view', tags=["hub", "garmin"]) }}

-- Hub model for Garmin hill score data - based on actual API structure
-- The API returns a nested structure with hillScoreDTOList containing daily records

WITH flattened_hill_scores AS (
  SELECT
    -- Top-level metadata
    CAST(JSON_VALUE(raw_data, '$.userProfilePK') AS INT64) as user_profile_pk,
    DATE(JSON_VALUE(raw_data, '$.startDate')) as period_start_date,
    DATE(JSON_VALUE(raw_data, '$.endDate')) as period_end_date,
    CAST(JSON_VALUE(raw_data, '$.maxScore') AS INT64) as period_max_score,
    
    -- Extract individual daily records from hillScoreDTOList
    JSON_EXTRACT_ARRAY(raw_data, '$.hillScoreDTOList') as hill_score_list,
    
    -- Keep raw data for debugging
    raw_data,
    dp_inserted_at,
    source_file
    
  FROM {{ ref('lake_garmin__svc_hill_score') }}
),

expanded_daily_scores AS (
  SELECT
    -- Top-level fields
    user_profile_pk,
    period_start_date,
    period_end_date,
    period_max_score,
    
    -- Daily hill score record
    daily_record,
    
    -- Extract fields from each daily record
    DATE(JSON_VALUE(daily_record, '$.calendarDate')) as calendar_date,
    CAST(JSON_VALUE(daily_record, '$.userProfilePK') AS INT64) as daily_user_profile_pk,
    CAST(JSON_VALUE(daily_record, '$.deviceId') AS INT64) as device_id,
    
    -- Core hill score metrics
    CAST(JSON_VALUE(daily_record, '$.strengthScore') AS INT64) as strength_score,
    CAST(JSON_VALUE(daily_record, '$.enduranceScore') AS INT64) as endurance_score,
    CAST(JSON_VALUE(daily_record, '$.overallScore') AS INT64) as overall_score,
    
    -- Classification and feedback
    CAST(JSON_VALUE(daily_record, '$.hillScoreClassificationId') AS INT64) as hill_score_classification_id,
    CAST(JSON_VALUE(daily_record, '$.hillScoreFeedbackPhraseId') AS INT64) as hill_score_feedback_phrase_id,
    
    -- VO2 Max metrics (often null)
    CAST(JSON_VALUE(daily_record, '$.vo2Max') AS FLOAT64) as vo2_max,
    CAST(JSON_VALUE(daily_record, '$.vo2MaxPreciseValue') AS FLOAT64) as vo2_max_precise_value,
    
    -- Device information
    CAST(JSON_VALUE(daily_record, '$.primaryTrainingDevice') AS BOOL) as primary_training_device,
    
    -- Raw data for debugging
    raw_data,
    dp_inserted_at,
    source_file
    
  FROM flattened_hill_scores,
  UNNEST(hill_score_list) as daily_record
)

SELECT
  -- Primary date and identification
  calendar_date as score_date,
  calendar_date,
  user_profile_pk,
  daily_user_profile_pk,
  device_id,
  
  -- Period context
  period_start_date,
  period_end_date,
  period_max_score,
  
  -- Core hill score metrics
  strength_score,
  endurance_score,
  overall_score,
  
  -- Classification and feedback
  hill_score_classification_id,
  hill_score_feedback_phrase_id,
  
  -- VO2 Max metrics
  vo2_max,
  vo2_max_precise_value,
  
  -- Device information
  primary_training_device,
  
  -- Raw data for debugging and future fields
  raw_data,
  
  -- Metadata
  dp_inserted_at,
  source_file

FROM expanded_daily_scores
WHERE calendar_date IS NOT NULL