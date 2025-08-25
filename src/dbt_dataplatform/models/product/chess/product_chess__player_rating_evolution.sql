{{ config(dataset=get_schema('product'), materialized='view', tags=["product", "chess"]) }}

-- Chess.com Player Rating Evolution Analysis
-- Tracks rating changes across different time controls

WITH player_current_stats AS (
  SELECT
    username,
    chess_daily.last_rating as daily_current,
    chess_daily.best_rating as daily_best,
    chess_rapid.last_rating as rapid_current,
    chess_rapid.best_rating as rapid_best,
    chess_bullet.last_rating as bullet_current,
    chess_bullet.best_rating as bullet_best,
    chess_blitz.last_rating as blitz_current,
    chess_blitz.best_rating as blitz_best,
    fide_rating,
    dp_inserted_at
  FROM {{ ref('hub_chess__player_stats') }}
),

rating_analysis AS (
  SELECT
    username,
    
    -- Current ratings by time control
    daily_current,
    rapid_current,
    bullet_current,
    blitz_current,
    fide_rating,
    
    -- Peak ratings by time control
    daily_best,
    rapid_best,
    bullet_best,
    blitz_best,
    
    -- Rating differences from peak
    COALESCE(daily_current - daily_best, 0) as daily_peak_diff,
    COALESCE(rapid_current - rapid_best, 0) as rapid_peak_diff,
    COALESCE(bullet_current - bullet_best, 0) as bullet_peak_diff,
    COALESCE(blitz_current - blitz_best, 0) as blitz_peak_diff,
    
    -- Overall performance indicators
    CASE 
      WHEN rapid_current IS NOT NULL THEN rapid_current
      WHEN blitz_current IS NOT NULL THEN blitz_current
      WHEN bullet_current IS NOT NULL THEN bullet_current
      WHEN daily_current IS NOT NULL THEN daily_current
      ELSE NULL
    END as primary_rating,
    
    -- Rating spread analysis
    GREATEST(
      COALESCE(daily_current, 0),
      COALESCE(rapid_current, 0), 
      COALESCE(bullet_current, 0),
      COALESCE(blitz_current, 0)
    ) - LEAST(
      COALESCE(daily_current, 9999),
      COALESCE(rapid_current, 9999),
      COALESCE(bullet_current, 9999),
      COALESCE(blitz_current, 9999)
    ) as rating_spread,
    
    dp_inserted_at
  FROM player_current_stats
)

SELECT
  username,
  
  -- Current ratings
  daily_current as daily_rating,
  rapid_current as rapid_rating,
  bullet_current as bullet_rating,
  blitz_current as blitz_rating,
  fide_rating,
  primary_rating,
  
  -- Peak ratings
  daily_best as daily_peak,
  rapid_best as rapid_peak,
  bullet_best as bullet_peak,
  blitz_best as blitz_peak,
  
  -- Performance vs peak
  daily_peak_diff,
  rapid_peak_diff,
  bullet_peak_diff,
  blitz_peak_diff,
  
  -- Rating consistency
  rating_spread,
  CASE 
    WHEN rating_spread < 100 THEN 'Very Consistent'
    WHEN rating_spread < 200 THEN 'Consistent'
    WHEN rating_spread < 300 THEN 'Moderate Variation'
    ELSE 'High Variation'
  END as consistency_level,
  
  -- Playing strength category
  CASE 
    WHEN primary_rating >= 2400 THEN 'Master+'
    WHEN primary_rating >= 2200 THEN 'Expert'
    WHEN primary_rating >= 2000 THEN 'Advanced'
    WHEN primary_rating >= 1800 THEN 'Intermediate'
    WHEN primary_rating >= 1600 THEN 'Improving'
    WHEN primary_rating >= 1400 THEN 'Beginner+'
    WHEN primary_rating >= 1200 THEN 'Beginner'
    ELSE 'Novice'
  END as skill_category,
  
  dp_inserted_at

FROM rating_analysis
WHERE primary_rating IS NOT NULL