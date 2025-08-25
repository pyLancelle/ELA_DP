{{ config(dataset=get_schema('product'), materialized='view', tags=["product", "chess"]) }}

-- Chess.com Game Analysis
-- Provides detailed game statistics and performance insights

WITH game_details AS (
  SELECT
    game_id,
    username,
    time_class,
    is_rated,
    
    -- Game timing
    timing.start_time,
    timing.end_time,
    DATETIME_DIFF(timing.end_time, timing.start_time, MINUTE) as game_duration_minutes,
    
    -- Player information
    white_player.username as white_username,
    white_player.rating as white_rating,
    white_player.result as white_result,
    black_player.username as black_username,
    black_player.rating as black_rating,
    black_player.result as black_result,
    
    -- Opening information
    opening.eco_code,
    opening.opening_name,
    
    -- Accuracy data
    accuracies.white_accuracy,
    accuracies.black_accuracy,
    
    -- Determine user's role and result
    CASE 
      WHEN white_player.username = username THEN 'white'
      WHEN black_player.username = username THEN 'black'
      ELSE 'unknown'
    END as user_color,
    
    CASE 
      WHEN white_player.username = username THEN white_player.result
      WHEN black_player.username = username THEN black_player.result
      ELSE 'unknown'
    END as user_result,
    
    CASE 
      WHEN white_player.username = username THEN white_player.rating
      WHEN black_player.username = username THEN black_player.rating
      ELSE NULL
    END as user_rating,
    
    CASE 
      WHEN white_player.username = username THEN black_player.rating
      WHEN black_player.username = username THEN white_player.rating
      ELSE NULL
    END as opponent_rating,
    
    CASE 
      WHEN white_player.username = username THEN white_accuracy
      WHEN black_player.username = username THEN black_accuracy
      ELSE NULL
    END as user_accuracy,
    
    dp_inserted_at
    
  FROM {{ ref('hub_chess__games') }}
),

game_stats AS (
  SELECT
    *,
    
    -- Rating difference analysis
    user_rating - opponent_rating as rating_advantage,
    
    -- Performance categories
    CASE 
      WHEN user_result = 'win' THEN 1
      WHEN user_result = 'resigned' THEN 0
      WHEN user_result = 'checkmated' THEN 0
      WHEN user_result = 'timeout' THEN 0
      WHEN user_result = 'abandoned' THEN 0
      ELSE 0.5  -- draws, stalemate, etc.
    END as score_points,
    
    -- Expected score based on rating difference (Elo formula)
    1 / (1 + POWER(10, -(user_rating - opponent_rating) / 400.0)) as expected_score,
    
    -- Game difficulty
    CASE 
      WHEN ABS(user_rating - opponent_rating) <= 50 THEN 'Even'
      WHEN user_rating - opponent_rating > 50 THEN 'Favored'
      WHEN opponent_rating - user_rating > 50 THEN 'Underdog'
      ELSE 'Unknown'
    END as game_difficulty
    
  FROM game_details
  WHERE user_color != 'unknown'
),

performance_metrics AS (
  SELECT
    *,
    
    -- Performance vs expectation
    score_points - expected_score as performance_vs_expected,
    
    -- Accuracy performance
    CASE 
      WHEN user_accuracy >= 90 THEN 'Excellent'
      WHEN user_accuracy >= 80 THEN 'Good'
      WHEN user_accuracy >= 70 THEN 'Average'
      WHEN user_accuracy >= 60 THEN 'Below Average'
      ELSE 'Poor'
    END as accuracy_grade,
    
    -- Opening family classification
    CASE 
      WHEN opening.eco_code LIKE 'A%' THEN 'Flank Openings'
      WHEN opening.eco_code LIKE 'B%' THEN 'Semi-Open Games'
      WHEN opening.eco_code LIKE 'C%' THEN 'Open Games'
      WHEN opening.eco_code LIKE 'D%' THEN 'Closed Games'
      WHEN opening.eco_code LIKE 'E%' THEN 'Indian Systems'
      ELSE 'Other'
    END as opening_family
    
  FROM game_stats
)

SELECT
  game_id,
  username,
  
  -- Game context
  time_class,
  is_rated,
  start_time,
  game_duration_minutes,
  
  -- Player roles and results
  user_color,
  user_result,
  score_points,
  
  -- Ratings and expectations
  user_rating,
  opponent_rating,
  rating_advantage,
  expected_score,
  performance_vs_expected,
  game_difficulty,
  
  -- Accuracy analysis
  user_accuracy,
  accuracy_grade,
  
  -- Opening analysis
  eco_code,
  opening_name,
  opening_family,
  
  -- Opponents
  CASE 
    WHEN user_color = 'white' THEN black_username
    ELSE white_username
  END as opponent_username,
  
  dp_inserted_at

FROM performance_metrics
ORDER BY start_time DESC