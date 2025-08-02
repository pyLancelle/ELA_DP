-- Vue dbt pour la dernière activité de course à pied (3 derniers jours)
-- À déployer dans votre repo dbt séparé

WITH recent_running_activities AS (
  SELECT 
    -- Identifiants de base
    activity_id,
    activity_name,
    activity_type.type_key as activity_type,
    
    -- Timestamps
    start_time_gmt,
    start_time_local,
    DATETIME(start_time_local) as activity_datetime,
    
    -- Métriques de base
    duration_seconds,
    ROUND(duration_seconds / 60, 1) as duration_minutes,
    distance_meters,
    ROUND(distance_meters / 1000, 2) as distance_km,
    
    -- Vitesse et allure
    speed.average_mps as avg_speed_mps,
    speed.max_mps as max_speed_mps,
    CASE 
      WHEN speed.average_mps > 0 
      THEN ROUND((1000 / speed.average_mps) / 60, 2) 
      ELSE NULL 
    END as pace_min_per_km,
    
    -- Calories
    calories.total_calories as calories,
    calories.bmr_calories,
    
    -- Fréquence cardiaque
    heart_rate.average_bpm as avg_heart_rate,
    heart_rate.max_bpm as max_heart_rate,
    
    -- Métriques de course spécifiques
    running_metrics.avg_cadence_spm as cadence,
    running_metrics.total_steps as total_steps,
    running_metrics.avg_vertical_oscillation_cm as vertical_oscillation,
    running_metrics.avg_ground_contact_time_ms as ground_contact_time,
    running_metrics.avg_stride_length_cm as stride_length,
    
    -- Dénivelé
    elevation.gain_meters as elevation_gain,
    elevation.loss_meters as elevation_loss,
    
    -- Localisation
    location.start_latitude,
    location.start_longitude,
    location.location_name,
    
    -- Caractéristiques de l'activité
    activity_features.is_favorite,
    activity_features.is_personal_record,
    
    -- Effet d'entraînement
    training_effect.aerobic_effect,
    training_effect.anaerobic_effect,
    training_effect.training_load,
    
    -- Calcul des jours depuis l'activité
    DATE_DIFF(CURRENT_DATE(), DATE(start_time_local), DAY) as days_ago

  FROM {{ ref('hub_garmin__activities') }}
  WHERE 
    -- Filtrer les activités des 3 derniers jours
    DATE(start_time_local) >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
    -- Filtrer uniquement les activités de course
    AND activity_type.type_key IN ('running', 'track_running', 'trail_running')
    -- Filtrer les activités avec une distance minimale (éviter les faux démarrages)
    AND distance_meters >= 500
),

-- Récupérer uniquement la plus récente
latest_activity AS (
  SELECT *,
    -- Rang par date (la plus récente en premier)
    ROW_NUMBER() OVER (ORDER BY start_time_local DESC) as rn
  FROM recent_running_activities
)

SELECT 
  activity_id,
  activity_name,
  activity_type,
  start_time_gmt,
  start_time_local,
  activity_datetime,
  duration_seconds,
  duration_minutes,
  distance_meters,
  distance_km,
  avg_speed_mps,
  max_speed_mps,
  pace_min_per_km,
  calories,
  bmr_calories,
  avg_heart_rate,
  max_heart_rate,
  cadence,
  total_steps,
  vertical_oscillation,
  ground_contact_time,
  stride_length,
  elevation_gain,
  elevation_loss,
  start_latitude,
  start_longitude,
  location_name,
  is_favorite,
  is_personal_record,
  aerobic_effect,
  anaerobic_effect,
  training_load,
  days_ago,
  
  -- Métadonnées
  CURRENT_TIMESTAMP() as view_created_at

FROM latest_activity
WHERE rn = 1  -- Garder seulement la plus récente