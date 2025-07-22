{{ config(dataset=get_schema('lake')) }}

-- Pure activities data extraction from staging_garmin_raw
-- Source: activities data type from Garmin Connect API

SELECT
    -- Core activity identifiers
    JSON_EXTRACT_SCALAR(raw_data, '$.activityId') AS activity_id,
    JSON_EXTRACT_SCALAR(raw_data, '$.activityName') AS activity_name,
    
    -- Timestamps (convert to proper TIMESTAMP type)
    SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', JSON_EXTRACT_SCALAR(raw_data, '$.startTimeGMT')) AS start_time_gmt,
    SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', JSON_EXTRACT_SCALAR(raw_data, '$.startTimeLocal')) AS start_time_local,
    SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', JSON_EXTRACT_SCALAR(raw_data, '$.endTimeGMT')) AS end_time_gmt,
    
    -- Activity classification
    JSON_EXTRACT_SCALAR(raw_data, '$.activityType.typeKey') AS activity_type_key,
    SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.activityType.typeId') AS INT64) AS activity_type_id,
    JSON_EXTRACT_SCALAR(raw_data, '$.eventType.typeKey') AS event_type_key,
    SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.sportTypeId') AS INT64) AS sport_type_id,
    
    -- Core metrics
    SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.distance') AS FLOAT64) AS distance_meters,
    SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.duration') AS FLOAT64) AS duration_seconds,
    SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.movingDuration') AS FLOAT64) AS moving_duration_seconds,
    SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.elapsedDuration') AS FLOAT64) AS elapsed_duration_seconds,
    
    -- Elevation data
    SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.elevationGain') AS FLOAT64) AS elevation_gain_meters,
    SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.elevationLoss') AS FLOAT64) AS elevation_loss_meters,
    SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.minElevation') AS FLOAT64) AS min_elevation_meters,
    SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.maxElevation') AS FLOAT64) AS max_elevation_meters,
    
    -- Speed metrics
    SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.averageSpeed') AS FLOAT64) AS average_speed_ms,
    SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.maxSpeed') AS FLOAT64) AS max_speed_ms,
    
    -- GPS location
    SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.startLatitude') AS FLOAT64) AS start_latitude,
    SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.startLongitude') AS FLOAT64) AS start_longitude,
    SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.endLatitude') AS FLOAT64) AS end_latitude,
    SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.endLongitude') AS FLOAT64) AS end_longitude,
    JSON_EXTRACT_SCALAR(raw_data, '$.locationName') AS location_name,
    
    -- Energy metrics
    SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.calories') AS FLOAT64) AS calories,
    SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.bmrCalories') AS FLOAT64) AS bmr_calories,
    
    -- Heart rate metrics
    SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.averageHR') AS INT64) AS average_heart_rate,
    SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.maxHR') AS INT64) AS max_heart_rate,
    
    -- Heart rate zones (time in seconds)
    SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.hrTimeInZone_1') AS FLOAT64) AS hr_zone_1_seconds,
    SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.hrTimeInZone_2') AS FLOAT64) AS hr_zone_2_seconds,
    SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.hrTimeInZone_3') AS FLOAT64) AS hr_zone_3_seconds,
    SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.hrTimeInZone_4') AS FLOAT64) AS hr_zone_4_seconds,
    SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.hrTimeInZone_5') AS FLOAT64) AS hr_zone_5_seconds,
    
    -- Power metrics (for cycling/running power)
    SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.avgPower') AS INT64) AS average_power_watts,
    SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.maxPower') AS INT64) AS max_power_watts,
    SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.normPower') AS INT64) AS normalized_power_watts,
    
    -- Power zones (time in seconds)  
    SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.powerTimeInZone_1') AS FLOAT64) AS power_zone_1_seconds,
    SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.powerTimeInZone_2') AS FLOAT64) AS power_zone_2_seconds,
    SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.powerTimeInZone_3') AS FLOAT64) AS power_zone_3_seconds,
    SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.powerTimeInZone_4') AS FLOAT64) AS power_zone_4_seconds,
    SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.powerTimeInZone_5') AS FLOAT64) AS power_zone_5_seconds,
    
    -- Running-specific metrics
    SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.averageRunningCadenceInStepsPerMinute') AS FLOAT64) AS average_cadence_spm,
    SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.maxRunningCadenceInStepsPerMinute') AS FLOAT64) AS max_cadence_spm,
    SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.avgVerticalOscillation') AS FLOAT64) AS avg_vertical_oscillation,
    SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.avgGroundContactTime') AS FLOAT64) AS avg_ground_contact_time,
    SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.avgStrideLength') AS FLOAT64) AS avg_stride_length,
    SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.avgVerticalRatio') AS FLOAT64) AS avg_vertical_ratio,
    SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.steps') AS INT64) AS total_steps,
    
    -- Training metrics
    SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.aerobicTrainingEffect') AS FLOAT64) AS aerobic_training_effect,
    SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.anaerobicTrainingEffect') AS FLOAT64) AS anaerobic_training_effect,
    SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.vO2MaxValue') AS FLOAT64) AS vo2_max,
    SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.activityTrainingLoad') AS FLOAT64) AS training_load,
    JSON_EXTRACT_SCALAR(raw_data, '$.trainingEffectLabel') AS training_effect_label,
    
    -- Environmental data
    SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.minTemperature') AS FLOAT64) AS min_temperature_celsius,
    SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.maxTemperature') AS FLOAT64) AS max_temperature_celsius,
    
    -- Device and metadata
    JSON_EXTRACT_SCALAR(raw_data, '$.deviceId') AS device_id,
    JSON_EXTRACT_SCALAR(raw_data, '$.manufacturer') AS manufacturer,
    SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.lapCount') AS INT64) AS lap_count,
    SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.differenceBodyBattery') AS INT64) AS body_battery_impact,
    
    -- Fast split times (useful for personal records)
    SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.fastestSplit_1000') AS FLOAT64) AS fastest_1km_seconds,
    SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.fastestSplit_1609') AS FLOAT64) AS fastest_mile_seconds,
    SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.fastestSplit_5000') AS FLOAT64) AS fastest_5km_seconds,
    
    -- Activity intensity
    SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.moderateIntensityMinutes') AS INT64) AS moderate_intensity_minutes,
    SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.vigorousIntensityMinutes') AS INT64) AS vigorous_intensity_minutes,
    
    -- Complex nested data preserved as JSON for hub layer processing
    JSON_EXTRACT(raw_data, '$.splitSummaries') AS split_summaries_json,
    JSON_EXTRACT(raw_data, '$.activityType') AS activity_type_json,
    JSON_EXTRACT(raw_data, '$.eventType') AS event_type_json,
    
    -- Source metadata
    dp_inserted_at,
    source_file

FROM {{ source('garmin', 'staging_garmin_raw') }}
WHERE data_type = 'activities'
  AND JSON_EXTRACT_SCALAR(raw_data, '$.activityId') IS NOT NULL