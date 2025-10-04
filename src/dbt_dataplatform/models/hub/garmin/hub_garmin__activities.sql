{{ config(
    materialized='incremental',
    unique_key='activity_id',
    partition_by={'field': 'activity_date', 'data_type': 'date'},
    cluster_by=['activity_id'],
    dataset=get_schema('hub'),
    tags=["hub", "garmin"]
) }}

-- Hub model for Garmin activities data with structured objects
-- Uses STRUCT to preserve logical groupings for efficient analysis

SELECT
    -- Core identifiers and basic info (kept flat for easy filtering/joining)
    CAST(JSON_VALUE(raw_data, '$.activityId') AS INT64) as activity_id,
    JSON_VALUE(raw_data, '$.activityName') as activity_name,
    DATE(TIMESTAMP(JSON_VALUE(raw_data, '$.startTimeGMT'))) as activity_date,
    TIMESTAMP(JSON_VALUE(raw_data, '$.startTimeLocal')) as start_time_local,
    TIMESTAMP(JSON_VALUE(raw_data, '$.startTimeGMT')) as start_time_gmt,
    TIMESTAMP(JSON_VALUE(raw_data, '$.endTimeGMT')) as end_time_gmt,
    TIMESTAMP_MILLIS(CAST(JSON_VALUE(raw_data, '$.beginTimestamp') AS INT64)) as begin_timestamp,
    
    -- Activity type information
    STRUCT(
        CAST(JSON_VALUE(raw_data, '$.activityType.typeId') AS INT64) as type_id,
        JSON_VALUE(raw_data, '$.activityType.typeKey') as type_key,
        CAST(JSON_VALUE(raw_data, '$.activityType.parentTypeId') AS INT64) as parent_type_id,
        CAST(JSON_VALUE(raw_data, '$.activityType.isHidden') AS BOOL) as is_hidden,
        CAST(JSON_VALUE(raw_data, '$.activityType.restricted') AS BOOL) as restricted,
        CAST(JSON_VALUE(raw_data, '$.activityType.trimmable') AS BOOL) as trimmable
    ) as activity_type,
    
    -- Event type information
    STRUCT(
        SAFE_CAST(JSON_VALUE(raw_data, '$.eventType.typeId') AS INT64) as type_id,
        JSON_VALUE(raw_data, '$.eventType.typeKey') as type_key,
        SAFE_CAST(JSON_VALUE(raw_data, '$.eventType.sortOrder') AS INT64) as sort_order
    ) as event_type,
    
    -- Distance and duration metrics (flat for easy analysis)
    CAST(JSON_VALUE(raw_data, '$.distance') AS FLOAT64) as distance_meters,
    CAST(JSON_VALUE(raw_data, '$.duration') AS FLOAT64) as duration_seconds,
    CAST(JSON_VALUE(raw_data, '$.elapsedDuration') AS FLOAT64) as elapsed_duration_seconds,
    CAST(JSON_VALUE(raw_data, '$.movingDuration') AS FLOAT64) as moving_duration_seconds,
    
    -- Elevation metrics
    STRUCT(
        CAST(JSON_VALUE(raw_data, '$.elevationGain') AS FLOAT64) as gain_meters,
        CAST(JSON_VALUE(raw_data, '$.elevationLoss') AS FLOAT64) as loss_meters,
        CAST(JSON_VALUE(raw_data, '$.minElevation') AS FLOAT64) as min_meters,
        CAST(JSON_VALUE(raw_data, '$.maxElevation') AS FLOAT64) as max_meters
    ) as elevation,
    
    -- Speed metrics
    STRUCT(
        CAST(JSON_VALUE(raw_data, '$.averageSpeed') AS FLOAT64) as average_mps,
        CAST(JSON_VALUE(raw_data, '$.maxSpeed') AS FLOAT64) as max_mps,
        CAST(JSON_VALUE(raw_data, '$.avgGradeAdjustedSpeed') AS FLOAT64) as avg_grade_adjusted_mps
    ) as speed,
    
    -- Location information
    STRUCT(
        CAST(JSON_VALUE(raw_data, '$.startLatitude') AS FLOAT64) as start_latitude,
        CAST(JSON_VALUE(raw_data, '$.startLongitude') AS FLOAT64) as start_longitude,
        CAST(JSON_VALUE(raw_data, '$.endLatitude') AS FLOAT64) as end_latitude,
        CAST(JSON_VALUE(raw_data, '$.endLongitude') AS FLOAT64) as end_longitude,
        JSON_VALUE(raw_data, '$.locationName') as location_name
    ) as location,
    
    -- Owner information
    STRUCT(
        CAST(JSON_VALUE(raw_data, '$.ownerId') AS INT64) as owner_id,
        JSON_VALUE(raw_data, '$.ownerDisplayName') as display_name,
        JSON_VALUE(raw_data, '$.ownerFullName') as full_name,
        JSON_VALUE(raw_data, '$.ownerProfileImageUrlSmall') as profile_image_small,
        JSON_VALUE(raw_data, '$.ownerProfileImageUrlMedium') as profile_image_medium,
        JSON_VALUE(raw_data, '$.ownerProfileImageUrlLarge') as profile_image_large
    ) as owner,
    
    -- Calories and energy
    STRUCT(
        CAST(JSON_VALUE(raw_data, '$.calories') AS FLOAT64) as total_calories,
        CAST(JSON_VALUE(raw_data, '$.bmrCalories') AS FLOAT64) as bmr_calories
    ) as calories,
    
    -- Heart rate metrics
    STRUCT(
        CAST(JSON_VALUE(raw_data, '$.averageHR') AS INT64) as average_bpm,
        CAST(JSON_VALUE(raw_data, '$.maxHR') AS INT64) as max_bpm,
        CAST(JSON_VALUE(raw_data, '$.hrTimeInZone_1') AS FLOAT64) as time_in_zone_1_seconds,
        CAST(JSON_VALUE(raw_data, '$.hrTimeInZone_2') AS FLOAT64) as time_in_zone_2_seconds,
        CAST(JSON_VALUE(raw_data, '$.hrTimeInZone_3') AS FLOAT64) as time_in_zone_3_seconds,
        CAST(JSON_VALUE(raw_data, '$.hrTimeInZone_4') AS FLOAT64) as time_in_zone_4_seconds,
        CAST(JSON_VALUE(raw_data, '$.hrTimeInZone_5') AS FLOAT64) as time_in_zone_5_seconds
    ) as heart_rate,
    
    -- Power metrics (for cycling/running with power)
    STRUCT(
        CAST(JSON_VALUE(raw_data, '$.avgPower') AS FLOAT64) as average_watts,
        CAST(JSON_VALUE(raw_data, '$.maxPower') AS FLOAT64) as max_watts,
        CAST(JSON_VALUE(raw_data, '$.normPower') AS FLOAT64) as normalized_watts,
        CAST(JSON_VALUE(raw_data, '$.powerTimeInZone_1') AS FLOAT64) as time_in_zone_1_seconds,
        CAST(JSON_VALUE(raw_data, '$.powerTimeInZone_2') AS FLOAT64) as time_in_zone_2_seconds,
        CAST(JSON_VALUE(raw_data, '$.powerTimeInZone_3') AS FLOAT64) as time_in_zone_3_seconds,
        CAST(JSON_VALUE(raw_data, '$.powerTimeInZone_4') AS FLOAT64) as time_in_zone_4_seconds,
        CAST(JSON_VALUE(raw_data, '$.powerTimeInZone_5') AS FLOAT64) as time_in_zone_5_seconds
    ) as power,
    
    -- Running-specific metrics
    STRUCT(
        CAST(JSON_VALUE(raw_data, '$.averageRunningCadenceInStepsPerMinute') AS FLOAT64) as avg_cadence_spm,
        CAST(JSON_VALUE(raw_data, '$.maxRunningCadenceInStepsPerMinute') AS FLOAT64) as max_cadence_spm,
        CAST(JSON_VALUE(raw_data, '$.maxDoubleCadence') AS FLOAT64) as max_double_cadence,
        CAST(JSON_VALUE(raw_data, '$.steps') AS INT64) as total_steps,
        CAST(JSON_VALUE(raw_data, '$.avgVerticalOscillation') AS FLOAT64) as avg_vertical_oscillation_cm,
        CAST(JSON_VALUE(raw_data, '$.avgGroundContactTime') AS FLOAT64) as avg_ground_contact_time_ms,
        CAST(JSON_VALUE(raw_data, '$.avgStrideLength') AS FLOAT64) as avg_stride_length_cm,
        CAST(JSON_VALUE(raw_data, '$.avgVerticalRatio') AS FLOAT64) as avg_vertical_ratio_percent
    ) as running_metrics,
    
    -- Training effect and fitness
    STRUCT(
        CAST(JSON_VALUE(raw_data, '$.aerobicTrainingEffect') AS FLOAT64) as aerobic_effect,
        CAST(JSON_VALUE(raw_data, '$.anaerobicTrainingEffect') AS FLOAT64) as anaerobic_effect,
        JSON_VALUE(raw_data, '$.aerobicTrainingEffectMessage') as aerobic_effect_message,
        JSON_VALUE(raw_data, '$.anaerobicTrainingEffectMessage') as anaerobic_effect_message,
        JSON_VALUE(raw_data, '$.trainingEffectLabel') as training_effect_label,
        CAST(JSON_VALUE(raw_data, '$.activityTrainingLoad') AS FLOAT64) as training_load,
        CAST(JSON_VALUE(raw_data, '$.vO2MaxValue') AS FLOAT64) as vo2_max
    ) as training_effect,
    
    -- Temperature and environment
    STRUCT(
        CAST(JSON_VALUE(raw_data, '$.minTemperature') AS FLOAT64) as min_celsius,
        CAST(JSON_VALUE(raw_data, '$.maxTemperature') AS FLOAT64) as max_celsius
    ) as temperature,
    
    -- Activity features and flags
    STRUCT(
        CAST(JSON_VALUE(raw_data, '$.hasPolyline') AS BOOL) as has_polyline,
        CAST(JSON_VALUE(raw_data, '$.hasImages') AS BOOL) as has_images,
        CAST(JSON_VALUE(raw_data, '$.hasVideo') AS BOOL) as has_video,
        CAST(JSON_VALUE(raw_data, '$.hasHeatMap') AS BOOL) as has_heat_map,
        CAST(JSON_VALUE(raw_data, '$.hasSplits') AS BOOL) as has_splits,
        CAST(JSON_VALUE(raw_data, '$.favorite') AS BOOL) as is_favorite,
        CAST(JSON_VALUE(raw_data, '$.purposeful') AS BOOL) as is_purposeful,
        CAST(JSON_VALUE(raw_data, '$.manualActivity') AS BOOL) as is_manual,
        CAST(JSON_VALUE(raw_data, '$.pr') AS BOOL) as is_personal_record
    ) as activity_features,
    
    -- Privacy settings
    STRUCT(
        CAST(JSON_VALUE(raw_data, '$.privacy.typeId') AS INT64) as type_id,
        JSON_VALUE(raw_data, '$.privacy.typeKey') as type_key
    ) as privacy,
    
    -- Performance splits (fastest times)
    STRUCT(
        CAST(JSON_VALUE(raw_data, '$.fastestSplit_1000') AS FLOAT64) as fastest_1000m_seconds,
        CAST(JSON_VALUE(raw_data, '$.fastestSplit_1609') AS FLOAT64) as fastest_mile_seconds,
        CAST(JSON_VALUE(raw_data, '$.fastestSplit_5000') AS FLOAT64) as fastest_5000m_seconds,
        CAST(JSON_VALUE(raw_data, '$.fastestSplit_10000') AS FLOAT64) as fastest_10000m_seconds
    ) as fastest_splits,
    
    -- Intensity minutes
    STRUCT(
        CAST(JSON_VALUE(raw_data, '$.moderateIntensityMinutes') AS INT64) as moderate_minutes,
        CAST(JSON_VALUE(raw_data, '$.vigorousIntensityMinutes') AS INT64) as vigorous_minutes
    ) as intensity_minutes,
    
    -- Device and technical info
    STRUCT(
        CAST(JSON_VALUE(raw_data, '$.deviceId') AS INT64) as device_id,
        JSON_VALUE(raw_data, '$.manufacturer') as manufacturer,
        CAST(JSON_VALUE(raw_data, '$.timeZoneId') AS INT64) as timezone_id,
        CAST(JSON_VALUE(raw_data, '$.sportTypeId') AS INT64) as sport_type_id,
        CAST(JSON_VALUE(raw_data, '$.workoutId') AS INT64) as workout_id
    ) as device_info,
    
    -- Additional metrics
    CAST(JSON_VALUE(raw_data, '$.waterEstimated') AS FLOAT64) as estimated_water_loss_ml,
    CAST(JSON_VALUE(raw_data, '$.lapCount') AS INT64) as lap_count,
    CAST(JSON_VALUE(raw_data, '$.differenceBodyBattery') AS INT64) as body_battery_change,
    CAST(JSON_VALUE(raw_data, '$.minActivityLapDuration') AS FLOAT64) as min_lap_duration_seconds,
    CAST(JSON_VALUE(raw_data, '$.maxVerticalSpeed') AS FLOAT64) as max_vertical_speed_mps,
    
    -- User roles parsed (array of strings)
    ARRAY(
        SELECT JSON_VALUE(role)
        FROM UNNEST(JSON_QUERY_ARRAY(JSON_QUERY(raw_data, '$.userRoles'))) AS role
    ) as user_roles,

    -- Split summaries parsed (segment splits with detailed metrics)
    ARRAY(
        SELECT AS STRUCT
            JSON_VALUE(TO_JSON_STRING(split), '$.splitType') as split_type,
            SAFE_CAST(JSON_VALUE(TO_JSON_STRING(split), '$.noOfSplits') AS INT64) as no_of_splits,
            SAFE_CAST(JSON_VALUE(TO_JSON_STRING(split), '$.distance') AS FLOAT64) as distance_meters,
            SAFE_CAST(JSON_VALUE(TO_JSON_STRING(split), '$.maxDistance') AS FLOAT64) as max_distance_meters,
            SAFE_CAST(JSON_VALUE(TO_JSON_STRING(split), '$.duration') AS FLOAT64) as duration_seconds,
            SAFE_CAST(JSON_VALUE(TO_JSON_STRING(split), '$.movingDuration') AS FLOAT64) as moving_duration_seconds,
            SAFE_CAST(JSON_VALUE(TO_JSON_STRING(split), '$.averageSpeed') AS FLOAT64) as average_speed_mps,
            SAFE_CAST(JSON_VALUE(TO_JSON_STRING(split), '$.maxSpeed') AS FLOAT64) as max_speed_mps,
            SAFE_CAST(JSON_VALUE(TO_JSON_STRING(split), '$.averageHR') AS INT64) as average_hr,
            SAFE_CAST(JSON_VALUE(TO_JSON_STRING(split), '$.averageRunningCadenceInStepsPerMinute') AS FLOAT64) as avg_cadence_spm,
            SAFE_CAST(JSON_VALUE(TO_JSON_STRING(split), '$.maxRunningCadenceInStepsPerMinute') AS FLOAT64) as max_cadence_spm,
            SAFE_CAST(JSON_VALUE(TO_JSON_STRING(split), '$.calories') AS FLOAT64) as calories,
            SAFE_CAST(JSON_VALUE(TO_JSON_STRING(split), '$.averageElevationGain') AS FLOAT64) as avg_elevation_gain_meters,
            SAFE_CAST(JSON_VALUE(TO_JSON_STRING(split), '$.maxElevationGain') AS FLOAT64) as max_elevation_gain_meters,
            SAFE_CAST(JSON_VALUE(TO_JSON_STRING(split), '$.elevationLoss') AS FLOAT64) as elevation_loss_meters,
            SAFE_CAST(JSON_VALUE(TO_JSON_STRING(split), '$.totalAscent') AS FLOAT64) as total_ascent_meters,
            SAFE_CAST(JSON_VALUE(TO_JSON_STRING(split), '$.numClimbSends') AS INT64) as num_climb_sends,
            SAFE_CAST(JSON_VALUE(TO_JSON_STRING(split), '$.numFalls') AS INT64) as num_falls
        FROM UNNEST(JSON_QUERY_ARRAY(JSON_QUERY(raw_data, '$.splitSummaries'))) AS split
    ) as split_summaries,

    -- Dive-specific information parsed
    STRUCT(
        ARRAY(
            SELECT AS STRUCT
                CAST(JSON_VALUE(TO_JSON_STRING(gas), '$.oxygenPercentage') AS FLOAT64) as oxygen_percentage,
                CAST(JSON_VALUE(TO_JSON_STRING(gas), '$.heliumPercentage') AS FLOAT64) as helium_percentage,
                JSON_VALUE(TO_JSON_STRING(gas), '$.gasMode') as gas_mode
            FROM UNNEST(JSON_QUERY_ARRAY(JSON_QUERY(raw_data, '$.summarizedDiveInfo.summarizedDiveGases'))) AS gas
        ) as dive_gases,
        CAST(JSON_VALUE(raw_data, '$.qualifyingDive') AS BOOL) as is_qualifying_dive,
        CAST(JSON_VALUE(raw_data, '$.decoDive') AS BOOL) as is_deco_dive
    ) as dive_info,
    
    -- Additional boolean flags
    CAST(JSON_VALUE(raw_data, '$.userPro') AS BOOL) as user_is_pro,
    CAST(JSON_VALUE(raw_data, '$.parent') AS BOOL) as is_parent_activity,
    CAST(JSON_VALUE(raw_data, '$.autoCalcCalories') AS BOOL) as auto_calc_calories,
    CAST(JSON_VALUE(raw_data, '$.elevationCorrected') AS BOOL) as elevation_corrected,
    CAST(JSON_VALUE(raw_data, '$.atpActivity') AS BOOL) as is_atp_activity,
    
    -- Metadata
    dp_inserted_at,
    source_file

FROM {{ ref('lake_garmin__svc_activities') }}

{% if is_incremental() %}
WHERE dp_inserted_at > (SELECT MAX(dp_inserted_at) FROM {{ this }})
{% endif %}