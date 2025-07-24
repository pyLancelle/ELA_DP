{{ config(dataset=get_schema('hub'), materialized='view', tags=["hub", "garmin"]) }}

-- Hub model for Garmin sleep data with structured objects
-- Uses STRUCT to preserve logical groupings instead of flattening everything

SELECT
    -- Core identifiers and basic fields (kept flat for easy filtering/joining)
    CAST(JSON_VALUE(raw_data, '$.dailySleepDTO.id') AS INT64) as sleep_id,
    CAST(JSON_VALUE(raw_data, '$.dailySleepDTO.userProfilePK') AS INT64) as user_profile_pk,
    DATE(JSON_VALUE(raw_data, '$.dailySleepDTO.calendarDate')) as sleep_date,
    CAST(JSON_VALUE(raw_data, '$.dailySleepDTO.sleepTimeSeconds') AS INT64) as total_sleep_seconds,
    CAST(JSON_VALUE(raw_data, '$.dailySleepDTO.napTimeSeconds') AS INT64) as nap_time_seconds,
    
    -- Sleep window and timing info
    STRUCT(
        CAST(JSON_VALUE(raw_data, '$.dailySleepDTO.sleepWindowConfirmed') AS BOOL) as confirmed,
        JSON_VALUE(raw_data, '$.dailySleepDTO.sleepWindowConfirmationType') as confirmation_type,
        TIMESTAMP_MILLIS(CAST(JSON_VALUE(raw_data, '$.dailySleepDTO.sleepStartTimestampGMT') AS INT64)) as start_gmt,
        TIMESTAMP_MILLIS(CAST(JSON_VALUE(raw_data, '$.dailySleepDTO.sleepEndTimestampGMT') AS INT64)) as end_gmt,
        TIMESTAMP_MILLIS(CAST(JSON_VALUE(raw_data, '$.dailySleepDTO.sleepStartTimestampLocal') AS INT64)) as start_local,
        TIMESTAMP_MILLIS(CAST(JSON_VALUE(raw_data, '$.dailySleepDTO.sleepEndTimestampLocal') AS INT64)) as end_local,
        TIMESTAMP_MILLIS(CAST(JSON_VALUE(raw_data, '$.dailySleepDTO.autoSleepStartTimestampGMT') AS INT64)) as auto_start_gmt,
        TIMESTAMP_MILLIS(CAST(JSON_VALUE(raw_data, '$.dailySleepDTO.autoSleepEndTimestampGMT') AS INT64)) as auto_end_gmt
    ) as sleep_window,
    
    -- Sleep phases (flat for easy analysis)
    CAST(JSON_VALUE(raw_data, '$.dailySleepDTO.unmeasurableSleepSeconds') AS INT64) as unmeasurable_sleep_seconds,
    CAST(JSON_VALUE(raw_data, '$.dailySleepDTO.deepSleepSeconds') AS INT64) as deep_sleep_seconds,
    CAST(JSON_VALUE(raw_data, '$.dailySleepDTO.lightSleepSeconds') AS INT64) as light_sleep_seconds,
    CAST(JSON_VALUE(raw_data, '$.dailySleepDTO.remSleepSeconds') AS INT64) as rem_sleep_seconds,
    CAST(JSON_VALUE(raw_data, '$.dailySleepDTO.awakeSleepSeconds') AS INT64) as awake_sleep_seconds,
    
    -- Device and metadata flags
    STRUCT(
        CAST(JSON_VALUE(raw_data, '$.dailySleepDTO.deviceRemCapable') AS BOOL) as rem_capable,
        CAST(JSON_VALUE(raw_data, '$.dailySleepDTO.retro') AS BOOL) as is_retro,
        CAST(JSON_VALUE(raw_data, '$.dailySleepDTO.sleepFromDevice') AS BOOL) as from_device,
        CAST(JSON_VALUE(raw_data, '$.dailySleepDTO.sleepVersion') AS INT64) as version
    ) as device_info,
    
    -- Health metrics
    STRUCT(
        CAST(JSON_VALUE(raw_data, '$.dailySleepDTO.averageSpO2Value') AS FLOAT64) as avg_spo2,
        CAST(JSON_VALUE(raw_data, '$.dailySleepDTO.lowestSpO2Value') AS INT64) as lowest_spo2,
        CAST(JSON_VALUE(raw_data, '$.dailySleepDTO.highestSpO2Value') AS INT64) as highest_spo2,
        CAST(JSON_VALUE(raw_data, '$.dailySleepDTO.averageSpO2HRSleep') AS FLOAT64) as avg_spo2_hr_sleep,
        CAST(JSON_VALUE(raw_data, '$.dailySleepDTO.averageRespirationValue') AS FLOAT64) as avg_respiration,
        CAST(JSON_VALUE(raw_data, '$.dailySleepDTO.lowestRespirationValue') AS FLOAT64) as lowest_respiration,
        CAST(JSON_VALUE(raw_data, '$.dailySleepDTO.highestRespirationValue') AS FLOAT64) as highest_respiration,
        CAST(JSON_VALUE(raw_data, '$.dailySleepDTO.awakeCount') AS INT64) as awake_count,
        CAST(JSON_VALUE(raw_data, '$.dailySleepDTO.avgSleepStress') AS FLOAT64) as avg_stress
    ) as health_metrics,
    
    -- Sleep scores as structured object
    STRUCT(
        STRUCT(
            CAST(JSON_VALUE(raw_data, '$.dailySleepDTO.sleepScores.overall.value') AS INT64) as value,
            JSON_VALUE(raw_data, '$.dailySleepDTO.sleepScores.overall.qualifierKey') as qualifier
        ) as overall,
        
        STRUCT(
            JSON_VALUE(raw_data, '$.dailySleepDTO.sleepScores.totalDuration.qualifierKey') as qualifier,
            CAST(JSON_VALUE(raw_data, '$.dailySleepDTO.sleepScores.totalDuration.optimalStart') AS FLOAT64) as optimal_start,
            CAST(JSON_VALUE(raw_data, '$.dailySleepDTO.sleepScores.totalDuration.optimalEnd') AS FLOAT64) as optimal_end
        ) as total_duration,
        
        STRUCT(
            JSON_VALUE(raw_data, '$.dailySleepDTO.sleepScores.stress.qualifierKey') as qualifier,
            CAST(JSON_VALUE(raw_data, '$.dailySleepDTO.sleepScores.stress.optimalStart') AS FLOAT64) as optimal_start,
            CAST(JSON_VALUE(raw_data, '$.dailySleepDTO.sleepScores.stress.optimalEnd') AS FLOAT64) as optimal_end
        ) as stress,
        
        STRUCT(
            JSON_VALUE(raw_data, '$.dailySleepDTO.sleepScores.awakeCount.qualifierKey') as qualifier,
            CAST(JSON_VALUE(raw_data, '$.dailySleepDTO.sleepScores.awakeCount.optimalStart') AS FLOAT64) as optimal_start,
            CAST(JSON_VALUE(raw_data, '$.dailySleepDTO.sleepScores.awakeCount.optimalEnd') AS FLOAT64) as optimal_end
        ) as awake_count,
        
        STRUCT(
            CAST(JSON_VALUE(raw_data, '$.dailySleepDTO.sleepScores.remPercentage.value') AS INT64) as value,
            JSON_VALUE(raw_data, '$.dailySleepDTO.sleepScores.remPercentage.qualifierKey') as qualifier,
            CAST(JSON_VALUE(raw_data, '$.dailySleepDTO.sleepScores.remPercentage.optimalStart') AS FLOAT64) as optimal_start,
            CAST(JSON_VALUE(raw_data, '$.dailySleepDTO.sleepScores.remPercentage.optimalEnd') AS FLOAT64) as optimal_end,
            CAST(JSON_VALUE(raw_data, '$.dailySleepDTO.sleepScores.remPercentage.idealStartInSeconds') AS FLOAT64) as ideal_start_seconds,
            CAST(JSON_VALUE(raw_data, '$.dailySleepDTO.sleepScores.remPercentage.idealEndInSeconds') AS FLOAT64) as ideal_end_seconds
        ) as rem_percentage,
        
        STRUCT(
            JSON_VALUE(raw_data, '$.dailySleepDTO.sleepScores.restlessness.qualifierKey') as qualifier,
            CAST(JSON_VALUE(raw_data, '$.dailySleepDTO.sleepScores.restlessness.optimalStart') AS FLOAT64) as optimal_start,
            CAST(JSON_VALUE(raw_data, '$.dailySleepDTO.sleepScores.restlessness.optimalEnd') AS FLOAT64) as optimal_end
        ) as restlessness,
        
        STRUCT(
            CAST(JSON_VALUE(raw_data, '$.dailySleepDTO.sleepScores.lightPercentage.value') AS INT64) as value,
            JSON_VALUE(raw_data, '$.dailySleepDTO.sleepScores.lightPercentage.qualifierKey') as qualifier,
            CAST(JSON_VALUE(raw_data, '$.dailySleepDTO.sleepScores.lightPercentage.optimalStart') AS FLOAT64) as optimal_start,
            CAST(JSON_VALUE(raw_data, '$.dailySleepDTO.sleepScores.lightPercentage.optimalEnd') AS FLOAT64) as optimal_end,
            CAST(JSON_VALUE(raw_data, '$.dailySleepDTO.sleepScores.lightPercentage.idealStartInSeconds') AS FLOAT64) as ideal_start_seconds,
            CAST(JSON_VALUE(raw_data, '$.dailySleepDTO.sleepScores.lightPercentage.idealEndInSeconds') AS FLOAT64) as ideal_end_seconds
        ) as light_percentage,
        
        STRUCT(
            CAST(JSON_VALUE(raw_data, '$.dailySleepDTO.sleepScores.deepPercentage.value') AS INT64) as value,
            JSON_VALUE(raw_data, '$.dailySleepDTO.sleepScores.deepPercentage.qualifierKey') as qualifier,
            CAST(JSON_VALUE(raw_data, '$.dailySleepDTO.sleepScores.deepPercentage.optimalStart') AS FLOAT64) as optimal_start,
            CAST(JSON_VALUE(raw_data, '$.dailySleepDTO.sleepScores.deepPercentage.optimalEnd') AS FLOAT64) as optimal_end,
            CAST(JSON_VALUE(raw_data, '$.dailySleepDTO.sleepScores.deepPercentage.idealStartInSeconds') AS FLOAT64) as ideal_start_seconds,
            CAST(JSON_VALUE(raw_data, '$.dailySleepDTO.sleepScores.deepPercentage.idealEndInSeconds') AS FLOAT64) as ideal_end_seconds
        ) as deep_percentage
    ) as sleep_scores,
    
    -- Sleep insights and feedback
    STRUCT(
        JSON_VALUE(raw_data, '$.dailySleepDTO.ageGroup') as age_group,
        JSON_VALUE(raw_data, '$.dailySleepDTO.sleepScoreFeedback') as score_feedback,
        JSON_VALUE(raw_data, '$.dailySleepDTO.sleepScoreInsight') as score_insight,
        JSON_VALUE(raw_data, '$.dailySleepDTO.sleepScorePersonalizedInsight') as personalized_insight
    ) as insights,
    
    -- Sleep need information
    STRUCT(
        CAST(JSON_VALUE(raw_data, '$.dailySleepDTO.sleepNeed.userProfilePk') AS INT64) as user_profile_pk,
        DATE(JSON_VALUE(raw_data, '$.dailySleepDTO.sleepNeed.calendarDate')) as calendar_date,
        CAST(JSON_VALUE(raw_data, '$.dailySleepDTO.sleepNeed.deviceId') AS INT64) as device_id,
        TIMESTAMP(JSON_VALUE(raw_data, '$.dailySleepDTO.sleepNeed.timestampGmt')) as timestamp_gmt,
        CAST(JSON_VALUE(raw_data, '$.dailySleepDTO.sleepNeed.baseline') AS INT64) as baseline,
        CAST(JSON_VALUE(raw_data, '$.dailySleepDTO.sleepNeed.actual') AS INT64) as actual,
        JSON_VALUE(raw_data, '$.dailySleepDTO.sleepNeed.feedback') as feedback,
        JSON_VALUE(raw_data, '$.dailySleepDTO.sleepNeed.trainingFeedback') as training_feedback,
        JSON_VALUE(raw_data, '$.dailySleepDTO.sleepNeed.sleepHistoryAdjustment') as sleep_history_adjustment,
        JSON_VALUE(raw_data, '$.dailySleepDTO.sleepNeed.hrvAdjustment') as hrv_adjustment,
        JSON_VALUE(raw_data, '$.dailySleepDTO.sleepNeed.napAdjustment') as nap_adjustment,
        CAST(JSON_VALUE(raw_data, '$.dailySleepDTO.sleepNeed.displayedForTheDay') AS BOOL) as displayed_for_day,
        CAST(JSON_VALUE(raw_data, '$.dailySleepDTO.sleepNeed.preferredActivityTracker') AS BOOL) as preferred_activity_tracker
    ) as sleep_need,
    
    -- Next sleep need information
    STRUCT(
        CAST(JSON_VALUE(raw_data, '$.dailySleepDTO.nextSleepNeed.userProfilePk') AS INT64) as user_profile_pk,
        DATE(JSON_VALUE(raw_data, '$.dailySleepDTO.nextSleepNeed.calendarDate')) as calendar_date,
        CAST(JSON_VALUE(raw_data, '$.dailySleepDTO.nextSleepNeed.deviceId') AS INT64) as device_id,
        TIMESTAMP(JSON_VALUE(raw_data, '$.dailySleepDTO.nextSleepNeed.timestampGmt')) as timestamp_gmt,
        CAST(JSON_VALUE(raw_data, '$.dailySleepDTO.nextSleepNeed.baseline') AS INT64) as baseline,
        CAST(JSON_VALUE(raw_data, '$.dailySleepDTO.nextSleepNeed.actual') AS INT64) as actual,
        JSON_VALUE(raw_data, '$.dailySleepDTO.nextSleepNeed.feedback') as feedback,
        JSON_VALUE(raw_data, '$.dailySleepDTO.nextSleepNeed.trainingFeedback') as training_feedback,
        JSON_VALUE(raw_data, '$.dailySleepDTO.nextSleepNeed.sleepHistoryAdjustment') as sleep_history_adjustment,
        JSON_VALUE(raw_data, '$.dailySleepDTO.nextSleepNeed.hrvAdjustment') as hrv_adjustment,
        JSON_VALUE(raw_data, '$.dailySleepDTO.nextSleepNeed.napAdjustment') as nap_adjustment,
        CAST(JSON_VALUE(raw_data, '$.dailySleepDTO.nextSleepNeed.displayedForTheDay') AS BOOL) as displayed_for_day,
        CAST(JSON_VALUE(raw_data, '$.dailySleepDTO.nextSleepNeed.preferredActivityTracker') AS BOOL) as preferred_activity_tracker
    ) as next_sleep_need,
    
    -- Additional root-level fields
    JSON_VALUE(raw_data, '$.dailySleepDTO.breathingDisruptionSeverity') as breathing_disruption_severity,
    JSON_VALUE(raw_data, '$.remSleepData') as rem_sleep_data,
    CAST(JSON_VALUE(raw_data, '$.restlessMomentsCount') AS INT64) as restless_moments_count,
    CAST(JSON_VALUE(raw_data, '$.respirationVersion') AS INT64) as respiration_version,
    CAST(JSON_VALUE(raw_data, '$.skinTempDataExists') AS BOOL) as skin_temp_data_exists,
    CAST(JSON_VALUE(raw_data, '$.avgOvernightHrv') AS FLOAT64) as avg_overnight_hrv,
    JSON_VALUE(raw_data, '$.hrvStatus') as hrv_status,
    CAST(JSON_VALUE(raw_data, '$.bodyBatteryChange') AS INT64) as body_battery_change,
    CAST(JSON_VALUE(raw_data, '$.restingHeartRate') AS INT64) as resting_heart_rate,
    DATE(JSON_VALUE(raw_data, '$.date')) as data_date,
    
    
    -- Metadata
    dp_inserted_at,
    source_file
    
FROM {{ ref('lake_garmin__svc_sleep') }}