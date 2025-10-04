{{ config(
    materialized='incremental',
    unique_key='score_date',
    partition_by={'field': 'score_date', 'data_type': 'date'},
    cluster_by=['score_date'],
    dataset=get_schema('hub'),
    tags=["hub", "garmin"]
) }}

-- Hub model for Garmin endurance score data - comprehensive field extraction
-- Extracts all possible fields from the JSON response

SELECT
    -- Primary date fields
    DATE(JSON_VALUE(raw_data, '$.date')) as score_date,
    DATE(JSON_VALUE(raw_data, '$.calendarDate')) as calendar_date,
    DATE(JSON_VALUE(raw_data, '$.fromCalendarDate')) as from_calendar_date,
    DATE(JSON_VALUE(raw_data, '$.untilCalendarDate')) as until_calendar_date,
    
    -- Core endurance score metrics
    CAST(JSON_VALUE(raw_data, '$.enduranceScore') AS FLOAT64) as endurance_score,
    CAST(JSON_VALUE(raw_data, '$.enduranceScoreChange') AS FLOAT64) as endurance_score_change,
    JSON_VALUE(raw_data, '$.enduranceScoreCategory') as endurance_score_category,
    CAST(JSON_VALUE(raw_data, '$.enduranceScoreValue') AS FLOAT64) as endurance_score_value,
    
    -- Endurance gain/loss metrics
    CAST(JSON_VALUE(raw_data, '$.enduranceGain') AS FLOAT64) as endurance_gain,
    CAST(JSON_VALUE(raw_data, '$.enduranceLoss') AS FLOAT64) as endurance_loss,
    CAST(JSON_VALUE(raw_data, '$.netEnduranceChange') AS FLOAT64) as net_endurance_change,
    
    -- Historical comparisons
    CAST(JSON_VALUE(raw_data, '$.lastWeekEnduranceScore') AS FLOAT64) as last_week_endurance_score,
    CAST(JSON_VALUE(raw_data, '$.lastMonthEnduranceScore') AS FLOAT64) as last_month_endurance_score,
    CAST(JSON_VALUE(raw_data, '$.targetEnduranceScore') AS FLOAT64) as target_endurance_score,
    CAST(JSON_VALUE(raw_data, '$.baselineEnduranceScore') AS FLOAT64) as baseline_endurance_score,
    
    -- Training load and fitness metrics
    CAST(JSON_VALUE(raw_data, '$.trainingLoad') AS FLOAT64) as training_load,
    CAST(JSON_VALUE(raw_data, '$.peakTrainingLoad') AS FLOAT64) as peak_training_load,
    CAST(JSON_VALUE(raw_data, '$.lowAerobicTrainingLoad') AS FLOAT64) as low_aerobic_training_load,
    CAST(JSON_VALUE(raw_data, '$.highAerobicTrainingLoad') AS FLOAT64) as high_aerobic_training_load,
    CAST(JSON_VALUE(raw_data, '$.anaerobicTrainingLoad') AS FLOAT64) as anaerobic_training_load,
    
    -- Endurance trend and status
    JSON_VALUE(raw_data, '$.enduranceTrend') as endurance_trend,
    JSON_VALUE(raw_data, '$.enduranceStatus') as endurance_status,
    CAST(JSON_VALUE(raw_data, '$.enduranceTrendValue') AS FLOAT64) as endurance_trend_value,
    
    -- VO2 Max related fields
    CAST(JSON_VALUE(raw_data, '$.vo2Max') AS FLOAT64) as vo2_max,
    CAST(JSON_VALUE(raw_data, '$.vo2MaxRunning') AS FLOAT64) as vo2_max_running,
    CAST(JSON_VALUE(raw_data, '$.vo2MaxCycling') AS FLOAT64) as vo2_max_cycling,
    
    -- Activity and performance metrics
    CAST(JSON_VALUE(raw_data, '$.totalActivities') AS INT64) as total_activities,
    CAST(JSON_VALUE(raw_data, '$.totalDistance') AS FLOAT64) as total_distance_meters,
    CAST(JSON_VALUE(raw_data, '$.totalDuration') AS INT64) as total_duration_seconds,
    CAST(JSON_VALUE(raw_data, '$.averageIntensity') AS FLOAT64) as average_intensity,
    
    -- Weekly and monthly aggregations
    CAST(JSON_VALUE(raw_data, '$.weeklyLoad') AS FLOAT64) as weekly_load,
    CAST(JSON_VALUE(raw_data, '$.monthlyLoad') AS FLOAT64) as monthly_load,
    CAST(JSON_VALUE(raw_data, '$.weeklyEnduranceChange') AS FLOAT64) as weekly_endurance_change,
    CAST(JSON_VALUE(raw_data, '$.monthlyEnduranceChange') AS FLOAT64) as monthly_endurance_change,
    
    -- Fitness age and performance indicators
    CAST(JSON_VALUE(raw_data, '$.fitnessAge') AS INT64) as fitness_age,
    CAST(JSON_VALUE(raw_data, '$.performanceCondition') AS FLOAT64) as performance_condition,
    JSON_VALUE(raw_data, '$.performanceConditionLabel') as performance_condition_label,
    
    -- Recovery and readiness metrics
    CAST(JSON_VALUE(raw_data, '$.recoveryTime') AS INT64) as recovery_time_hours,
    CAST(JSON_VALUE(raw_data, '$.trainingReadiness') AS FLOAT64) as training_readiness,
    JSON_VALUE(raw_data, '$.trainingReadinessLevel') as training_readiness_level,
    
    -- Additional Garmin-specific fields
    CAST(JSON_VALUE(raw_data, '$.lactateThresholdHeartRate') AS INT64) as lactate_threshold_heart_rate,
    CAST(JSON_VALUE(raw_data, '$.lactateThresholdPace') AS FLOAT64) as lactate_threshold_pace,
    CAST(JSON_VALUE(raw_data, '$.functionalThresholdPower') AS INT64) as functional_threshold_power,
    
    -- Time zone and locale
    JSON_VALUE(raw_data, '$.timeZone') as time_zone,
    JSON_VALUE(raw_data, '$.locale') as locale,
    
    -- Version and sync information
    JSON_VALUE(raw_data, '$.version') as api_version,
    CAST(JSON_VALUE(raw_data, '$.lastSyncTime') AS INT64) as last_sync_time_millis,
    TIMESTAMP_MILLIS(CAST(JSON_VALUE(raw_data, '$.lastSyncTime') AS INT64)) as last_sync_time_utc,
    
    -- Technical fields
    CAST(JSON_VALUE(raw_data, '$.timestamp') AS INT64) as timestamp_millis,
    TIMESTAMP_MILLIS(CAST(JSON_VALUE(raw_data, '$.timestamp') AS INT64)) as timestamp_utc,
    CAST(JSON_VALUE(raw_data, '$.startTimeGMT') AS INT64) as start_time_gmt_millis,
    TIMESTAMP_MILLIS(CAST(JSON_VALUE(raw_data, '$.startTimeGMT') AS INT64)) as start_time_gmt_utc,
    CAST(JSON_VALUE(raw_data, '$.userProfilePK') AS INT64) as user_profile_pk,
    CAST(JSON_VALUE(raw_data, '$.userId') AS INT64) as user_id,
    
    -- Device and measurement context
    JSON_VALUE(raw_data, '$.deviceId') as device_id,
    JSON_VALUE(raw_data, '$.measurementType') as measurement_type,
    JSON_VALUE(raw_data, '$.dataSource') as data_source,
    JSON_VALUE(raw_data, '$.calculationMethod') as calculation_method,
    
    -- Additional metadata that might be present
    CAST(JSON_VALUE(raw_data, '$.id') AS INT64) as record_id,
    JSON_VALUE(raw_data, '$.activityType') as activity_type,
    JSON_VALUE(raw_data, '$.sport') as sport,
    CAST(JSON_VALUE(raw_data, '$.duration') AS INT64) as duration_seconds,
    CAST(JSON_VALUE(raw_data, '$.distance') AS FLOAT64) as distance_meters,
    
    -- Confidence and quality indicators
    CAST(JSON_VALUE(raw_data, '$.confidence') AS FLOAT64) as confidence_score,
    JSON_VALUE(raw_data, '$.qualityRating') as quality_rating,
    CAST(JSON_VALUE(raw_data, '$.dataQuality') AS FLOAT64) as data_quality_score,
    
    -- Raw data for debugging and future fields
    raw_data,
    
    -- Metadata
    dp_inserted_at,
    source_file

FROM {{ ref('lake_garmin__svc_endurance_score') }}

{% if is_incremental() %}
WHERE dp_inserted_at > (SELECT MAX(dp_inserted_at) FROM {{ this }})
{% endif %}