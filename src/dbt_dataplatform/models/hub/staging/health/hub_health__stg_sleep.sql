/*
Une ligne par nuit de sommeil avec les métriques essentielles et interprétations.

Structure :
- Colonnes de base : date, sleep_score, bedtime, waketime, total_sleep_hours
- Phases de sommeil (STRUCT) : deep_sleep, light_sleep, rem_sleep avec heures, %, optimaux et status
- Temps éveillé (STRUCT) : awake avec heures, %, nombre de réveils et qualité
- Body Battery (STRUCT) : at_bedtime, at_waketime, recovery et qualité de récupération
- Métriques santé : avg_hrv, avg_stress, resting_heart_rate
- Score global : sleep_quality (excellent, good, fair, poor, very_poor)
- Sleep levels (ARRAY) : données granulaires des niveaux de sommeil avec timestamps
*/

{{
  config(
      tags=['health', 'hub'],
      materialized='view'
  )
}}

SELECT
    -- Informations de base
    DATE(date) AS date,
    dailySleepDTO.sleepScores.overall.value AS sleep_score,
    DATETIME(SAFE.TIMESTAMP_MILLIS(dailySleepDTO.sleepStartTimestampLocal)) AS bedtime,
    DATETIME(SAFE.TIMESTAMP_MILLIS(dailySleepDTO.sleepEndTimestampLocal)) AS waketime,
    ROUND(dailySleepDTO.sleepTimeSeconds / 3600.0, 2) AS total_sleep_hours,

    -- Deep Sleep avec interprétation
    STRUCT(
        ROUND(dailySleepDTO.deepSleepSeconds / 3600.0, 2) AS hours,
        ROUND(100.0 * dailySleepDTO.deepSleepSeconds / NULLIF(dailySleepDTO.sleepTimeSeconds, 0), 2) AS percentage,
        STRUCT(
            ROUND(dailySleepDTO.sleepScores.deepPercentage.idealStartInSeconds / 3600.0, 1) AS hours,
            ROUND(100.0 * dailySleepDTO.deepSleepSeconds / NULLIF(dailySleepDTO.sleepTimeSeconds, 0), 2) >=
                ROUND(100.0 * dailySleepDTO.sleepScores.deepPercentage.idealStartInSeconds / NULLIF(dailySleepDTO.sleepTimeSeconds, 0), 2) AS is_sufficient
        ) AS optimal_min,
        STRUCT(
            ROUND(dailySleepDTO.sleepScores.deepPercentage.idealEndInSeconds / 3600.0, 1) AS hours,
            ROUND(100.0 * dailySleepDTO.deepSleepSeconds / NULLIF(dailySleepDTO.sleepTimeSeconds, 0), 2) <=
                ROUND(100.0 * dailySleepDTO.sleepScores.deepPercentage.idealEndInSeconds / NULLIF(dailySleepDTO.sleepTimeSeconds, 0), 2) AS is_within_range
        ) AS optimal_max,
        CASE
            WHEN ROUND(100.0 * dailySleepDTO.deepSleepSeconds / NULLIF(dailySleepDTO.sleepTimeSeconds, 0), 2) >=
                    ROUND(100.0 * dailySleepDTO.sleepScores.deepPercentage.idealStartInSeconds / NULLIF(dailySleepDTO.sleepTimeSeconds, 0), 2)
                AND ROUND(100.0 * dailySleepDTO.deepSleepSeconds / NULLIF(dailySleepDTO.sleepTimeSeconds, 0), 2) <=
                    ROUND(100.0 * dailySleepDTO.sleepScores.deepPercentage.idealEndInSeconds / NULLIF(dailySleepDTO.sleepTimeSeconds, 0), 2)
            THEN 'optimal'
            WHEN ROUND(100.0 * dailySleepDTO.deepSleepSeconds / NULLIF(dailySleepDTO.sleepTimeSeconds, 0), 2) <
                    ROUND(100.0 * dailySleepDTO.sleepScores.deepPercentage.idealStartInSeconds / NULLIF(dailySleepDTO.sleepTimeSeconds, 0), 2)
            THEN 'insufficient'
            ELSE 'excessive'
        END AS status
    ) AS deep_sleep,

    -- Light Sleep avec interprétation
    STRUCT(
        ROUND(dailySleepDTO.lightSleepSeconds / 3600.0, 2) AS hours,
        ROUND(100.0 * dailySleepDTO.lightSleepSeconds / NULLIF(dailySleepDTO.sleepTimeSeconds, 0), 2) AS percentage,
        STRUCT(
            ROUND(dailySleepDTO.sleepScores.lightPercentage.idealStartInSeconds / 3600.0, 1) AS hours,
            ROUND(100.0 * dailySleepDTO.lightSleepSeconds / NULLIF(dailySleepDTO.sleepTimeSeconds, 0), 2) >=
                ROUND(100.0 * dailySleepDTO.sleepScores.lightPercentage.idealStartInSeconds / NULLIF(dailySleepDTO.sleepTimeSeconds, 0), 2) AS is_sufficient
        ) AS optimal_min,
        STRUCT(
            ROUND(dailySleepDTO.sleepScores.lightPercentage.idealEndInSeconds / 3600.0, 1) AS hours,
            ROUND(100.0 * dailySleepDTO.lightSleepSeconds / NULLIF(dailySleepDTO.sleepTimeSeconds, 0), 2) <=
                ROUND(100.0 * dailySleepDTO.sleepScores.lightPercentage.idealEndInSeconds / NULLIF(dailySleepDTO.sleepTimeSeconds, 0), 2) AS is_within_range
        ) AS optimal_max,
        CASE
            WHEN ROUND(100.0 * dailySleepDTO.lightSleepSeconds / NULLIF(dailySleepDTO.sleepTimeSeconds, 0), 2) >=
                    ROUND(100.0 * dailySleepDTO.sleepScores.lightPercentage.idealStartInSeconds / NULLIF(dailySleepDTO.sleepTimeSeconds, 0), 2)
                AND ROUND(100.0 * dailySleepDTO.lightSleepSeconds / NULLIF(dailySleepDTO.sleepTimeSeconds, 0), 2) <=
                    ROUND(100.0 * dailySleepDTO.sleepScores.lightPercentage.idealEndInSeconds / NULLIF(dailySleepDTO.sleepTimeSeconds, 0), 2)
            THEN 'optimal'
            WHEN ROUND(100.0 * dailySleepDTO.lightSleepSeconds / NULLIF(dailySleepDTO.sleepTimeSeconds, 0), 2) <
                    ROUND(100.0 * dailySleepDTO.sleepScores.lightPercentage.idealStartInSeconds / NULLIF(dailySleepDTO.sleepTimeSeconds, 0), 2)
            THEN 'insufficient'
            ELSE 'excessive'
        END AS status
    ) AS light_sleep,

    -- REM Sleep avec interprétation
    STRUCT(
        ROUND(dailySleepDTO.remSleepSeconds / 3600.0, 2) AS hours,
        ROUND(100.0 * dailySleepDTO.remSleepSeconds / NULLIF(dailySleepDTO.sleepTimeSeconds, 0), 2) AS percentage,
        STRUCT(
            ROUND(dailySleepDTO.sleepScores.remPercentage.idealStartInSeconds / 3600.0, 1) AS hours,
            ROUND(100.0 * dailySleepDTO.remSleepSeconds / NULLIF(dailySleepDTO.sleepTimeSeconds, 0), 2) >=
                ROUND(100.0 * dailySleepDTO.sleepScores.remPercentage.idealStartInSeconds / NULLIF(dailySleepDTO.sleepTimeSeconds, 0), 2) AS is_sufficient
        ) AS optimal_min,
        STRUCT(
            ROUND(dailySleepDTO.sleepScores.remPercentage.idealEndInSeconds / 3600.0, 1) AS hours,
            ROUND(100.0 * dailySleepDTO.remSleepSeconds / NULLIF(dailySleepDTO.sleepTimeSeconds, 0), 2) <=
                ROUND(100.0 * dailySleepDTO.sleepScores.remPercentage.idealEndInSeconds / NULLIF(dailySleepDTO.sleepTimeSeconds, 0), 2) AS is_within_range
        ) AS optimal_max,
        CASE
            WHEN ROUND(100.0 * dailySleepDTO.remSleepSeconds / NULLIF(dailySleepDTO.sleepTimeSeconds, 0), 2) >=
                    ROUND(100.0 * dailySleepDTO.sleepScores.remPercentage.idealStartInSeconds / NULLIF(dailySleepDTO.sleepTimeSeconds, 0), 2)
                AND ROUND(100.0 * dailySleepDTO.remSleepSeconds / NULLIF(dailySleepDTO.sleepTimeSeconds, 0), 2) <=
                    ROUND(100.0 * dailySleepDTO.sleepScores.remPercentage.idealEndInSeconds / NULLIF(dailySleepDTO.sleepTimeSeconds, 0), 2)
            THEN 'optimal'
            WHEN ROUND(100.0 * dailySleepDTO.remSleepSeconds / NULLIF(dailySleepDTO.sleepTimeSeconds, 0), 2) <
                    ROUND(100.0 * dailySleepDTO.sleepScores.remPercentage.idealStartInSeconds / NULLIF(dailySleepDTO.sleepTimeSeconds, 0), 2)
            THEN 'insufficient'
            ELSE 'excessive'
        END AS status
    ) AS rem_sleep,

    -- Temps éveillé avec qualité
    STRUCT(
        ROUND(dailySleepDTO.awakeSleepSeconds / 3600.0, 2) AS hours,
        ROUND(100.0 * dailySleepDTO.awakeSleepSeconds / NULLIF(dailySleepDTO.sleepTimeSeconds, 0), 2) AS percentage,
        dailySleepDTO.awakeCount AS wake_count,
        CASE
            WHEN dailySleepDTO.awakeCount = 0 THEN 'excellent'
            WHEN dailySleepDTO.awakeCount <= 2 THEN 'good'
            WHEN dailySleepDTO.awakeCount <= 5 THEN 'fair'
            ELSE 'poor'
        END AS quality
    ) AS awake,

    -- Body Battery avec qualité de récupération
    STRUCT(
        CASE
            WHEN ARRAY_LENGTH(sleepBodyBattery) > 0 THEN sleepBodyBattery[OFFSET(0)].value
            ELSE NULL
        END AS at_bedtime,
        CASE
            WHEN ARRAY_LENGTH(sleepBodyBattery) > 0 THEN sleepBodyBattery[OFFSET(ARRAY_LENGTH(sleepBodyBattery) - 1)].value
            ELSE NULL
        END AS at_waketime,
        bodyBatteryChange AS recovery,
        CASE
            WHEN bodyBatteryChange >= 30 THEN 'excellent_recovery'
            WHEN bodyBatteryChange >= 20 THEN 'good_recovery'
            WHEN bodyBatteryChange >= 10 THEN 'moderate_recovery'
            WHEN bodyBatteryChange >= 0 THEN 'minimal_recovery'
            ELSE 'negative_recovery'
        END AS recovery_quality
    ) AS body_battery,

    -- Métriques santé
    avgOvernightHrv AS avg_hrv,
    dailySleepDTO.avgSleepStress AS avg_stress,
    restingHeartRate AS resting_heart_rate,

    -- Score de qualité global
    CASE
        WHEN dailySleepDTO.sleepScores.overall.value >= 90 THEN 'excellent'
        WHEN dailySleepDTO.sleepScores.overall.value >= 80 THEN 'good'
        WHEN dailySleepDTO.sleepScores.overall.value >= 70 THEN 'fair'
        WHEN dailySleepDTO.sleepScores.overall.value >= 60 THEN 'poor'
        ELSE 'very_poor'
    END AS sleep_quality,

    -- Niveaux de sommeil granulaires avec interprétation (en heure locale)
    ARRAY(
        SELECT AS STRUCT
            DATETIME_ADD(
                DATETIME(startGMT),
                INTERVAL CAST((dailySleepDTO.sleepStartTimestampLocal - dailySleepDTO.sleepStartTimestampGMT) / 1000 / 3600 AS INT64) HOUR
            ) AS start_time,
            DATETIME_ADD(
                DATETIME(endGMT),
                INTERVAL CAST((dailySleepDTO.sleepStartTimestampLocal - dailySleepDTO.sleepStartTimestampGMT) / 1000 / 3600 AS INT64) HOUR
            ) AS end_time,
            activityLevel AS level_code,
            CASE
                WHEN activityLevel = -1.0 THEN 'awake'
                WHEN activityLevel = 0.0 THEN 'deep'
                WHEN activityLevel = 1.0 THEN 'light'
                WHEN activityLevel = 2.0 THEN 'rem'
                WHEN activityLevel = 3.0 THEN 'awake_restless'
                ELSE 'unknown'
            END AS level_name,
            DATETIME_DIFF(
                DATETIME(endGMT),
                DATETIME(startGMT),
                MINUTE
            ) AS duration_minutes
        FROM UNNEST(sleepLevels)
        ORDER BY startGMT
    ) AS sleep_levels,

    _dp_inserted_at

FROM {{ ref('lake_garmin__svc_sleep') }}
WHERE date IS NOT NULL
