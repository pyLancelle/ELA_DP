/*
Vue consolidée des activités running avec laps, intervalles, timeseries et polyline.
*/

{{
  config(
      tags=['health', 'hub'],
      materialized='view'
  )
}}

-- CTE pour les laps par km
WITH laps_data AS (
    SELECT
        splits.activityId,
        ARRAY_AGG(
            STRUCT(
                ss.lapIndex,
                ss.startTimeGMT,
                ss.distance,
                ss.duration,
                ss.averageSpeed,
                ss.calories,
                ss.averageHR,
                ss.maxHR,
                ss.elevationGain,
                ss.elevationLoss
            )
            ORDER BY ss.lapIndex
        ) AS kilometer_laps
    FROM {{ ref('lake_garmin__svc_activity_splits') }} AS splits,
         UNNEST(splits.splits.lapDTOs) AS ss
    GROUP BY splits.activityId
),

-- CTE pour les intervalles avec types Garmin réels
intervals_data AS (
    SELECT
        splits.activityId,
        ARRAY_AGG(
            STRUCT(
                ss.lapIndexes[SAFE_OFFSET(0)]          AS lapIndex,
                ss.startTimeGMT,
                CAST(ss.distance AS FLOAT64)     AS distance,
                CAST(ss.duration AS FLOAT64)     AS duration,
                CAST(ss.averageSpeed AS FLOAT64) AS averageSpeed,
                CAST(ss.calories AS INT64)       AS calories,
                CAST(ss.averageHR AS FLOAT64)    AS averageHR,
                CAST(ss.maxHR AS FLOAT64)        AS maxHR,
                CAST(ss.elevationGain AS FLOAT64) AS elevationGain,
                CAST(ss.elevationLoss AS FLOAT64) AS elevationLoss,
                LOWER(REPLACE(REPLACE(ss.type, 'INTERVAL_', ''), 'INTERVAL', 'work')) AS intensityType,
                CASE ss.type
                    WHEN 'INTERVAL_WARMUP'   THEN 'Échauffement'
                    WHEN 'INTERVAL_WORK'     THEN 'Effort'
                    WHEN 'INTERVAL_RECOVERY' THEN 'Récupération'
                    WHEN 'INTERVAL_COOLDOWN' THEN 'Retour au calme'
                    WHEN 'INTERVAL_REST'     THEN 'Repos'
                    ELSE ss.type
                END AS name
            )
            ORDER BY ss.lapIndexes[SAFE_OFFSET(0)]
        ) AS training_intervals
    FROM {{ ref('lake_garmin__svc_activity_splits') }} AS splits,
         UNNEST(splits.typed_splits.splits) AS ss
    WHERE ss.type LIKE 'INTERVAL%'
    GROUP BY splits.activityId
),

-- CTE pour la time series (FC, altitude, allure, distance cumulée)
time_series_data AS (
    SELECT
        details.activityId,
        ARRAY_AGG(
            STRUCT(
                CAST(md.directTimestamp AS INT64)                                        AS timestamp,
                ROUND(CAST(md.sumDistance AS FLOAT64) / 1000, 4)                        AS distance,
                CAST(md.directHeartRate AS INT64)                                        AS heartRate,
                ROUND(SAFE_DIVIDE(1000, CAST(md.directSpeed AS FLOAT64)) / 60, 4)       AS pace,
                ROUND(CAST(md.directElevation AS FLOAT64), 1)                           AS altitude,
                ROUND(CAST(md.directSpeed AS FLOAT64) * 3.6, 2)                         AS speed
            )
            ORDER BY md.directTimestamp
        ) AS time_series
    FROM {{ ref('lake_garmin__svc_activity_details') }} AS details,
         UNNEST(details.detailed_data.activityDetailMetrics) AS md
    GROUP BY details.activityId
),

-- CTE pour la polyline GPS (coordonnées brutes lat/lng)
polyline_data AS (
    SELECT
        details.activityId,
        ARRAY_AGG(
            STRUCT(
                p.lat,
                p.lon,
                p.time
            )
            ORDER BY p.time
        ) AS polyline
    FROM {{ ref('lake_garmin__svc_activity_details') }} AS details,
         UNNEST(details.detailed_data.geoPolylineDTO.polyline) AS p
    GROUP BY details.activityId
)

SELECT
    activities.activityId,
    activities.activityName,
    activities.startTimeGMT,
    activities.endTimeGMT,
    activities.activityType.typeKey,
    activities.distance,
    activities.duration,
    activities.elapsedDuration,
    activities.elevationGain,
    activities.elevationLoss,
    activities.averageSpeed,
    activities.hasPolyline,
    activities.calories,
    activities.averageHR,
    activities.maxHR,
    activities.aerobicTrainingEffect,
    activities.anaerobicTrainingEffect,
    activities.minElevation,
    activities.maxElevation,
    activities.activityTrainingLoad,
    STRUCT(
        activities.fastestSplit_1000,
        activities.fastestSplit_1609,
        activities.fastestSplit_5000,
        activities.fastestSplit_10000,
        activities.fastestSplit_21098,
        activities.fastestSplit_42195
    ) AS fastestSplits,
    STRUCT(
        activities.hrTimeInZone_1,
        activities.hrTimeInZone_2,
        activities.hrTimeInZone_3,
        activities.hrTimeInZone_4,
        activities.hrTimeInZone_5
    ) AS hr_zones,
    STRUCT(
        activities.powerTimeInZone_1,
        activities.powerTimeInZone_2,
        activities.powerTimeInZone_3,
        activities.powerTimeInZone_4,
        activities.powerTimeInZone_5
    ) AS power_zones,
    laps_data.kilometer_laps,
    intervals_data.training_intervals,
    time_series_data.time_series,
    polyline_data.polyline,
    activities._dp_inserted_at

FROM {{ ref('lake_garmin__svc_activities') }} AS activities
LEFT JOIN laps_data
    ON activities.activityId = laps_data.activityId
LEFT JOIN intervals_data
    ON activities.activityId = intervals_data.activityId
LEFT JOIN time_series_data
    ON activities.activityId = time_series_data.activityId
LEFT JOIN polyline_data
    ON activities.activityId = polyline_data.activityId
WHERE activities.activityType.typeKey IN ('running', 'trail_running', 'track_running', 'treadmill_running')
