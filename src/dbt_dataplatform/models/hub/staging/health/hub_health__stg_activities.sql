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

-- CTE pour les intervalles
intervals_data AS (
    SELECT
        splits.activityId,
        ARRAY_AGG(
            STRUCT(
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
        ) AS training_intervals
    FROM {{ ref('lake_garmin__svc_activity_splits') }} AS splits,
         UNNEST(splits.typed_splits.splits) AS ss
    WHERE ss.type LIKE 'INTERVAL%'
    GROUP BY splits.activityId
),

-- CTE pour les données timeseries
timeseries_data AS (
    SELECT
        details.activityId,
        ARRAY_AGG(md ORDER BY md.directTimestamp) AS timeseries
    FROM {{ ref('lake_garmin__svc_activity_details') }} AS details,
         UNNEST(details.detailed_data.activityDetailMetrics) AS md
    GROUP BY details.activityId
),

-- CTE pour la polyline GPS
polyline_data AS (
    SELECT
        details.activityId,
        ARRAY_AGG(
            STRUCT(
                p.lat,
                p.lon,
                p.time
            )
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
    timeseries_data.timeseries,
    polyline_data.polyline

FROM {{ ref('lake_garmin__svc_activities') }} AS activities
LEFT JOIN laps_data
    ON activities.activityId = laps_data.activityId
LEFT JOIN intervals_data
    ON activities.activityId = intervals_data.activityId
LEFT JOIN timeseries_data
    ON activities.activityId = timeseries_data.activityId
LEFT JOIN polyline_data
    ON activities.activityId = polyline_data.activityId
WHERE activities.activityType.typeKey IN ('running', 'trail_running', 'track_running', 'treadmill_running')
