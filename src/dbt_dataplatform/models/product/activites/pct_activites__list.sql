/*
Vue produit pré-agrégée pour l'endpoint /api/activites.
Expose une ligne par activité avec toutes les données nécessaires pour les cards visuelles :
titre, type, distance, durée, FC moyenne, zones cardio (Z1-Z5 en %), et tracé GPS simplifié.
Matérialisée en table pour éviter les calculs à la volée.
*/

{{
  config(
      tags=['product', 'activites'],
      materialized='table'
  )
}}

WITH

-- Toutes les activités (tous types) depuis la source lake
all_activities AS (
    SELECT
        activityId,
        activityName,
        startTimeGMT,
        activityType.typeKey                                    AS typeKey,
        distance,
        duration,
        averageHR,
        hasPolyline,
        hrTimeInZone_1,
        hrTimeInZone_2,
        hrTimeInZone_3,
        hrTimeInZone_4,
        hrTimeInZone_5
    FROM {{ ref('lake_garmin__svc_activities') }}
    WHERE activityId IS NOT NULL
),

-- Polyline GPS simplifiée : échantillonnage régulier pour obtenir ~80 points max
polyline_data AS (
    SELECT
        details.activityId,
        -- Taille totale de la polyline brute
        ARRAY_LENGTH(
            ARRAY_AGG(p ORDER BY p.time)
        )                                                       AS raw_point_count,
        -- Points bruts ordonnés avec leur index
        ARRAY_AGG(
            STRUCT(p.lat AS lat, p.lon AS lng, p.time AS time)
            ORDER BY p.time
        )                                                       AS raw_points
    FROM {{ ref('lake_garmin__svc_activity_details') }} AS details,
         UNNEST(details.detailed_data.geoPolylineDTO.polyline) AS p
    GROUP BY details.activityId
),

-- Calcul du pas d'échantillonnage et application
polyline_simplified AS (
    SELECT
        activityId,
        -- Échantillonnage régulier : on garde 1 point tous les N points pour ~80 points max
        -- Si <= 80 points : on garde tout. Sinon : step = CEIL(count / 80)
        ARRAY(
            SELECT
                STRUCT(pt.lat AS lat, pt.lng AS lng)
            FROM UNNEST(raw_points) AS pt WITH OFFSET AS idx
            WHERE
                raw_point_count <= 80
                OR MOD(idx, CAST(CEIL(raw_point_count / 80.0) AS INT64)) = 0
        )                                                       AS simplified_points
    FROM polyline_data
),

-- Calcul des pourcentages de zones cardio
hr_zone_pcts AS (
    SELECT
        activityId,
        hrTimeInZone_1,
        hrTimeInZone_2,
        hrTimeInZone_3,
        hrTimeInZone_4,
        hrTimeInZone_5,
        -- Total des secondes en zone, pour la division
        (
            COALESCE(hrTimeInZone_1, 0)
            + COALESCE(hrTimeInZone_2, 0)
            + COALESCE(hrTimeInZone_3, 0)
            + COALESCE(hrTimeInZone_4, 0)
            + COALESCE(hrTimeInZone_5, 0)
        )                                                       AS hr_zones_total_sec
    FROM all_activities
)

SELECT
    -- Identifiants
    a.activityId,
    a.activityName,

    -- Date ISO 8601
    TIMESTAMP(a.startTimeGMT)                                   AS startTimeGMT,

    -- Type d'activité
    a.typeKey,

    -- Distance et durée formatées
    ROUND(a.distance / 1000.0, 1)                              AS distance_km,
    ROUND(a.duration / 60.0, 1)                                AS duration_minutes,

    -- Fréquence cardiaque moyenne
    CAST(a.averageHR AS INT64)                                  AS averageHR,

    -- Zones cardio en pourcentage du temps total en zone
    CAST(
        ROUND(
            z.hrTimeInZone_1 / NULLIF(z.hr_zones_total_sec, 0) * 100
        ) AS INT64
    )                                                           AS hrZone1_pct,
    CAST(
        ROUND(
            z.hrTimeInZone_2 / NULLIF(z.hr_zones_total_sec, 0) * 100
        ) AS INT64
    )                                                           AS hrZone2_pct,
    CAST(
        ROUND(
            z.hrTimeInZone_3 / NULLIF(z.hr_zones_total_sec, 0) * 100
        ) AS INT64
    )                                                           AS hrZone3_pct,
    CAST(
        ROUND(
            z.hrTimeInZone_4 / NULLIF(z.hr_zones_total_sec, 0) * 100
        ) AS INT64
    )                                                           AS hrZone4_pct,
    CAST(
        ROUND(
            z.hrTimeInZone_5 / NULLIF(z.hr_zones_total_sec, 0) * 100
        ) AS INT64
    )                                                           AS hrZone5_pct,

    -- Polyline GPS simplifiée : JSON string [[lat, lng], ...]
    -- null si l'activité n'a pas de polyline ou si aucun point GPS disponible
    CASE
        WHEN a.hasPolyline IS FALSE OR a.hasPolyline IS NULL THEN NULL
        WHEN ARRAY_LENGTH(ps.simplified_points) = 0 THEN NULL
        -- BigQuery n'accepte pas ARRAY<ARRAY<FLOAT64>> (nested arrays interdits).
        -- On sérialise directement via TO_JSON_STRING sur le tableau de structs {lat, lng},
        -- ce qui produit [{"lat":48.8566,"lng":2.3522}, ...].
        -- Côté API : json.loads(row["polyline_simplified"]) → liste de dicts.
        ELSE TO_JSON_STRING(ps.simplified_points)
    END                                                         AS polyline_simplified

FROM all_activities AS a

LEFT JOIN hr_zone_pcts AS z
    ON a.activityId = z.activityId

LEFT JOIN polyline_simplified AS ps
    ON a.activityId = ps.activityId

ORDER BY a.startTimeGMT DESC
