/*
Activités de running récentes avec les titres musicaux écoutés pendant chaque activité.

Ce modèle joint les activités de running avec les musiques écoutées pendant la durée de l'activité,
en tenant compte des timezones pour une correspondance temporelle précise.
*/

{{
  config(
      tags=['health', 'music', 'product'],
      materialized='view'
  )
}}

WITH
-- Convertir les horaires des activités en UTC pour correspondance avec les musiques
activities_with_utc_times AS (
    SELECT
        activityId,
        activityName,
        TIMESTAMP(startTimeGMT) AS activity_start_utc,
        TIMESTAMP(endTimeGMT) AS activity_end_utc,
        CAST(distance AS INT) as duration,
        duration,
        elapsedDuration,
        averageSpeed,
        calories,
        averageHR,
        maxHR,
        -- Autres champs utiles
        elevationGain,
        elevationLoss,
        hasPolyline
    FROM {{ ref('hub_health__svc_activities') }}
    ORDER BY activityId DESC
    LIMIT 100
),

-- Convertir les horaires des musiques en UTC (playedAt est déjà en UTC dans Spotify)
music_played_with_utc AS (
    SELECT
        playedAt AS music_played_utc,
        trackid,
        contextType,
        contextUri,
        contextHref,
        contextExternalUrl
    FROM {{ ref('hub_music__svc_fact_played') }}
),

-- Joindre les activités avec les musiques écoutées pendant leur durée
activities_with_music AS (
    SELECT
        a.*,
        -- Informations sur la musique
        ARRAY_AGG(
            STRUCT(
                m.music_played_utc AS track_played_at,
                m.trackid,
                t.trackName,
                t.all_artist_names,
                t.trackDurationMS,
                t.trackExternalUrl,
                al.albumName,
                al.albumImageUrl,
                ar.artistName AS primary_artist,
                ar.artistExternalUrl
            )
            ORDER BY m.music_played_utc
        ) AS tracks_played_during_activity,
        -- Statistiques sur la musique pendant l'activité
        COUNT(DISTINCT m.trackid) AS unique_tracks_count,
        SUM(t.trackDurationMS) AS total_music_duration_ms
    FROM activities_with_utc_times a
    LEFT JOIN music_played_with_utc m
        ON m.music_played_utc >= a.activity_start_utc
        AND m.music_played_utc <= a.activity_end_utc
    LEFT JOIN {{ ref('hub_music__svc_dim_tracks') }} t
        ON m.trackid = t.trackid
    LEFT JOIN {{ ref('hub_music__svc_dim_albums') }} al
        ON t.albumid = al.albumid
    LEFT JOIN {{ ref('hub_music__svc_bridge_tracks_artists') }} ba
        ON t.trackid = ba.trackid AND ba.artist_role = 'primary'
    LEFT JOIN {{ ref('hub_music__svc_dim_artists') }} ar
        ON ba.artistid = ar.artistid
    GROUP BY 
        activityId, activityName, activity_start_utc, activity_end_utc, 
        distance, duration, elapsedDuration, averageSpeed, calories, 
        averageHR, maxHR, elevationGain, elevationLoss, hasPolyline
)

SELECT
    activityId,
    activityName,
    activity_start_utc,
    activity_end_utc,

    distance,
    duration,
    elapsedDuration,
    averageSpeed,
    calories,
    averageHR,
    maxHR,
    elevationGain,
    elevationLoss,
    hasPolyline,
    
    -- Informations musicales
    tracks_played_during_activity,
    unique_tracks_count,
    total_music_duration_ms,
    
    -- Durée de l'activité en format lisible
    {{ duration_to_hms('duration') }} AS duration_formatted,
    
    -- Durée musicale en format lisible
    {{ ms_to_hms('total_music_duration_ms') }} AS music_duration_formatted,
    
    -- Ratio musique/activité
    CASE
        WHEN duration > 0 THEN ROUND(SAFE_DIVIDE(total_music_duration_ms, duration * 1000), 2)
        ELSE NULL
    END AS music_coverage_ratio

FROM activities_with_music
ORDER BY activityId DESC