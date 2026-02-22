# api/routers/activities.py
from fastapi import APIRouter, HTTPException
from typing import List
from api.models.activities import RecentActivity, ActivityListItem, ActivityDetail, GpsCoordinate
from api.database import get_bq_client
from api.config import PROJECT_ID, DATASET, DATASET_HUB

router = APIRouter()


@router.get("/recent", response_model=List[RecentActivity])
async def get_recent_activities():
    """Récupère les 100 dernières activités avec distance et durée formatées"""
    query = f"""
        SELECT
            activityId,
            activityName,
            startTimeGMT,
            ROUND(distance / 1000, 2) AS distance_km,
            CAST(ROUND(duration / 60, 0) AS INT64) AS duration_minutes,
            averageSpeed,
            typeKey
        FROM `{PROJECT_ID}.{DATASET}.pct_activites__last_run`
        ORDER BY startTimeGMT DESC
    """

    try:
        bq_client = get_bq_client()
        results = bq_client.query(query).result()
        return [dict(row) for row in results]
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error fetching activities: {str(e)}"
        )


@router.get("/list", response_model=List[ActivityListItem])
async def get_activities_list():
    """
    Retourne la liste paginée des activités pour l'affichage en cards.
    Données pré-agrégées par la vue dbt pct_activites__list :
    titre, type, distance, durée, FC moyenne, zones cardio Z1-Z5 en %, tracé GPS simplifié.
    """
    query = f"""
        SELECT
            activityId,
            activityName,
            startTimeGMT,
            typeKey,
            distance_km,
            duration_minutes,
            averageHR,
            hrZone1_pct,
            hrZone2_pct,
            hrZone3_pct,
            hrZone4_pct,
            hrZone5_pct,
            polyline_simplified
        FROM `{PROJECT_ID}.{DATASET}.pct_activites__list`
        ORDER BY startTimeGMT DESC
    """

    try:
        bq_client = get_bq_client()
        results = bq_client.query(query).result()
        return [dict(row) for row in results]
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error fetching activities list: {str(e)}"
        )


@router.get("/detail/{activity_id}", response_model=ActivityDetail)
async def get_activity_detail(activity_id: int):
    """
    Retourne le détail complet d'une activité pour la page de détail :
    - Métriques de base, zones cardio, scores d'entraînement
    - training_intervals enrichis avec intensityType et name réels Garmin
    - time_series : FC, altitude, allure, vitesse et distance cumulée
    - coordinates : points GPS bruts (lat/lng) pour la carte RunMap
    - tracks_played : musique écoutée pendant l'activité
    """
    query = f"""
        SELECT
            activityId,
            activityName,
            startTimeGMT,
            endTimeGMT,
            typeKey,
            distance,
            duration,
            elapsedDuration,
            elevationGain,
            elevationLoss,
            minElevation,
            maxElevation,
            averageSpeed,
            averageHR,
            maxHR,
            calories,
            hasPolyline,
            aerobicTrainingEffect,
            anaerobicTrainingEffect,
            activityTrainingLoad,
            fastestSplits,
            hr_zones,
            power_zones,
            kilometer_laps,
            training_intervals,
            time_series,
            tracks_played
        FROM `{PROJECT_ID}.{DATASET}.pct_activites__last_run`
        WHERE activityId = @activity_id
        LIMIT 1
    """

    from google.cloud import bigquery

    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("activity_id", "INT64", activity_id)]
    )

    try:
        bq_client = get_bq_client()

        # Récupération des données principales depuis la vue produit
        rows = list(bq_client.query(query, job_config=job_config).result())
        if not rows:
            raise HTTPException(status_code=404, detail=f"Activity {activity_id} not found")

        data = dict(rows[0])

        # Récupération des coordonnées GPS depuis le hub (polyline non exposée dans la vue produit)
        poly_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("activity_id", "INT64", activity_id)]
        )
        poly_rows = list(bq_client.query(
            f"""
            SELECT p.lat AS lat, p.lon AS lng
            FROM `{PROJECT_ID}.{DATASET_HUB}.hub_health__svc_activities`,
                 UNNEST(polyline) AS p
            WHERE activityId = @activity_id
            ORDER BY p.time
            """,
            job_config=poly_config
        ).result())
        data["coordinates"] = [{"lat": r["lat"], "lng": r["lng"]} for r in poly_rows] if poly_rows else None

        return data

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error fetching activity detail: {str(e)}"
        )
