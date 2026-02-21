# api/routers/activities.py
from fastapi import APIRouter, HTTPException
from typing import List
from api.models.activities import RecentActivity, ActivityListItem
from api.database import get_bq_client
from api.config import PROJECT_ID, DATASET

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
