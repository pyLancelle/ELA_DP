# api/routers/activities.py
from fastapi import APIRouter, HTTPException
from typing import List
from api.models.activities import RecentActivity
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
