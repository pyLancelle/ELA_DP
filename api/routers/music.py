# api/routers/music.py
from fastapi import APIRouter, Query, HTTPException
from typing import List
from api.models.music import TopArtist, TopTrack, TopAlbum
from api.database import get_bq_client
from api.config import VALID_PERIODS, DEFAULT_LIMIT, MAX_LIMIT, PROJECT_ID, DATASET

router = APIRouter()


@router.get("/top-artists", response_model=List[TopArtist])
async def get_top_artists(
    period: str = Query("last_7_days", description="Time period for analytics"),
    limit: int = Query(
        DEFAULT_LIMIT, ge=1, le=MAX_LIMIT, description="Number of results"
    ),
):
    """Récupère le top des artistes pour une période donnée"""

    # Validation
    if period not in VALID_PERIODS:
        raise HTTPException(
            status_code=400, detail=f"Invalid period. Must be one of: {VALID_PERIODS}"
        )

    # Query BigQuery
    query = f"""
        SELECT 
            rank,
            artistname as name,
            play_count,
            total_duration,
            albumimageurl as image_url,
            artistexternalurl as external_url
        FROM `{PROJECT_ID}.{DATASET}.pct_classement__top_artist_by_period`
        WHERE period = '{period}'
        ORDER BY rank ASC
        LIMIT {limit}
    """

    try:
        bq_client = get_bq_client()
        results = bq_client.query(query).result()
        return [dict(row) for row in results]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching data: {str(e)}")


@router.get("/top-tracks", response_model=List[TopTrack])
async def get_top_tracks(
    period: str = Query("last_7_days"),
    limit: int = Query(DEFAULT_LIMIT, ge=1, le=MAX_LIMIT),
):
    """Récupère le top des titres pour une période donnée"""

    if period not in VALID_PERIODS:
        raise HTTPException(
            status_code=400, detail=f"Invalid period. Must be one of: {VALID_PERIODS}"
        )

    query = f"""
        SELECT 
            rank,
            trackname as name,
            all_artist_names as artist_name,
            play_count,
            total_duration,
            albumimageurl as image_url,
            trackexternalurl as external_url
        FROM `{PROJECT_ID}.{DATASET}.pct_classement__top_track_by_period`
        WHERE period = '{period}'
        ORDER BY rank ASC
        LIMIT {limit}
    """

    try:
        bq_client = get_bq_client()
        results = bq_client.query(query).result()
        return [dict(row) for row in results]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching data: {str(e)}")


@router.get("/top-albums", response_model=List[TopAlbum])
async def get_top_albums(
    period: str = Query("last_7_days"),
    limit: int = Query(DEFAULT_LIMIT, ge=1, le=MAX_LIMIT),
):
    """Récupère le top des albums pour une période donnée"""

    if period not in VALID_PERIODS:
        raise HTTPException(
            status_code=400, detail=f"Invalid period. Must be one of: {VALID_PERIODS}"
        )

    query = f"""
        SELECT 
            rank,
            albumname as name,
            all_artist_names as artist_name,
            play_count,
            total_duration,
            albumimageurl as image_url,
            albumexternalurl as external_url
        FROM `{PROJECT_ID}.{DATASET}.pct_classement__top_album_by_period`
        WHERE period = '{period}'
        ORDER BY rank ASC
        LIMIT {limit}
    """

    try:
        bq_client = get_bq_client()
        results = bq_client.query(query).result()
        return [dict(row) for row in results]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching data: {str(e)}")
