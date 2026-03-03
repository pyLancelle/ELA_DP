# api/routers/artist_focus.py
from fastapi import APIRouter, HTTPException, Query
from typing import List, Optional
import asyncio
from api.models.artist_focus import (
    ArtistSummary,
    ArtistFocusOverview,
    ArtistFocusTrack,
    ArtistFocusAlbum,
    ArtistFocusCalendarDay,
    ArtistFocusHeatmapCell,
    ArtistFocusMonthly,
    ArtistFocusProfile,
)
from api.database import get_bq_client
from api.config import PROJECT_ID, DATASET

router = APIRouter()

TABLE_OVERVIEW = f"`{PROJECT_ID}.{DATASET}.pct_focus_artist__overview`"
TABLE_TOP_TRACKS = f"`{PROJECT_ID}.{DATASET}.pct_focus_artist__top_tracks`"
TABLE_ALBUMS = f"`{PROJECT_ID}.{DATASET}.pct_focus_artist__albums`"
TABLE_CALENDAR = f"`{PROJECT_ID}.{DATASET}.pct_focus_artist__listening_calendar`"
TABLE_HEATMAP = f"`{PROJECT_ID}.{DATASET}.pct_focus_artist__listening_heatmap`"
TABLE_EVOLUTION = f"`{PROJECT_ID}.{DATASET}.pct_focus_artist__evolution`"


@router.get("/artists", response_model=List[ArtistSummary])
async def list_artists(
    search: Optional[str] = Query(None, description="Filter by artist name (partial match)"),
    limit: int = Query(50, ge=1, le=200, description="Max results"),
):
    """Liste tous les artistes avec leurs stats globales (pour recherche / autocomplete)"""
    where = ""
    if search:
        where = f"WHERE LOWER(artist_name) LIKE LOWER('%{search}%')"

    query = f"""
        SELECT
            artist_id,
            artist_name,
            image_url,
            total_plays,
            total_duration
        FROM {TABLE_OVERVIEW}
        {where}
        ORDER BY total_plays DESC
        LIMIT {limit}
    """
    try:
        bq_client = get_bq_client()
        results = bq_client.query(query).result()
        return [dict(row) for row in results]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching artists: {str(e)}")


@router.get("/{artist_id}/overview", response_model=ArtistFocusOverview)
async def get_artist_overview(artist_id: str):
    """Carte d'identité + métriques globales d'un artiste"""
    query = f"""
        SELECT *
        FROM {TABLE_OVERVIEW}
        WHERE artist_id = '{artist_id}'
        LIMIT 1
    """
    try:
        bq_client = get_bq_client()
        results = list(bq_client.query(query).result())
        if not results:
            raise HTTPException(status_code=404, detail=f"Artist '{artist_id}' not found")
        return dict(results[0])
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching overview: {str(e)}")


@router.get("/{artist_id}/tracks", response_model=List[ArtistFocusTrack])
async def get_artist_top_tracks(artist_id: str):
    """Top 20 titres de l'artiste classés par nombre d'écoutes"""
    query = f"""
        SELECT *
        FROM {TABLE_TOP_TRACKS}
        WHERE artist_id = '{artist_id}'
        ORDER BY track_rank ASC
    """
    try:
        bq_client = get_bq_client()
        results = bq_client.query(query).result()
        return [dict(row) for row in results]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching tracks: {str(e)}")


@router.get("/{artist_id}/albums", response_model=List[ArtistFocusAlbum])
async def get_artist_albums(artist_id: str):
    """Albums de l'artiste avec taux de complétion et profondeur d'écoute"""
    query = f"""
        SELECT *
        FROM {TABLE_ALBUMS}
        WHERE artist_id = '{artist_id}'
        ORDER BY total_duration_ms DESC
    """
    try:
        bq_client = get_bq_client()
        results = bq_client.query(query).result()
        return [dict(row) for row in results]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching albums: {str(e)}")


@router.get("/{artist_id}/calendar", response_model=List[ArtistFocusCalendarDay])
async def get_artist_calendar(artist_id: str):
    """Écoutes jour par jour (heatmap calendrier)"""
    query = f"""
        SELECT *
        FROM {TABLE_CALENDAR}
        WHERE artist_id = '{artist_id}'
        ORDER BY listen_date ASC
    """
    try:
        bq_client = get_bq_client()
        results = bq_client.query(query).result()
        return [dict(row) for row in results]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching calendar: {str(e)}")


@router.get("/{artist_id}/heatmap", response_model=List[ArtistFocusHeatmapCell])
async def get_artist_heatmap(artist_id: str):
    """Densité d'écoute par heure x jour de la semaine"""
    query = f"""
        SELECT *
        FROM {TABLE_HEATMAP}
        WHERE artist_id = '{artist_id}'
        ORDER BY day_of_week ASC, hour_of_day ASC
    """
    try:
        bq_client = get_bq_client()
        results = bq_client.query(query).result()
        return [dict(row) for row in results]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching heatmap: {str(e)}")


@router.get("/{artist_id}/evolution", response_model=List[ArtistFocusMonthly])
async def get_artist_evolution(artist_id: str):
    """Tendance mensuelle d'écoute"""
    query = f"""
        SELECT *
        FROM {TABLE_EVOLUTION}
        WHERE artist_id = '{artist_id}'
        ORDER BY year_month ASC
    """
    try:
        bq_client = get_bq_client()
        results = bq_client.query(query).result()
        return [dict(row) for row in results]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching evolution: {str(e)}")


@router.get("/{artist_id}", response_model=ArtistFocusProfile)
async def get_artist_profile(artist_id: str):
    """Profil complet de l'artiste — toutes les données en un seul appel"""

    async def fetch_overview():
        query = f"""
            SELECT * FROM {TABLE_OVERVIEW}
            WHERE artist_id = '{artist_id}' LIMIT 1
        """
        bq_client = get_bq_client()
        results = list(bq_client.query(query).result())
        if not results:
            raise HTTPException(status_code=404, detail=f"Artist '{artist_id}' not found")
        return dict(results[0])

    async def fetch_top_tracks():
        query = f"""
            SELECT * FROM {TABLE_TOP_TRACKS}
            WHERE artist_id = '{artist_id}'
            ORDER BY track_rank ASC
        """
        bq_client = get_bq_client()
        return [dict(row) for row in bq_client.query(query).result()]

    async def fetch_albums():
        query = f"""
            SELECT * FROM {TABLE_ALBUMS}
            WHERE artist_id = '{artist_id}'
            ORDER BY total_duration_ms DESC
        """
        bq_client = get_bq_client()
        return [dict(row) for row in bq_client.query(query).result()]

    async def fetch_calendar():
        query = f"""
            SELECT * FROM {TABLE_CALENDAR}
            WHERE artist_id = '{artist_id}'
            ORDER BY listen_date ASC
        """
        bq_client = get_bq_client()
        return [dict(row) for row in bq_client.query(query).result()]

    async def fetch_heatmap():
        query = f"""
            SELECT * FROM {TABLE_HEATMAP}
            WHERE artist_id = '{artist_id}'
            ORDER BY day_of_week ASC, hour_of_day ASC
        """
        bq_client = get_bq_client()
        return [dict(row) for row in bq_client.query(query).result()]

    async def fetch_evolution():
        query = f"""
            SELECT * FROM {TABLE_EVOLUTION}
            WHERE artist_id = '{artist_id}'
            ORDER BY year_month ASC
        """
        bq_client = get_bq_client()
        return [dict(row) for row in bq_client.query(query).result()]

    try:
        overview, top_tracks, albums, calendar, heatmap, evolution = await asyncio.gather(
            fetch_overview(),
            fetch_top_tracks(),
            fetch_albums(),
            fetch_calendar(),
            fetch_heatmap(),
            fetch_evolution(),
        )
        return ArtistFocusProfile(
            overview=overview,
            top_tracks=top_tracks,
            albums=albums,
            calendar=calendar,
            heatmap=heatmap,
            evolution=evolution,
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching artist profile: {str(e)}")
