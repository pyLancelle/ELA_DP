# api/routers/music.py
from fastapi import APIRouter, Query, HTTPException
from typing import List, Optional
import math
import asyncio
from api.models.music import (
    TopArtist,
    TopTrack,
    TopAlbum,
    MusicClassement,
    RecentlyPlayedResponse,
    RecentlyPlayedItem,
    RecentlyPlayedTrack,
    RecentlyPlayedArtist,
    RecentlyPlayedAlbum,
    Pagination,
)
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


@router.get("/music-classement", response_model=MusicClassement)
async def get_music_classement(
    period: str = Query("last_7_days", description="Time period for analytics"),
    limit: int = Query(
        DEFAULT_LIMIT, ge=1, le=MAX_LIMIT, description="Number of results"
    ),
):
    """Récupère tous les classements (artistes, titres, albums) en un seul appel"""

    if period not in VALID_PERIODS:
        raise HTTPException(
            status_code=400, detail=f"Invalid period. Must be one of: {VALID_PERIODS}"
        )

    async def fetch_top_artists():
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
        bq_client = get_bq_client()
        results = bq_client.query(query).result()
        return [dict(row) for row in results]

    async def fetch_top_tracks():
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
        bq_client = get_bq_client()
        results = bq_client.query(query).result()
        return [dict(row) for row in results]

    async def fetch_top_albums():
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
        bq_client = get_bq_client()
        results = bq_client.query(query).result()
        return [dict(row) for row in results]

    try:
        # Exécuter les 3 requêtes en parallèle
        artists, tracks, albums = await asyncio.gather(
            fetch_top_artists(), fetch_top_tracks(), fetch_top_albums()
        )

        return MusicClassement(
            top_artists=artists, top_tracks=tracks, top_albums=albums
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching data: {str(e)}")


@router.get("/recently-played", response_model=RecentlyPlayedResponse)
async def get_recently_played(
    page: int = Query(1, ge=1, description="Page number"),
    pageSize: int = Query(50, ge=1, le=200, description="Items per page"),
    dateFrom: Optional[str] = Query(None, description="Start date filter (YYYY-MM-DD)"),
    dateTo: Optional[str] = Query(None, description="End date filter (YYYY-MM-DD)"),
    timeFrom: Optional[str] = Query(None, description="Start time filter (HH:MM)"),
    timeTo: Optional[str] = Query(None, description="End time filter (HH:MM)"),
    artist: Optional[str] = Query(
        None, description="Artist name filter (partial match)"
    ),
):
    """Récupère les pistes récemment écoutées avec pagination et filtres"""

    # Build WHERE clauses
    where_clauses = []
    if dateFrom:
        where_clauses.append(f"played_date >= '{dateFrom}'")
    if dateTo:
        where_clauses.append(f"played_date <= '{dateTo}'")
    if timeFrom:
        where_clauses.append(f"played_time >= '{timeFrom}'")
    if timeTo:
        where_clauses.append(f"played_time <= '{timeTo}'")
    if artist:
        where_clauses.append(f"LOWER(artist_name) LIKE LOWER('%{artist}%')")

    where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

    # Calculate offset
    offset = (page - 1) * pageSize

    async def fetch_tracks():
        query = f"""
            SELECT
                played_at,
                track_id,
                track_uri,
                track_name,
                track_duration_ms,
                track_external_url,
                artist_id,
                artist_name,
                album_id,
                album_name,
                album_image_url
            FROM `{PROJECT_ID}.{DATASET}.pct_music__recently_played`
            {where_sql}
            ORDER BY played_at DESC
            LIMIT {pageSize}
            OFFSET {offset}
        """
        bq_client = get_bq_client()
        results = bq_client.query(query).result()
        return [dict(row) for row in results]

    async def fetch_total_count():
        query = f"""
            SELECT COUNT(*) as total
            FROM `{PROJECT_ID}.{DATASET}.pct_music__recently_played`
            {where_sql}
        """
        bq_client = get_bq_client()
        results = bq_client.query(query).result()
        return list(results)[0]["total"]

    async def fetch_all_artists():
        query = f"""
            SELECT DISTINCT artist_name
            FROM `{PROJECT_ID}.{DATASET}.pct_music__recently_played`
            WHERE artist_name IS NOT NULL
            ORDER BY artist_name
        """
        bq_client = get_bq_client()
        results = bq_client.query(query).result()
        return [row["artist_name"] for row in results]

    try:
        tracks_data, total_count, all_artists = await asyncio.gather(
            fetch_tracks(), fetch_total_count(), fetch_all_artists()
        )

        # Transform data to response format
        tracks = []
        for row in tracks_data:
            played_at_str = row["played_at"].isoformat() if row["played_at"] else None
            item = RecentlyPlayedItem(
                id=f"{row['track_id']}_{played_at_str}",
                played_at=played_at_str,
                track=RecentlyPlayedTrack(
                    id=row["track_uri"] or row["track_id"],
                    name=row["track_name"],
                    duration_ms=row["track_duration_ms"] or 0,
                    external_url=row["track_external_url"],
                ),
                artist=RecentlyPlayedArtist(
                    id=row["artist_id"] or "",
                    name=row["artist_name"] or "",
                ),
                album=RecentlyPlayedAlbum(
                    id=row["album_id"] or "",
                    name=row["album_name"] or "",
                    image_url=row["album_image_url"],
                ),
            )
            tracks.append(item)

        total_pages = math.ceil(total_count / pageSize) if total_count > 0 else 1

        return RecentlyPlayedResponse(
            tracks=tracks,
            pagination=Pagination(
                page=page,
                pageSize=pageSize,
                totalItems=total_count,
                totalPages=total_pages,
            ),
            artists=all_artists,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching data: {str(e)}")
