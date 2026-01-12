# api/routers/homepage.py
from fastapi import APIRouter, HTTPException
from typing import List
import asyncio
from api.models.homepage import (
    MusicTimeDaily,
    RacePrediction,
    RunningWeekly,
    RunningWeeklyVolume,
    SleepBodyBattery,
    SleepStages,
    TopArtistHomepage,
    TopTrackHomepage,
    Vo2MaxTrend,
    HomepageData,
)
from api.database import get_bq_client
from api.config import PROJECT_ID, DATASET

router = APIRouter()


@router.get("/music-time-daily", response_model=List[MusicTimeDaily])
async def get_music_time_daily():
    """Récupère le temps d'écoute quotidien sur les 14 derniers jours"""
    query = f"""
        SELECT
            date,
            total_duration_ms,
            total_duration
        FROM `{PROJECT_ID}.{DATASET}.pct_homepage__music_time_daily`
        ORDER BY date DESC
    """

    try:
        bq_client = get_bq_client()
        results = bq_client.query(query).result()
        return [dict(row) for row in results]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching data: {str(e)}")


@router.get("/race-prediction", response_model=List[RacePrediction])
async def get_race_prediction():
    """Récupère les prédictions de temps de course pour différentes distances"""
    query = f"""
        SELECT
            distance,
            `current_date`,
            `current_time`,
            previous_date,
            previous_time,
            diff_seconds
        FROM `{PROJECT_ID}.{DATASET}.pct_homepage__race_prediction`
        ORDER BY CASE distance
            WHEN '5K' THEN 1
            WHEN '10K' THEN 2
            WHEN '21K' THEN 3
            WHEN '42K' THEN 4
        END
    """

    try:
        bq_client = get_bq_client()
        results = bq_client.query(query).result()
        return [dict(row) for row in results]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching data: {str(e)}")


@router.get("/running-weekly", response_model=List[RunningWeekly])
async def get_running_weekly():
    """Récupère les statistiques de course hebdomadaires"""
    query = f"""
        SELECT
            date,
            day_of_week,
            total_distance_km,
            aerobic_score,
            anaerobic_score
        FROM `{PROJECT_ID}.{DATASET}.pct_homepage__running_weekly`
        ORDER BY date DESC
    """

    try:
        bq_client = get_bq_client()
        results = bq_client.query(query).result()
        return [dict(row) for row in results]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching data: {str(e)}")


@router.get("/running-weekly-volume", response_model=List[RunningWeeklyVolume])
async def get_running_weekly_volume():
    """Récupère le volume hebdomadaire de course sur les 7 dernières semaines"""
    query = f"""
        SELECT
            week_start,
            number_of_runs,
            total_distance_km
        FROM `{PROJECT_ID}.{DATASET}.pct_homepage__running_weekly_volume`
        ORDER BY week_start DESC
    """

    try:
        bq_client = get_bq_client()
        results = bq_client.query(query).result()
        return [dict(row) for row in results]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching data: {str(e)}")


@router.get("/sleep-body-battery", response_model=List[SleepBodyBattery])
async def get_sleep_body_battery():
    """Récupère les données de sommeil et body battery des 7 derniers jours"""
    query = f"""
        SELECT
            date,
            day_abbr_french,
            sleep_score,
            battery_at_bedtime,
            battery_at_waketime,
            battery_gain,
            avg_hrv,
            resting_hr
        FROM `{PROJECT_ID}.{DATASET}.pct_homepage__sleep_body_battery`
        ORDER BY date ASC
    """

    try:
        bq_client = get_bq_client()
        results = bq_client.query(query).result()
        return [dict(row) for row in results]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching data: {str(e)}")


@router.get("/sleep-stages", response_model=List[SleepStages])
async def get_sleep_stages():
    """Récupère les phases de sommeil détaillées de la dernière nuit"""
    query = f"""
        SELECT
            date,
            start_time,
            end_time,
            level_name
        FROM `{PROJECT_ID}.{DATASET}.pct_homepage__sleep_stages`
        ORDER BY start_time
    """

    try:
        bq_client = get_bq_client()
        results = bq_client.query(query).result()
        return [dict(row) for row in results]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching data: {str(e)}")


@router.get("/top-artists", response_model=List[TopArtistHomepage])
async def get_top_artists():
    """Récupère le top 10 des artistes de la semaine"""
    query = f"""
        SELECT
            rank,
            artistname,
            total_duration,
            play_count,
            artistexternalurl,
            albumimageurl,
            artistid
        FROM `{PROJECT_ID}.{DATASET}.pct_homepage__top_artist`
        ORDER BY rank
    """

    try:
        bq_client = get_bq_client()
        results = bq_client.query(query).result()
        return [dict(row) for row in results]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching data: {str(e)}")


@router.get("/top-tracks", response_model=List[TopTrackHomepage])
async def get_top_tracks():
    """Récupère le top 10 des titres de la semaine"""
    query = f"""
        SELECT
            rank,
            trackname,
            all_artist_names,
            total_duration,
            play_count,
            trackExternalUrl,
            albumimageurl,
            trackid
        FROM `{PROJECT_ID}.{DATASET}.pct_homepage__top_track`
        ORDER BY rank
    """

    try:
        bq_client = get_bq_client()
        results = bq_client.query(query).result()
        return [dict(row) for row in results]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching data: {str(e)}")


@router.get("/vo2max-trend", response_model=Vo2MaxTrend)
async def get_vo2max_trend():
    """Récupère les données de tendance VO2max"""
    query = f"""
        SELECT
            `current_date`,
            current_vo2max,
            weekly_vo2max_array,
            vo2max_delta_6_months
        FROM `{PROJECT_ID}.{DATASET}.pct_homepage__vo2max_trend`
    """

    try:
        bq_client = get_bq_client()
        results = bq_client.query(query).result()
        rows = [dict(row) for row in results]
        if rows:
            data = rows[0]
            # Fix pour le bug BigQuery qui retourne f0_ au lieu de current_date
            # if 'f0_' in data and 'current_date' not in data:
            #     data['current_date'] = data.pop('f0_')
            return data
        else:
            raise HTTPException(status_code=404, detail="No VO2max data found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching data: {str(e)}")


@router.get("/", response_model=HomepageData)
async def get_homepage_data():
    """Récupère toutes les données de la homepage en un seul appel"""

    async def fetch_music_time_daily():
        query = f"""
            SELECT date, total_duration_ms, total_duration
            FROM `{PROJECT_ID}.{DATASET}.pct_homepage__music_time_daily`
            ORDER BY date DESC
        """
        bq_client = get_bq_client()
        results = bq_client.query(query).result()
        return [dict(row) for row in results]

    async def fetch_race_predictions():
        query = f"""
            SELECT distance, `current_date`, `current_time`, previous_date, previous_time, diff_seconds
            FROM `{PROJECT_ID}.{DATASET}.pct_homepage__race_prediction`
            ORDER BY CASE distance WHEN '5K' THEN 1 WHEN '10K' THEN 2 WHEN '21K' THEN 3 WHEN '42K' THEN 4 END
        """
        bq_client = get_bq_client()
        results = bq_client.query(query).result()
        return [dict(row) for row in results]

    async def fetch_running_weekly():
        query = f"""
            SELECT date, day_of_week, total_distance_km, aerobic_score, anaerobic_score
            FROM `{PROJECT_ID}.{DATASET}.pct_homepage__running_weekly`
            ORDER BY date DESC
        """
        bq_client = get_bq_client()
        results = bq_client.query(query).result()
        return [dict(row) for row in results]

    async def fetch_running_weekly_volume():
        query = f"""
            SELECT week_start, number_of_runs, total_distance_km
            FROM `{PROJECT_ID}.{DATASET}.pct_homepage__running_weekly_volume`
            ORDER BY week_start DESC
        """
        bq_client = get_bq_client()
        results = bq_client.query(query).result()
        return [dict(row) for row in results]

    async def fetch_sleep_body_battery():
        query = f"""
            SELECT date, day_abbr_french, sleep_score, battery_at_bedtime, battery_at_waketime,
                   battery_gain, avg_hrv, resting_hr
            FROM `{PROJECT_ID}.{DATASET}.pct_homepage__sleep_body_battery`
            ORDER BY date ASC
        """
        bq_client = get_bq_client()
        results = bq_client.query(query).result()
        return [dict(row) for row in results]

    async def fetch_sleep_stages():
        query = f"""
            SELECT date, start_time, end_time, level_name
            FROM `{PROJECT_ID}.{DATASET}.pct_homepage__sleep_stages`
            ORDER BY start_time
        """
        bq_client = get_bq_client()
        results = bq_client.query(query).result()
        return [dict(row) for row in results]

    async def fetch_top_artists():
        query = f"""
            SELECT rank, artistname, total_duration, play_count, artistexternalurl, albumimageurl, artistid
            FROM `{PROJECT_ID}.{DATASET}.pct_homepage__top_artist`
            ORDER BY rank
        """
        bq_client = get_bq_client()
        results = bq_client.query(query).result()
        return [dict(row) for row in results]

    async def fetch_top_tracks():
        query = f"""
            SELECT rank, trackname, all_artist_names, total_duration, play_count, trackExternalUrl, albumimageurl, trackid
            FROM `{PROJECT_ID}.{DATASET}.pct_homepage__top_track`
            ORDER BY rank
        """
        bq_client = get_bq_client()
        results = bq_client.query(query).result()
        return [dict(row) for row in results]

    async def fetch_vo2max_trend():
        query = f"""
            SELECT current_date, current_vo2max, weekly_vo2max_array, vo2max_delta_6_months
            FROM `{PROJECT_ID}.{DATASET}.pct_homepage__vo2max_trend`
        """
        bq_client = get_bq_client()
        results = bq_client.query(query).result()
        return [dict(row) for row in results]

    try:
        # Exécuter toutes les requêtes en parallèle
        (
            music_time_daily,
            race_predictions,
            running_weekly,
            running_weekly_volume,
            sleep_body_battery,
            sleep_stages,
            top_artists,
            top_tracks,
            vo2max_trend,
        ) = await asyncio.gather(
            fetch_music_time_daily(),
            fetch_race_predictions(),
            fetch_running_weekly(),
            fetch_running_weekly_volume(),
            fetch_sleep_body_battery(),
            fetch_sleep_stages(),
            fetch_top_artists(),
            fetch_top_tracks(),
            fetch_vo2max_trend(),
        )

        return HomepageData(
            music_time_daily=music_time_daily,
            race_predictions=race_predictions,
            running_weekly=running_weekly,
            running_weekly_volume=running_weekly_volume,
            sleep_body_battery=sleep_body_battery,
            sleep_stages=sleep_stages,
            top_artists=top_artists,
            top_tracks=top_tracks,
            vo2max_trend=vo2max_trend,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching data: {str(e)}")
