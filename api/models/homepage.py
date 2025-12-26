# api/models/homepage.py
from pydantic import BaseModel
from typing import Optional, List
from datetime import date, datetime


class MusicTimeDaily(BaseModel):
    date: date
    total_duration_ms: int
    total_duration: str


class RacePrediction(BaseModel):
    distance: str
    current_date: date
    current_time: int
    previous_date: Optional[date] = None
    previous_time: Optional[int] = None
    diff_seconds: Optional[int] = None


class RunningWeekly(BaseModel):
    date: date
    day_of_week: str
    total_distance_km: float
    aerobic_score: float
    anaerobic_score: float


class RunningWeeklyVolume(BaseModel):
    week_start: date
    number_of_runs: int
    total_distance_km: int


class SleepBodyBattery(BaseModel):
    date: date
    day_abbr_french: str
    sleep_score: Optional[int] = None
    battery_at_bedtime: Optional[int] = None
    battery_at_waketime: Optional[int] = None
    battery_gain: Optional[int] = None
    avg_hrv: Optional[int] = None
    resting_hr: Optional[int] = None


class SleepStages(BaseModel):
    date: date
    start_time: datetime
    end_time: datetime
    level_name: str


class TopArtistHomepage(BaseModel):
    rank: int
    artistname: str
    total_duration: str
    play_count: int
    artistexternalurl: Optional[str] = None
    albumimageurl: Optional[str] = None
    artistid: str


class TopTrackHomepage(BaseModel):
    rank: int
    trackname: str
    all_artist_names: str
    total_duration: str
    play_count: int
    trackExternalUrl: Optional[str] = None
    albumimageurl: Optional[str] = None
    trackid: str


class HomepageData(BaseModel):
    """Modèle qui regroupe toutes les données de la homepage"""

    music_time_daily: List[MusicTimeDaily]
    race_predictions: List[RacePrediction]
    running_weekly: List[RunningWeekly]
    running_weekly_volume: List[RunningWeeklyVolume]
    sleep_body_battery: List[SleepBodyBattery]
    sleep_stages: List[SleepStages]
    top_artists: List[TopArtistHomepage]
    top_tracks: List[TopTrackHomepage]
