# api/models/homepage.py
from pydantic import BaseModel
from typing import Optional, List
from datetime import date, datetime


class MusicTimeDailyItem(BaseModel):
    date: date
    total_duration_ms: int
    bar_height_percent: float
    day_letter: str
    duration_formatted: str


class MusicTimeDaily(BaseModel):
    data: List[MusicTimeDailyItem]
    avg_duration_formatted: str


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


class Vo2MaxTrend(BaseModel):
    current_date: date
    current_vo2max: float
    weekly_vo2max_array: List[float]
    vo2max_delta_6_months: float


class SleepScoreDaily(BaseModel):
    date: str
    day: str
    score: int


class SleepScores(BaseModel):
    average: int
    daily: List[SleepScoreDaily]


class BodyBatteryDaily(BaseModel):
    date: str
    day: str
    bedtime: int
    waketime: int
    gain: int


class BodyBattery(BaseModel):
    average_gain: int
    daily: List[BodyBatteryDaily]


class HrvDaily(BaseModel):
    date: str
    day: str
    value: int
    is_above_baseline: bool
    display_height_percent: float


class Hrv(BaseModel):
    average: int
    baseline: int
    daily: List[HrvDaily]


class RestingHrDaily(BaseModel):
    date: str
    day: str
    value: int
    display_height_percent: float


class RestingHr(BaseModel):
    average: int
    daily: List[RestingHrDaily]


class StepsDaily(BaseModel):
    date: str
    day: str
    steps: int


class Steps(BaseModel):
    average: int
    goal: int
    daily: List[StepsDaily]


class StressDailyItem(BaseModel):
    date: str
    day: str
    avg_stress: int
    max_stress: int


class StressDaily(BaseModel):
    average_stress: int
    daily: List[StressDailyItem]


class HomepageData(BaseModel):
    """Modèle qui regroupe toutes les données de la homepage"""

    music_time_daily: Optional[MusicTimeDaily] = None
    race_predictions: List[RacePrediction]
    running_weekly: List[RunningWeekly]
    running_weekly_volume: List[RunningWeeklyVolume]
    sleep_stages: List[SleepStages]
    top_artists: List[TopArtistHomepage]
    top_tracks: List[TopTrackHomepage]
    vo2max_trend: Optional[Vo2MaxTrend] = None
    sleep_scores: Optional[SleepScores] = None
    body_battery: Optional[BodyBattery] = None
    hrv: Optional[Hrv] = None
    resting_hr: Optional[RestingHr] = None
    steps: Optional[Steps] = None
    stress_daily: Optional[StressDaily] = None
