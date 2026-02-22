# api/models/activities.py
from pydantic import BaseModel, field_validator
from typing import Optional, List, Any, Dict
from datetime import datetime
import json


class RecentActivity(BaseModel):
    activityId: int
    activityName: str
    startTimeGMT: datetime
    distance_km: float
    duration_minutes: int
    averageSpeed: Optional[float] = None
    typeKey: Optional[str] = None


class ActivityListItem(BaseModel):
    activityId: int
    activityName: str
    startTimeGMT: datetime
    typeKey: Optional[str] = None
    distance_km: float
    duration_minutes: float
    averageHR: Optional[int] = None
    hrZone1_pct: Optional[int] = None
    hrZone2_pct: Optional[int] = None
    hrZone3_pct: Optional[int] = None
    hrZone4_pct: Optional[int] = None
    hrZone5_pct: Optional[int] = None
    # JSON string depuis BQ → désérialisé en liste de dicts {lat, lng}
    polyline_simplified: Optional[List[Any]] = None

    @field_validator("polyline_simplified", mode="before")
    @classmethod
    def parse_polyline(cls, v):
        if v is None:
            return None
        if isinstance(v, str):
            return json.loads(v)
        return v


class TrainingInterval(BaseModel):
    lapIndex: Optional[int] = None
    startTimeGMT: Optional[datetime] = None
    distance: Optional[float] = None       # mètres
    duration: Optional[float] = None       # secondes
    averageSpeed: Optional[float] = None   # m/s
    calories: Optional[int] = None
    averageHR: Optional[float] = None
    maxHR: Optional[float] = None
    elevationGain: Optional[float] = None
    elevationLoss: Optional[float] = None
    intensityType: Optional[str] = None    # warmup | work | recovery | cooldown | rest
    name: Optional[str] = None


class TimeSeriesPoint(BaseModel):
    timestamp: Optional[int] = None        # secondes depuis le début
    distance: Optional[float] = None       # km cumulés
    heartRate: Optional[int] = None        # bpm
    pace: Optional[float] = None           # min/km
    altitude: Optional[float] = None       # mètres
    speed: Optional[float] = None          # km/h


class GpsCoordinate(BaseModel):
    lat: float
    lng: float


class HRZones(BaseModel):
    hrTimeInZone_1: Optional[float] = None
    hrTimeInZone_2: Optional[float] = None
    hrTimeInZone_3: Optional[float] = None
    hrTimeInZone_4: Optional[float] = None
    hrTimeInZone_5: Optional[float] = None


class FastestSplits(BaseModel):
    fastestSplit_1000: Optional[float] = None
    fastestSplit_1609: Optional[float] = None
    fastestSplit_5000: Optional[float] = None
    fastestSplit_10000: Optional[float] = None
    fastestSplit_21098: Optional[float] = None
    fastestSplit_42195: Optional[float] = None


class ActivityDetail(BaseModel):
    # Identité
    activityId: int
    activityName: str
    startTimeGMT: datetime
    endTimeGMT: Optional[datetime] = None
    typeKey: Optional[str] = None
    # Métriques de base
    distance: Optional[float] = None
    duration: Optional[float] = None
    elapsedDuration: Optional[float] = None
    elevationGain: Optional[float] = None
    elevationLoss: Optional[float] = None
    minElevation: Optional[float] = None
    maxElevation: Optional[float] = None
    averageSpeed: Optional[float] = None
    averageHR: Optional[float] = None
    maxHR: Optional[float] = None
    calories: Optional[int] = None
    hasPolyline: Optional[bool] = None
    # Scores d'entraînement
    aerobicTrainingEffect: Optional[float] = None
    anaerobicTrainingEffect: Optional[float] = None
    activityTrainingLoad: Optional[float] = None
    # Structs
    fastestSplits: Optional[Dict[str, Any]] = None
    hr_zones: Optional[Dict[str, Any]] = None
    power_zones: Optional[Dict[str, Any]] = None
    # Arrays enrichis
    kilometer_laps: Optional[List[Dict[str, Any]]] = None
    training_intervals: Optional[List[TrainingInterval]] = None
    time_series: Optional[List[TimeSeriesPoint]] = None
    # GPS — coordonnées brutes depuis la polyline (lat/lng pour RunMap)
    coordinates: Optional[List[GpsCoordinate]] = None
    # Musique
    tracks_played: Optional[List[Dict[str, Any]]] = None
