# api/models/activities.py
from pydantic import BaseModel, field_validator
from typing import Optional, List, Any
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
