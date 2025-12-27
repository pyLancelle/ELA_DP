# api/models/activities.py
from pydantic import BaseModel
from typing import Optional
from datetime import datetime


class RecentActivity(BaseModel):
    activityId: int
    activityName: str
    startTimeGMT: datetime
    distance_km: float
    duration_minutes: int
    averageSpeed: Optional[float] = None
    typeKey: Optional[str] = None
