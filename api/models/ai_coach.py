# api/models/ai_coach.py
"""Pydantic models for AI Coach endpoints."""

from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime


# =============================================================================
# Profile Models
# =============================================================================


class ProfileRequest(BaseModel):
    """Request model for profile generation."""

    days: int = Field(
        default=90,
        ge=7,
        le=365,
        description="Number of days of historical data to analyze",
    )
    model: str = Field(
        default="claude-3-haiku-20240307",
        description="Claude model to use for generation",
    )


class TrainingZone(BaseModel):
    """Heart rate and pace zone."""

    hr_range: str
    pace_range: str


class TrainingZones(BaseModel):
    """All training zones."""

    zone1_recovery: TrainingZone
    zone2_aerobic: TrainingZone
    zone3_tempo: TrainingZone
    zone4_threshold: TrainingZone
    zone5_vo2max: TrainingZone


class RacePredictions(BaseModel):
    """Predicted race times."""

    five_k: Optional[str] = Field(None, alias="5k")
    ten_k: Optional[str] = Field(None, alias="10k")
    half_marathon: Optional[str] = None
    marathon: Optional[str] = None

    class Config:
        populate_by_name = True


class RecoveryProfile(BaseModel):
    """Recovery metrics analysis."""

    avg_sleep_quality: str
    avg_hrv: float
    hrv_trend: str
    recovery_capacity: str
    resting_hr: int
    resting_hr_trend: str


class IntensityDistribution(BaseModel):
    """Training intensity breakdown."""

    easy_pct: float
    moderate_pct: float
    hard_pct: float


class TrainingAnalysis(BaseModel):
    """Training pattern analysis."""

    consistency: str
    intensity_distribution: IntensityDistribution
    volume_trend: str
    injury_risk: str
    overtraining_risk: str


class Recommendations(BaseModel):
    """Coach recommendations."""

    immediate_focus: list[str]
    training_adjustments: list[str]
    recovery_tips: list[str]


class RunnerProfile(BaseModel):
    """Generated runner profile from Claude."""

    runner_level: str
    weekly_volume_km: float
    avg_runs_per_week: float
    vo2_max_estimate: Optional[float] = None
    primary_strengths: list[str]
    primary_weaknesses: list[str]
    training_zones: TrainingZones
    race_predictions: RacePredictions
    recovery_profile: RecoveryProfile
    training_analysis: TrainingAnalysis
    recommendations: Recommendations
    summary: str


class GenerationMetadata(BaseModel):
    """Metadata about the Claude API call."""

    model: str
    prompt_tokens: int
    completion_tokens: int
    total_tokens: int
    latency_seconds: float


class AnalysisPeriod(BaseModel):
    """Period of data analyzed."""

    days: int
    start_date: str
    end_date: str
    total_activities_analyzed: int
    total_nights_analyzed: int


class ProfileResponse(BaseModel):
    """Response model for profile generation."""

    profile_id: str
    user_id: str
    created_at: datetime
    generation_metadata: GenerationMetadata
    analysis_period: AnalysisPeriod
    profile: RunnerProfile


class ProfileSummaryResponse(BaseModel):
    """Simplified profile summary response."""

    profile_id: str
    runner_level: str
    weekly_volume_km: float
    primary_strengths: list[str]
    primary_weaknesses: list[str]
    summary: str
    created_at: datetime
