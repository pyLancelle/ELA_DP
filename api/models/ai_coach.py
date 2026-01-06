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


# =============================================================================
# Context Models
# =============================================================================


class TrainingObjective(BaseModel):
    """Training objective details."""

    type: str = Field(..., description="Objective type: 'race' or 'general_fitness'")
    race_type: Optional[str] = Field(None, description="e.g., 'marathon', 'half_marathon', '10k'")
    race_date: Optional[str] = Field(None, description="Target race date (YYYY-MM-DD)")
    target_time: Optional[str] = Field(None, description="Target finish time (HH:MM:SS)")
    current_level: str = Field(..., description="Current fitness level")
    current_weekly_volume_km: Optional[float] = None


class TrainingConstraints(BaseModel):
    """Training constraints and limitations."""

    weekly_sessions: int = Field(ge=2, le=7, description="Number of sessions per week")
    max_weekly_volume_km: float = Field(ge=10, description="Maximum weekly volume in km")
    unavailable_days: list[str] = Field(default_factory=list, description="Days/times unavailable")
    injury_history: list[str] = Field(default_factory=list, description="Past injuries")
    equipment: list[str] = Field(default_factory=list, description="Available equipment")


class TrainingPreferences(BaseModel):
    """Training preferences."""

    training_style: str = Field(default="structured with flexibility")
    terrain: str = Field(default="mixed road and trail")
    preferred_workout_types: list[str] = Field(default_factory=list)
    avoid: list[str] = Field(default_factory=list)
    long_run_day: Optional[str] = Field(None, description="Preferred long run day")
    hard_session_day: Optional[str] = Field(None, description="Preferred hard session day")


class VolumeDistribution(BaseModel):
    """Volume distribution across training zones."""

    zone_2_pct: float = Field(default=80, ge=0, le=100)
    zone_3_pct: float = Field(default=10, ge=0, le=100)
    zone_4_5_pct: float = Field(default=10, ge=0, le=100)


class WeeklyStructure(BaseModel):
    """Weekly training structure rules."""

    hard_sessions_max: int = Field(default=2, ge=1, le=4)
    recovery_days_min: int = Field(default=1, ge=0, le=3)
    long_run_pct_of_weekly_volume: float = Field(default=30, ge=20, le=40)


class ProgressionRules(BaseModel):
    """Progression and safety rules."""

    weekly_volume_increase_max_pct: float = Field(default=10, ge=5, le=15)
    long_run_increase_max_km: float = Field(default=3, ge=1, le=5)
    consecutive_hard_days_max: int = Field(default=2, ge=1, le=3)


class TrainingPhilosophy(BaseModel):
    """Training philosophy and adaptation rules."""

    volume_distribution: VolumeDistribution = Field(default_factory=VolumeDistribution)
    weekly_structure: WeeklyStructure = Field(default_factory=WeeklyStructure)
    progression_rules: ProgressionRules = Field(default_factory=ProgressionRules)
    adaptation_priorities: list[str] = Field(
        default_factory=lambda: [
            "Sleep quality first - skip hard session if sleep <7h for 3 days",
            "HRV baseline -10% = recovery week",
            "Body Battery <25 at bedtime = reduce intensity next day",
        ]
    )


class ContextUploadRequest(BaseModel):
    """Request model for uploading a training context."""

    context_type: str = Field(..., description="'race_goal' or 'general_training'")
    objective: TrainingObjective
    constraints: TrainingConstraints
    preferences: TrainingPreferences
    training_philosophy: TrainingPhilosophy = Field(default_factory=TrainingPhilosophy)
    notes: Optional[str] = Field(None, description="Additional notes")


class ContextResponse(BaseModel):
    """Response model for context upload."""

    context_id: str
    gcs_path: str
    uploaded_at: str


class ContextMetadata(BaseModel):
    """Context metadata for listing."""

    context_id: str
    gcs_path: str
    created_at: str
    context_type: str
    objective: Optional[str] = None
