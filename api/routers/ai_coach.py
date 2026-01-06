# api/routers/ai_coach.py
"""API router for AI Coach endpoints."""

import logging
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from typing import Any

from api.models.ai_coach import (
    ProfileRequest,
    ProfileResponse,
    ContextUploadRequest,
    ContextResponse,
    ContextMetadata,
)
from src.services.ai_coach.profile_generator import (
    generate_runner_profile,
    ProfileGenerationError,
)
from src.services.ai_coach.cycle_generator import (
    generate_cycle_plan,
    CycleGenerationError,
)
from src.services.ai_coach.weekly_generator import (
    generate_weekly_plan,
    generate_weekly_review,
    WeeklyGenerationError,
)
from src.services.ai_coach.gcs_manager import (
    upload_context,
    get_context,
    list_contexts,
    GCSUploadError,
    GCSRetrievalError,
)

logger = logging.getLogger(__name__)

router = APIRouter()


# =============================================================================
# Context Management Endpoints
# =============================================================================


@router.post("/context", response_model=ContextResponse)
async def upload_training_context(request: ContextUploadRequest):
    """
    Upload a training context document to GCS.

    The context includes objectives, constraints, preferences, and training philosophy.
    This document will be used to generate personalized training plans.

    **Example request**:
    ```json
    {
      "context_type": "race_goal",
      "objective": {
        "type": "race",
        "race_type": "marathon",
        "race_date": "2026-04-20",
        "target_time": "03:30:00",
        "current_level": "intermediate",
        "current_weekly_volume_km": 45
      },
      "constraints": {
        "weekly_sessions": 4,
        "max_weekly_volume_km": 80,
        "unavailable_days": ["Sunday morning before 9am"],
        "injury_history": ["IT band syndrome - recovered 2024"],
        "equipment": ["Garmin Forerunner 965", "HRM-Pro strap"]
      },
      "preferences": {
        "training_style": "structured with flexibility",
        "terrain": "mixed road and trail",
        "preferred_workout_types": ["tempo runs", "long runs", "intervals"],
        "avoid": ["track workouts", "very early morning runs"],
        "long_run_day": "Saturday",
        "hard_session_day": "Wednesday"
      },
      "training_philosophy": {
        "volume_distribution": {
          "zone_2_pct": 80,
          "zone_3_pct": 10,
          "zone_4_5_pct": 10
        },
        "weekly_structure": {
          "hard_sessions_max": 2,
          "recovery_days_min": 1,
          "long_run_pct_of_weekly_volume": 30
        },
        "progression_rules": {
          "weekly_volume_increase_max_pct": 10,
          "long_run_increase_max_km": 3,
          "consecutive_hard_days_max": 2
        },
        "adaptation_priorities": [
          "Sleep quality first - skip hard session if sleep <7h for 3 days",
          "HRV baseline -10% = recovery week"
        ]
      },
      "notes": "Prefer progressive long runs. Recovery runs should be truly easy."
    }
    ```
    """
    logger.info(f"Context upload requested | type={request.context_type}")

    try:
        # Convert Pydantic models to dict
        context_data = {
            "objective": request.objective.model_dump(),
            "constraints": request.constraints.model_dump(),
            "preferences": request.preferences.model_dump(),
            "training_philosophy": request.training_philosophy.model_dump(),
            "notes": request.notes,
        }

        result = upload_context(
            context_data=context_data,
            context_type=request.context_type,
        )

        logger.info(f"Context uploaded successfully | context_id={result['context_id']}")
        return result

    except GCSUploadError as e:
        logger.error(f"Context upload failed: {e}")
        raise HTTPException(
            status_code=500,
            detail={
                "error": "Context upload failed",
                "message": str(e),
            },
        )
    except Exception as e:
        logger.error(f"Unexpected error during context upload: {e}")
        raise HTTPException(
            status_code=500,
            detail={
                "error": "Unexpected error",
                "message": str(e),
            },
        )


@router.get("/contexts", response_model=list[ContextMetadata])
async def list_training_contexts(limit: int = 10):
    """
    List recent training contexts for the user.

    Returns metadata about uploaded contexts (most recent first).
    """
    logger.info(f"Context listing requested | limit={limit}")

    try:
        contexts = list_contexts(limit=limit)
        logger.info(f"Listed {len(contexts)} contexts")
        return contexts

    except GCSRetrievalError as e:
        logger.error(f"Context listing failed: {e}")
        raise HTTPException(
            status_code=500,
            detail={
                "error": "Context listing failed",
                "message": str(e),
            },
        )
    except Exception as e:
        logger.error(f"Unexpected error during context listing: {e}")
        raise HTTPException(
            status_code=500,
            detail={
                "error": "Unexpected error",
                "message": str(e),
            },
        )


@router.get("/context/{context_id}")
async def get_training_context(context_id: str):
    """
    Retrieve a specific training context by ID.

    **Note**: This searches through recent contexts to find the matching ID.
    For better performance in production, consider storing context_id → gcs_path mapping in BigQuery.
    """
    logger.info(f"Context retrieval requested | context_id={context_id}")

    try:
        # List contexts and find the one with matching ID
        contexts = list_contexts(limit=50)
        matching_context = next((c for c in contexts if c["context_id"] == context_id), None)

        if not matching_context:
            raise HTTPException(
                status_code=404,
                detail={
                    "error": "Context not found",
                    "message": f"No context found with ID {context_id}",
                },
            )

        # Retrieve full context from GCS
        context_doc = get_context(matching_context["gcs_path"])
        logger.info(f"Context retrieved | context_id={context_id}")
        return context_doc

    except HTTPException:
        raise
    except GCSRetrievalError as e:
        logger.error(f"Context retrieval failed: {e}")
        raise HTTPException(
            status_code=500,
            detail={
                "error": "Context retrieval failed",
                "message": str(e),
            },
        )
    except Exception as e:
        logger.error(f"Unexpected error during context retrieval: {e}")
        raise HTTPException(
            status_code=500,
            detail={
                "error": "Unexpected error",
                "message": str(e),
            },
        )


# =============================================================================
# Profile Endpoints
# =============================================================================


@router.post("/profile", response_model=ProfileResponse)
async def create_profile(request: ProfileRequest = ProfileRequest()):
    """
    Generate a comprehensive runner profile using Claude AI.

    Analyzes historical training and health data to create a personalized
    runner profile including:
    - Runner level assessment (beginner/intermediate/advanced/elite)
    - Training zones (HR and pace)
    - Race predictions
    - Strengths and weaknesses
    - Recovery analysis
    - Personalized recommendations

    **Note**: This endpoint calls Claude API and may take 5-15 seconds.
    """
    logger.info(f"Profile generation requested: days={request.days}, model={request.model}")

    try:
        result = generate_runner_profile(
            days=request.days,
            model=request.model,
        )
        return result

    except ProfileGenerationError as e:
        logger.error(f"Profile generation failed: {e}")
        raise HTTPException(
            status_code=500,
            detail={
                "error": "Profile generation failed",
                "message": str(e),
                "profile_id": e.profile_id,
            },
        )
    except Exception as e:
        logger.error(f"Unexpected error during profile generation: {e}")
        raise HTTPException(
            status_code=500,
            detail={
                "error": "Unexpected error",
                "message": str(e),
            },
        )


# =============================================================================
# Cycle Plan Endpoints
# =============================================================================


class CycleRequest(BaseModel):
    """Request model for cycle plan generation."""

    goal: str = Field(..., description="Training goal (e.g., 'marathon PR', 'first 10k')")
    start_date: str | None = Field(None, description="Start date (YYYY-MM-DD), defaults to next Monday")
    total_weeks: int = Field(default=12, ge=4, le=24, description="Number of weeks")
    target_race_date: str | None = Field(None, description="Target race date (YYYY-MM-DD)")
    constraints: dict[str, Any] | None = Field(None, description="Training constraints")
    model: str = Field(default="claude-3-haiku-20240307")


@router.post("/cycle")
async def create_cycle(request: CycleRequest):
    """
    Generate a multi-week training cycle plan.

    Creates a periodized training plan based on runner profile and goals.
    Requires a profile to be generated first.

    **Note**: This endpoint calls Claude API and may take 10-20 seconds.
    """
    logger.info(f"Cycle generation requested: goal={request.goal}, weeks={request.total_weeks}")

    try:
        # First generate a fresh profile
        profile_result = generate_runner_profile(days=90, model=request.model)
        runner_profile = profile_result["profile"]

        result = generate_cycle_plan(
            runner_profile=runner_profile,
            goal=request.goal,
            start_date=request.start_date,
            total_weeks=request.total_weeks,
            constraints=request.constraints,
            target_race_date=request.target_race_date,
            model=request.model,
        )
        return result

    except (ProfileGenerationError, CycleGenerationError) as e:
        logger.error(f"Cycle generation failed: {e}")
        raise HTTPException(
            status_code=500,
            detail={
                "error": "Cycle generation failed",
                "message": str(e),
                "cycle_id": getattr(e, "cycle_id", None),
            },
        )
    except Exception as e:
        logger.error(f"Unexpected error during cycle generation: {e}")
        raise HTTPException(status_code=500, detail={"error": "Unexpected error", "message": str(e)})


# =============================================================================
# Weekly Plan Endpoints
# =============================================================================


class WeeklyPlanRequest(BaseModel):
    """Request model for weekly plan generation."""

    cycle_id: str = Field(..., description="The cycle this week belongs to")
    cycle_plan: dict[str, Any] = Field(..., description="The full cycle plan")
    week_number: int = Field(..., ge=1, description="Week number in the cycle")
    runner_profile: dict[str, Any] = Field(..., description="Runner profile")
    previous_review: dict[str, Any] | None = Field(None, description="Previous week's review")
    model: str = Field(default="claude-3-haiku-20240307")


@router.post("/weekly-plan")
async def create_weekly_plan(request: WeeklyPlanRequest):
    """
    Generate a detailed weekly training plan.

    Adapts the cycle plan for the specific week based on current health status.

    **Note**: This endpoint calls Claude API and may take 5-15 seconds.
    """
    logger.info(f"Weekly plan requested: cycle_id={request.cycle_id}, week={request.week_number}")

    try:
        result = generate_weekly_plan(
            cycle_id=request.cycle_id,
            cycle_plan=request.cycle_plan,
            week_number=request.week_number,
            runner_profile=request.runner_profile,
            previous_review=request.previous_review,
            model=request.model,
        )
        return result

    except WeeklyGenerationError as e:
        logger.error(f"Weekly plan generation failed: {e}")
        raise HTTPException(
            status_code=500,
            detail={
                "error": "Weekly plan generation failed",
                "message": str(e),
                "cycle_id": e.cycle_id,
                "week_number": e.week_number,
            },
        )
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        raise HTTPException(status_code=500, detail={"error": "Unexpected error", "message": str(e)})


# =============================================================================
# Weekly Review Endpoints
# =============================================================================


class WeeklyReviewRequest(BaseModel):
    """Request model for weekly review generation."""

    cycle_id: str = Field(..., description="The cycle this week belongs to")
    week_number: int = Field(..., ge=1, description="Week number in the cycle")
    week_start: str = Field(..., description="Start of week (YYYY-MM-DD)")
    week_end: str = Field(..., description="End of week (YYYY-MM-DD)")
    planned_sessions: list[dict[str, Any]] | None = Field(None, description="Planned workouts")
    model: str = Field(default="claude-3-haiku-20240307")


@router.post("/weekly-review")
async def create_weekly_review(request: WeeklyReviewRequest):
    """
    Generate a weekly training review.

    Analyzes actual training vs planned and provides insights for next week.

    **Note**: This endpoint calls Claude API and may take 5-15 seconds.
    """
    logger.info(
        f"Weekly review requested: cycle_id={request.cycle_id}, "
        f"week={request.week_number}, period={request.week_start} to {request.week_end}"
    )

    try:
        result = generate_weekly_review(
            cycle_id=request.cycle_id,
            week_number=request.week_number,
            week_start=request.week_start,
            week_end=request.week_end,
            planned_sessions=request.planned_sessions,
            model=request.model,
        )
        return result

    except WeeklyGenerationError as e:
        logger.error(f"Weekly review generation failed: {e}")
        raise HTTPException(
            status_code=500,
            detail={
                "error": "Weekly review generation failed",
                "message": str(e),
                "cycle_id": e.cycle_id,
                "week_number": e.week_number,
            },
        )
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        raise HTTPException(status_code=500, detail={"error": "Unexpected error", "message": str(e)})
