"""
Weekly Plan & Review Generator for AI Coach
--------------------------------------------
Generates weekly training plans and reviews using Claude AI.
"""

import logging
from datetime import date, datetime, timedelta, timezone
from typing import Any
from uuid import uuid4

from .anthropic_client import ClaudeResponse, call_claude
from .config import USER_ID
from .data_aggregator import get_recent_health_snapshot, get_weekly_review_data
from .prompt_builder import (
    build_weekly_plan_system_prompt,
    build_weekly_plan_user_prompt,
    build_weekly_review_system_prompt,
    build_weekly_review_user_prompt,
)
from .response_parser import ParseError, parse_weekly_plan_response, parse_weekly_review_response

logger = logging.getLogger(__name__)


class WeeklyGenerationError(Exception):
    """Exception raised when weekly plan/review generation fails."""

    def __init__(
        self,
        message: str,
        cycle_id: str | None = None,
        week_number: int | None = None,
        cause: Exception | None = None,
    ):
        super().__init__(message)
        self.cycle_id = cycle_id
        self.week_number = week_number
        self.cause = cause


# =============================================================================
# Weekly Plan Generator
# =============================================================================


def generate_weekly_plan(
    cycle_id: str,
    cycle_plan: dict[str, Any],
    week_number: int,
    runner_profile: dict[str, Any],
    previous_review: dict[str, Any] | None = None,
    model: str = "claude-3-haiku-20240307",
) -> dict[str, Any]:
    """
    Generate a detailed weekly training plan using Claude AI.

    Adapts the cycle plan for the specific week based on current health status.

    Args:
        cycle_id: The cycle this week belongs to.
        cycle_plan: The overall cycle plan from generate_cycle_plan().
        week_number: Which week of the cycle (1-indexed).
        runner_profile: Runner's profile.
        previous_review: Optional previous week's review for adaptation.
        model: Claude model to use.

    Returns:
        Dictionary containing:
        - plan_id: Unique identifier
        - cycle_id: Parent cycle
        - week_number: Week number in cycle
        - user_id: User identifier
        - created_at: Timestamp
        - generation_metadata: Token usage, latency
        - plan: The generated weekly plan

    Raises:
        WeeklyGenerationError: If generation fails.
    """
    plan_id = str(uuid4())
    logger.info(
        f"Starting weekly plan generation | cycle_id={cycle_id} | "
        f"week={week_number} | plan_id={plan_id}"
    )

    try:
        # Get recent health data for adaptation
        recent_health = get_recent_health_snapshot(days=7)

        # Build prompts
        system_prompt = build_weekly_plan_system_prompt()
        user_prompt = build_weekly_plan_user_prompt(
            cycle_plan=cycle_plan,
            week_number=week_number,
            runner_profile=runner_profile,
            recent_health=recent_health,
            previous_review=previous_review,
        )

        # Call Claude API
        response: ClaudeResponse = call_claude(
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            use_case="generate_weekly_plan",
            model=model,
            max_tokens=4096,
            cycle_id=cycle_id,
            week_number=week_number,
        )

        # Parse response
        try:
            plan = parse_weekly_plan_response(response.content)
        except ParseError as e:
            logger.error(f"Failed to parse weekly plan response: {e}")
            raise WeeklyGenerationError(
                f"Failed to parse Claude response: {e}",
                cycle_id=cycle_id,
                week_number=week_number,
                cause=e,
            )

        result = {
            "plan_id": plan_id,
            "cycle_id": cycle_id,
            "week_number": week_number,
            "user_id": USER_ID,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "generation_metadata": {
                "model": response.model,
                "prompt_tokens": response.usage.prompt_tokens,
                "completion_tokens": response.usage.completion_tokens,
                "total_tokens": response.usage.total_tokens,
                "latency_seconds": response.latency_seconds,
            },
            "plan": plan,
        }

        logger.info(
            f"Weekly plan generated | cycle_id={cycle_id} | week={week_number} | "
            f"workouts={len(plan.get('daily_workouts', []))} | "
            f"tokens={response.usage.total_tokens}"
        )

        return result

    except WeeklyGenerationError:
        raise
    except Exception as e:
        logger.error(
            f"Weekly plan generation failed | cycle_id={cycle_id} | "
            f"week={week_number} | error={e}"
        )
        raise WeeklyGenerationError(
            f"Weekly plan generation failed: {e}",
            cycle_id=cycle_id,
            week_number=week_number,
            cause=e,
        )


# =============================================================================
# Weekly Review Generator
# =============================================================================


def generate_weekly_review(
    cycle_id: str,
    week_number: int,
    week_start: str,
    week_end: str,
    planned_sessions: list[dict[str, Any]] | None = None,
    runner_context: dict[str, Any] | None = None,
    model: str = "claude-3-haiku-20240307",
) -> dict[str, Any]:
    """
    Generate a weekly training review using Claude AI.

    Analyzes actual training vs planned and provides insights.

    Args:
        cycle_id: The cycle this week belongs to.
        week_number: Which week of the cycle (1-indexed).
        week_start: Start of week (YYYY-MM-DD).
        week_end: End of week (YYYY-MM-DD).
        planned_sessions: Optional list of planned workouts for comparison.
        runner_context: Optional runner context/preferences.
        model: Claude model to use.

    Returns:
        Dictionary containing:
        - review_id: Unique identifier
        - cycle_id: Parent cycle
        - week_number: Week number in cycle
        - user_id: User identifier
        - created_at: Timestamp
        - generation_metadata: Token usage, latency
        - review: The generated review

    Raises:
        WeeklyGenerationError: If generation fails.
    """
    review_id = str(uuid4())
    logger.info(
        f"Starting weekly review generation | cycle_id={cycle_id} | "
        f"week={week_number} | period={week_start} to {week_end}"
    )

    try:
        # Get actual training data for the week
        week_data = get_weekly_review_data(week_start, week_end)

        # Build prompts
        system_prompt = build_weekly_review_system_prompt()
        user_prompt = build_weekly_review_user_prompt(
            week_data=week_data,
            planned_sessions=planned_sessions,
            runner_context=runner_context,
        )

        # Call Claude API
        response: ClaudeResponse = call_claude(
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            use_case="generate_weekly_review",
            model=model,
            max_tokens=4096,
            cycle_id=cycle_id,
            week_number=week_number,
        )

        # Parse response
        try:
            review = parse_weekly_review_response(response.content)
        except ParseError as e:
            logger.error(f"Failed to parse weekly review response: {e}")
            raise WeeklyGenerationError(
                f"Failed to parse Claude response: {e}",
                cycle_id=cycle_id,
                week_number=week_number,
                cause=e,
            )

        result = {
            "review_id": review_id,
            "cycle_id": cycle_id,
            "week_number": week_number,
            "user_id": USER_ID,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "week_period": {
                "start": week_start,
                "end": week_end,
            },
            "generation_metadata": {
                "model": response.model,
                "prompt_tokens": response.usage.prompt_tokens,
                "completion_tokens": response.usage.completion_tokens,
                "total_tokens": response.usage.total_tokens,
                "latency_seconds": response.latency_seconds,
            },
            "review": review,
        }

        logger.info(
            f"Weekly review generated | cycle_id={cycle_id} | week={week_number} | "
            f"tokens={response.usage.total_tokens}"
        )

        return result

    except WeeklyGenerationError:
        raise
    except Exception as e:
        logger.error(
            f"Weekly review generation failed | cycle_id={cycle_id} | "
            f"week={week_number} | error={e}"
        )
        raise WeeklyGenerationError(
            f"Weekly review generation failed: {e}",
            cycle_id=cycle_id,
            week_number=week_number,
            cause=e,
        )
