"""
Cycle Plan Generator for AI Coach
----------------------------------
Generates multi-week training cycle plans using Claude AI.
"""

import logging
from datetime import date, datetime, timedelta, timezone
from typing import Any
from uuid import uuid4

from .anthropic_client import ClaudeResponse, call_claude
from .config import USER_ID
from .prompt_builder import build_cycle_plan_system_prompt, build_cycle_plan_user_prompt
from .response_parser import ParseError, parse_cycle_plan_response

logger = logging.getLogger(__name__)


class CycleGenerationError(Exception):
    """Exception raised when cycle plan generation fails."""

    def __init__(
        self,
        message: str,
        cycle_id: str | None = None,
        cause: Exception | None = None,
    ):
        super().__init__(message)
        self.cycle_id = cycle_id
        self.cause = cause


def generate_cycle_id(goal: str, start_date: str) -> str:
    """
    Generate a human-readable cycle ID.

    Args:
        goal: The training goal (e.g., "marathon", "10k", "base_building").
        start_date: Start date in YYYY-MM-DD format.

    Returns:
        Cycle ID like "cycle_2025_01_marathon"
    """
    date_part = start_date[:7].replace("-", "_")  # "2025-01" -> "2025_01"
    goal_clean = goal.lower().replace(" ", "_").replace("-", "_")[:20]
    return f"cycle_{date_part}_{goal_clean}"


def generate_cycle_plan(
    runner_profile: dict[str, Any],
    goal: str,
    start_date: str | None = None,
    total_weeks: int = 12,
    constraints: dict[str, Any] | None = None,
    target_race_date: str | None = None,
    model: str = "claude-3-haiku-20240307",
) -> dict[str, Any]:
    """
    Generate a multi-week training cycle plan using Claude AI.

    Args:
        runner_profile: Runner's profile from generate_runner_profile().
        goal: Training goal (e.g., "marathon PR", "first 10k", "base building").
        start_date: Cycle start date (YYYY-MM-DD). Defaults to next Monday.
        total_weeks: Number of weeks in the cycle. Defaults to 12.
        constraints: Optional dict with user constraints like:
            - max_days_per_week: int
            - long_run_day: str
            - blocked_days: list[str]
        target_race_date: Optional target race date (YYYY-MM-DD).
        model: Claude model to use.

    Returns:
        Dictionary containing:
        - cycle_id: Unique identifier for this cycle
        - user_id: User identifier
        - created_at: Timestamp
        - generation_metadata: Token usage, latency, model info
        - cycle_config: Input parameters
        - plan: The generated cycle plan

    Raises:
        CycleGenerationError: If generation fails at any step.
    """
    # Default start date to next Monday
    if start_date is None:
        today = date.today()
        days_until_monday = (7 - today.weekday()) % 7
        if days_until_monday == 0:
            days_until_monday = 7
        start_date = (today + timedelta(days=days_until_monday)).isoformat()

    cycle_id = generate_cycle_id(goal, start_date)
    logger.info(f"Starting cycle plan generation | cycle_id={cycle_id} | weeks={total_weeks}")

    try:
        # Build context for the prompt
        context = {
            "goal": goal,
            "constraints": constraints or {},
            "target_race_date": target_race_date,
        }

        # Build prompts
        system_prompt = build_cycle_plan_system_prompt()
        user_prompt = build_cycle_plan_user_prompt(
            runner_profile=runner_profile,
            context=context,
            cycle_start_date=start_date,
            total_weeks=total_weeks,
        )

        # Call Claude API
        logger.info(f"Calling Claude API for cycle plan | model={model}")
        response: ClaudeResponse = call_claude(
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            use_case="generate_cycle_plan",
            model=model,
            max_tokens=8192,  # Cycle plans can be large
            cycle_id=cycle_id,
        )

        # Parse response
        try:
            plan = parse_cycle_plan_response(response.content)
        except ParseError as e:
            logger.error(f"Failed to parse cycle plan response: {e}")
            raise CycleGenerationError(
                f"Failed to parse Claude response: {e}",
                cycle_id=cycle_id,
                cause=e,
            )

        # Build result
        end_date = (
            date.fromisoformat(start_date) + timedelta(weeks=total_weeks)
        ).isoformat()

        result = {
            "cycle_id": cycle_id,
            "user_id": USER_ID,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "generation_metadata": {
                "model": response.model,
                "prompt_tokens": response.usage.prompt_tokens,
                "completion_tokens": response.usage.completion_tokens,
                "total_tokens": response.usage.total_tokens,
                "latency_seconds": response.latency_seconds,
            },
            "cycle_config": {
                "goal": goal,
                "start_date": start_date,
                "end_date": end_date,
                "total_weeks": total_weeks,
                "constraints": constraints,
                "target_race_date": target_race_date,
            },
            "plan": plan,
        }

        logger.info(
            f"Cycle plan generated successfully | cycle_id={cycle_id} | "
            f"weeks={len(plan.get('weekly_summaries', []))} | "
            f"tokens={response.usage.total_tokens}"
        )

        return result

    except CycleGenerationError:
        raise
    except Exception as e:
        logger.error(f"Cycle plan generation failed | cycle_id={cycle_id} | error={e}")
        raise CycleGenerationError(
            f"Cycle plan generation failed: {e}",
            cycle_id=cycle_id,
            cause=e,
        )
