"""
Profile Generator for AI Coach
-------------------------------
Generates runner profiles using Claude AI based on training and health data.
"""

import logging
from datetime import date, datetime, timezone
from typing import Any
from uuid import uuid4

from .anthropic_client import ClaudeResponse, call_claude
from .config import USER_ID
from .data_aggregator import get_full_profile_data
from .prompt_builder import build_profile_system_prompt, build_profile_user_prompt
from .response_parser import ParseError, parse_profile_response

logger = logging.getLogger(__name__)


class ProfileGenerationError(Exception):
    """Exception raised when profile generation fails."""

    def __init__(
        self,
        message: str,
        profile_id: str | None = None,
        cause: Exception | None = None,
    ):
        """
        Initialize ProfileGenerationError.

        Args:
            message: Error message describing what went wrong.
            profile_id: The profile ID that was being generated.
            cause: The underlying exception that caused this error.
        """
        super().__init__(message)
        self.profile_id = profile_id
        self.cause = cause


def generate_runner_profile(
    days: int = 90,
    model: str = "claude-3-haiku-20240307",
) -> dict[str, Any]:
    """
    Generate a comprehensive runner profile using Claude AI.

    Orchestrates the full flow:
    1. Aggregate data from BigQuery
    2. Build prompts
    3. Call Claude API
    4. Parse and validate response
    5. Return enriched profile

    Args:
        days: Number of days of historical data to analyze. Defaults to 90.
        model: Claude model to use. Defaults to Haiku for cost efficiency.

    Returns:
        Dictionary containing:
        - profile_id: Unique identifier
        - user_id: User identifier
        - created_at: Timestamp
        - generation_metadata: Token usage, latency, model info
        - analysis_period: Start/end dates
        - profile: The generated runner profile

    Raises:
        ProfileGenerationError: If generation fails at any step.
    """
    profile_id = str(uuid4())
    logger.info(f"Starting profile generation | profile_id={profile_id} | days={days}")

    try:
        # Step 1: Aggregate data
        logger.info("Step 1: Aggregating data from BigQuery")
        profile_data = get_full_profile_data(days=days)

        # Step 2: Build prompts
        logger.info("Step 2: Building prompts")
        system_prompt = build_profile_system_prompt()
        user_prompt = build_profile_user_prompt(profile_data)

        logger.debug(f"System prompt length: {len(system_prompt)} chars")
        logger.debug(f"User prompt length: {len(user_prompt)} chars")

        # Step 3: Call Claude API
        logger.info(f"Step 3: Calling Claude API | model={model}")
        response: ClaudeResponse = call_claude(
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            use_case="generate_profile",
            model=model,
            max_tokens=4096,
        )

        # Step 4: Parse and validate response
        logger.info("Step 4: Parsing response")
        try:
            profile = parse_profile_response(response.content)
        except ParseError as e:
            logger.error(f"Failed to parse profile response: {e}")
            raise ProfileGenerationError(
                f"Failed to parse Claude response: {e}",
                profile_id=profile_id,
                cause=e,
            )

        # Step 5: Build final result
        logger.info("Step 5: Building final result")
        analysis_end = date.today()
        analysis_start = date.today().replace(
            day=max(1, date.today().day - days)
        ) if days < 31 else date.today().replace(month=max(1, date.today().month - (days // 30)))

        result = {
            "profile_id": profile_id,
            "user_id": USER_ID,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "generation_metadata": {
                "model": response.model,
                "prompt_tokens": response.usage.prompt_tokens,
                "completion_tokens": response.usage.completion_tokens,
                "total_tokens": response.usage.total_tokens,
                "latency_seconds": response.latency_seconds,
            },
            "analysis_period": {
                "days": days,
                "start_date": (date.today() - __import__('datetime').timedelta(days=days)).isoformat(),
                "end_date": date.today().isoformat(),
                "total_activities_analyzed": profile_data.get("activities", {}).get("overall_statistics", {}).get("total_runs", 0),
                "total_nights_analyzed": profile_data.get("health", {}).get("overall_statistics", {}).get("nights_tracked", 0),
            },
            "profile": profile,
        }

        logger.info(
            f"Profile generated successfully | profile_id={profile_id} | "
            f"level={profile.get('runner_level')} | "
            f"tokens={response.usage.total_tokens} | "
            f"latency={response.latency_seconds:.2f}s"
        )

        return result

    except ProfileGenerationError:
        raise
    except Exception as e:
        logger.error(f"Profile generation failed | profile_id={profile_id} | error={e}")
        raise ProfileGenerationError(
            f"Profile generation failed: {e}",
            profile_id=profile_id,
            cause=e,
        )


def get_profile_summary(profile: dict[str, Any]) -> str:
    """
    Get a human-readable summary of a generated profile.

    Args:
        profile: Profile dictionary from generate_runner_profile().

    Returns:
        Formatted string summary.
    """
    p = profile.get("profile", {})
    meta = profile.get("generation_metadata", {})
    period = profile.get("analysis_period", {})

    summary = f"""
=== RUNNER PROFILE SUMMARY ===
Profile ID: {profile.get('profile_id', 'N/A')}
Generated: {profile.get('created_at', 'N/A')}

RUNNER LEVEL: {p.get('runner_level', 'N/A').upper()}
Weekly Volume: {p.get('weekly_volume_km', 0):.1f} km
Runs/Week: {p.get('avg_runs_per_week', 0):.1f}

STRENGTHS: {', '.join(p.get('primary_strengths', []))}
WEAKNESSES: {', '.join(p.get('primary_weaknesses', []))}

RACE PREDICTIONS:
  5K: {p.get('race_predictions', {}).get('5k', 'N/A')}
  10K: {p.get('race_predictions', {}).get('10k', 'N/A')}
  Half Marathon: {p.get('race_predictions', {}).get('half_marathon', 'N/A')}
  Marathon: {p.get('race_predictions', {}).get('marathon', 'N/A')}

RECOVERY:
  Sleep Quality: {p.get('recovery_profile', {}).get('avg_sleep_quality', 'N/A')}
  HRV: {p.get('recovery_profile', {}).get('avg_hrv', 'N/A')} ms
  Resting HR: {p.get('recovery_profile', {}).get('resting_hr', 'N/A')} bpm

TRAINING ANALYSIS:
  Consistency: {p.get('training_analysis', {}).get('consistency', 'N/A')}
  Volume Trend: {p.get('training_analysis', {}).get('volume_trend', 'N/A')}
  Injury Risk: {p.get('training_analysis', {}).get('injury_risk', 'N/A')}

SUMMARY: {p.get('summary', 'N/A')}

--- Generation Stats ---
Analysis Period: {period.get('days', 0)} days
Activities Analyzed: {period.get('total_activities_analyzed', 0)}
Nights Analyzed: {period.get('total_nights_analyzed', 0)}
Model: {meta.get('model', 'N/A')}
Tokens: {meta.get('total_tokens', 0)}
Latency: {meta.get('latency_seconds', 0):.2f}s
"""
    return summary.strip()
