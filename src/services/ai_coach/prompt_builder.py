"""
Prompt Builder for AI Coach
---------------------------
Builds structured prompts for Claude API calls.
Loads system prompts from external files for easy editing.
"""

import json
from pathlib import Path
from typing import Any

# Path to prompts directory
PROMPTS_DIR = Path(__file__).parent / "prompts"


def _load_prompt(filename: str) -> str:
    """
    Load a prompt from a text file.

    Args:
        filename: Name of the prompt file (e.g., "profile_system.txt").

    Returns:
        Content of the prompt file.

    Raises:
        FileNotFoundError: If the prompt file doesn't exist.
    """
    filepath = PROMPTS_DIR / filename
    return filepath.read_text(encoding="utf-8")


# =============================================================================
# Profile Generation Prompts
# =============================================================================


def build_profile_system_prompt() -> str:
    """
    Build the system prompt for runner profile generation.

    Returns:
        System prompt string with instructions and JSON schema.
    """
    return _load_prompt("profile_system.md")


def build_profile_user_prompt(profile_data: dict[str, Any]) -> str:
    """
    Build the user prompt with runner data for profile generation.

    Args:
        profile_data: Dictionary containing activity and health data
                     from get_full_profile_data().

    Returns:
        User prompt string with formatted data.
    """
    activities = profile_data.get("activities", {})
    health = profile_data.get("health", {})

    activity_stats = activities.get("overall_statistics", {})
    health_stats = health.get("overall_statistics", {})

    prompt = f"""Analyze the following runner data and generate a comprehensive profile.

=== ANALYSIS PERIOD ===
Period: {profile_data.get('analysis_period_days', 90)} days
Generated: {profile_data.get('generated_at', 'unknown')}

=== ACTIVITY OVERVIEW ({activity_stats.get('total_runs', 0)} runs) ===
Total Distance: {activity_stats.get('total_distance_km', 0)} km
Average Distance: {activity_stats.get('avg_distance_km', 0)} km
Longest Run: {activity_stats.get('max_distance_km', 0)} km
Total Duration: {activity_stats.get('total_duration_hours', 0)} hours
Average Pace: {activity_stats.get('avg_pace_min_per_km', 0)} min/km
Best Pace (runs >5km): {activity_stats.get('best_pace_min_per_km', 'N/A')} min/km
Average HR: {activity_stats.get('avg_hr', 0)} bpm
Average Max HR: {activity_stats.get('avg_max_hr', 0)} bpm
Total Elevation: {activity_stats.get('total_elevation_m', 0)} m
Avg Aerobic TE: {activity_stats.get('avg_aerobic_te', 0)}
Avg Anaerobic TE: {activity_stats.get('avg_anaerobic_te', 0)}
Total Training Load: {activity_stats.get('total_training_load', 0)}
Avg Training Load/Run: {activity_stats.get('avg_training_load', 0)}

=== HEALTH OVERVIEW ({health_stats.get('nights_tracked', 0)} nights) ===
Avg Sleep Score: {health_stats.get('avg_sleep_score', 0)}
Sleep Score Range: {health_stats.get('min_sleep_score', 0)} - {health_stats.get('max_sleep_score', 0)}
Avg Sleep Duration: {health_stats.get('avg_sleep_hours', 0)} hours
Avg Deep Sleep: {health_stats.get('avg_deep_sleep_pct', 0)}%
Avg REM Sleep: {health_stats.get('avg_rem_sleep_pct', 0)}%
Avg Body Battery Recovery: {health_stats.get('avg_bb_recovery', 0)}
Avg HRV: {health_stats.get('avg_hrv', 0)} ms
HRV Range: {health_stats.get('min_hrv', 0)} - {health_stats.get('max_hrv', 0)} ms
Avg Resting HR: {health_stats.get('avg_resting_hr', 0)} bpm
Min Resting HR: {health_stats.get('min_resting_hr', 0)} bpm
Avg Stress: {health_stats.get('avg_stress', 0)}
Sleep Quality Distribution:
  - Excellent nights: {health_stats.get('excellent_nights', 0)}
  - Good nights: {health_stats.get('good_nights', 0)}
  - Fair nights: {health_stats.get('fair_nights', 0)}
  - Poor nights: {health_stats.get('poor_nights', 0)}

=== RECENT 7 DAYS - DETAILED ACTIVITIES ===
{json.dumps(activities.get('recent_7_days', []), indent=2, default=str)}

=== RECENT 7 DAYS - DETAILED HEALTH ===
{json.dumps(health.get('recent_7_days', []), indent=2, default=str)}

=== DAYS 8-30 - ACTIVITY SUMMARY ===
{json.dumps(activities.get('days_8_to_30', []), indent=2, default=str)}

=== DAYS 8-30 - HEALTH SUMMARY ===
{json.dumps(health.get('days_8_to_30', []), indent=2, default=str)}

=== WEEKLY AGGREGATES (DAYS 31-90) - ACTIVITIES ===
{json.dumps(activities.get('weekly_aggregates_31_to_90', []), indent=2, default=str)}

=== WEEKLY AGGREGATES (DAYS 31-90) - HEALTH ===
{json.dumps(health.get('weekly_aggregates_31_to_90', []), indent=2, default=str)}

Based on this data, generate a complete runner profile in the exact JSON format specified."""

    return prompt


# =============================================================================
# Weekly Review Prompts
# =============================================================================


def build_weekly_review_system_prompt() -> str:
    """
    Build the system prompt for weekly review generation.

    Returns:
        System prompt string with instructions and JSON schema.
    """
    return _load_prompt("weekly_review_system.md")


def build_weekly_review_user_prompt(
    week_data: dict[str, Any],
    planned_sessions: list[dict[str, Any]] | None = None,
    runner_context: dict[str, Any] | None = None,
) -> str:
    """
    Build the user prompt for weekly review generation.

    Args:
        week_data: Dictionary from get_weekly_review_data().
        planned_sessions: Optional list of planned workouts for the week.
        runner_context: Optional runner context/preferences.

    Returns:
        User prompt string with formatted data.
    """
    activity_summary = week_data.get("activity_summary", {})
    health_summary = week_data.get("health_summary", {})

    prompt = f"""Analyze this runner's week and generate a comprehensive review.

=== WEEK OVERVIEW ===
Period: {week_data.get('week_start')} to {week_data.get('week_end')}

=== ACTIVITY SUMMARY ===
Total Runs: {activity_summary.get('total_runs', 0)}
Total Distance: {activity_summary.get('total_distance_km', 0)} km
Total Duration: {activity_summary.get('total_duration_hours', 0)} hours
Average Pace: {activity_summary.get('avg_pace', 'N/A')} min/km
Total Elevation: {activity_summary.get('total_elevation_m', 0)} m
Total Training Load: {activity_summary.get('total_training_load', 0)}

=== HEALTH SUMMARY ===
Avg Sleep Score: {health_summary.get('avg_sleep_score', 'N/A')}
Avg Sleep Duration: {health_summary.get('avg_sleep_hours', 'N/A')} hours
Avg HRV: {health_summary.get('avg_hrv', 'N/A')} ms
Avg Resting HR: {health_summary.get('avg_resting_hr', 'N/A')} bpm
Avg Body Battery Recovery: {health_summary.get('avg_bb_recovery', 'N/A')}

=== DETAILED ACTIVITIES ===
{json.dumps(week_data.get('activities', []), indent=2, default=str)}

=== DETAILED HEALTH DATA ===
{json.dumps(week_data.get('health', []), indent=2, default=str)}
"""

    if planned_sessions:
        prompt += f"""
=== PLANNED SESSIONS ===
{json.dumps(planned_sessions, indent=2, default=str)}
"""

    if runner_context:
        prompt += f"""
=== RUNNER CONTEXT ===
{json.dumps(runner_context, indent=2, default=str)}
"""

    prompt += "\nBased on this data, generate a complete weekly review in the exact JSON format specified."

    return prompt


# =============================================================================
# Cycle Plan Prompts
# =============================================================================


def build_cycle_plan_system_prompt() -> str:
    """
    Build the system prompt for cycle plan generation.

    Returns:
        System prompt string with instructions and JSON schema.
    """
    return _load_prompt("cycle_plan_system.md")


def build_cycle_plan_user_prompt(
    runner_profile: dict[str, Any],
    context: dict[str, Any],
    cycle_start_date: str,
    total_weeks: int,
) -> str:
    """
    Build the user prompt for cycle plan generation.

    Args:
        runner_profile: Runner's profile from generate_runner_profile().
        context: User context with goals, constraints, preferences.
        cycle_start_date: Start date (YYYY-MM-DD).
        total_weeks: Number of weeks in the cycle.

    Returns:
        User prompt string with formatted data.
    """
    prompt = f"""Create a {total_weeks}-week training cycle starting {cycle_start_date}.

=== RUNNER PROFILE ===
{json.dumps(runner_profile, indent=2, default=str)}

=== TRAINING CONTEXT & GOALS ===
{json.dumps(context, indent=2, default=str)}

=== CYCLE PARAMETERS ===
Start Date: {cycle_start_date}
Duration: {total_weeks} weeks

Generate a complete periodized training cycle in the exact JSON format specified.
Ensure the plan is appropriate for the runner's level and respects their constraints."""

    return prompt


# =============================================================================
# Weekly Plan Prompts
# =============================================================================


def build_weekly_plan_system_prompt() -> str:
    """
    Build the system prompt for weekly plan generation.

    Returns:
        System prompt string with instructions and JSON schema.
    """
    return _load_prompt("weekly_plan_system.md")


def build_weekly_plan_user_prompt(
    cycle_plan: dict[str, Any],
    week_number: int,
    runner_profile: dict[str, Any],
    recent_health: list[dict[str, Any]],
    previous_review: dict[str, Any] | None = None,
) -> str:
    """
    Build the user prompt for weekly plan generation.

    Args:
        cycle_plan: The overall cycle plan.
        week_number: Which week of the cycle (1-indexed).
        runner_profile: Runner's profile.
        recent_health: Recent health data (last 7 days).
        previous_review: Optional previous week's review for adaptation.

    Returns:
        User prompt string with formatted data.
    """
    prompt = f"""Generate a detailed weekly plan for Week {week_number} of the training cycle.

=== CYCLE PLAN ===
{json.dumps(cycle_plan, indent=2, default=str)}

=== RUNNER PROFILE ===
{json.dumps(runner_profile, indent=2, default=str)}

=== CURRENT HEALTH STATUS (Last 7 Days) ===
{json.dumps(recent_health, indent=2, default=str)}
"""

    if previous_review:
        prompt += f"""
=== PREVIOUS WEEK REVIEW ===
{json.dumps(previous_review, indent=2, default=str)}
"""

    prompt += f"""
Generate a complete weekly plan for Week {week_number} in the exact JSON format specified.
Adapt the plan based on the runner's current health status and any feedback from previous weeks."""

    return prompt
