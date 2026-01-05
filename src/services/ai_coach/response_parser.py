"""
Response Parser for AI Coach
----------------------------
Parses and validates JSON responses from Claude API.
Ensures responses match expected schemas.
"""

import json
import logging
import re
from typing import Any, TypeVar

logger = logging.getLogger(__name__)

T = TypeVar("T", bound=dict)


class ParseError(Exception):
    """Exception raised when response parsing fails."""

    def __init__(self, message: str, raw_content: str | None = None):
        """
        Initialize ParseError.

        Args:
            message: Error message describing what went wrong.
            raw_content: The raw content that failed to parse.
        """
        super().__init__(message)
        self.raw_content = raw_content


def extract_json_from_response(content: str) -> dict[str, Any]:
    """
    Extract JSON from Claude's response, handling potential markdown wrapping.

    Args:
        content: Raw response content from Claude.

    Returns:
        Parsed JSON as dictionary.

    Raises:
        ParseError: If JSON extraction or parsing fails.
    """
    if not content or not content.strip():
        raise ParseError("Empty response content", content)

    # Try direct JSON parse first
    try:
        return json.loads(content)
    except json.JSONDecodeError:
        pass

    # Try to extract JSON from markdown code blocks
    json_patterns = [
        r"```json\s*([\s\S]*?)\s*```",  # ```json ... ```
        r"```\s*([\s\S]*?)\s*```",  # ``` ... ```
        r"\{[\s\S]*\}",  # Raw JSON object
    ]

    for pattern in json_patterns:
        match = re.search(pattern, content)
        if match:
            json_str = match.group(1) if match.lastindex else match.group(0)
            try:
                return json.loads(json_str.strip())
            except json.JSONDecodeError:
                continue

    raise ParseError(
        f"Could not extract valid JSON from response. Content preview: {content[:500]}",
        content,
    )


def validate_required_fields(
    data: dict[str, Any],
    required_fields: list[str],
    context: str = "response",
) -> None:
    """
    Validate that required fields are present in the data.

    Args:
        data: Dictionary to validate.
        required_fields: List of required field names.
        context: Context string for error messages.

    Raises:
        ParseError: If any required field is missing.
    """
    missing = [field for field in required_fields if field not in data]
    if missing:
        raise ParseError(
            f"Missing required fields in {context}: {missing}",
            json.dumps(data),
        )


# =============================================================================
# Profile Response Parser
# =============================================================================

PROFILE_REQUIRED_FIELDS = [
    "runner_level",
    "weekly_volume_km",
    "primary_strengths",
    "primary_weaknesses",
    "training_zones",
    "recovery_profile",
    "training_analysis",
    "recommendations",
    "summary",
]


def parse_profile_response(content: str) -> dict[str, Any]:
    """
    Parse and validate a runner profile response from Claude.

    Args:
        content: Raw response content from Claude.

    Returns:
        Validated profile dictionary.

    Raises:
        ParseError: If parsing or validation fails.
    """
    logger.info("Parsing profile response")

    data = extract_json_from_response(content)
    validate_required_fields(data, PROFILE_REQUIRED_FIELDS, "runner profile")

    # Validate runner_level enum
    valid_levels = ["beginner", "intermediate", "advanced", "elite"]
    if data.get("runner_level") not in valid_levels:
        logger.warning(
            f"Invalid runner_level: {data.get('runner_level')}, defaulting to 'intermediate'"
        )
        data["runner_level"] = "intermediate"

    # Ensure lists are lists
    for field in ["primary_strengths", "primary_weaknesses"]:
        if not isinstance(data.get(field), list):
            data[field] = [data.get(field)] if data.get(field) else []

    # Validate nested structures exist
    for nested_field in ["training_zones", "recovery_profile", "training_analysis", "recommendations"]:
        if not isinstance(data.get(nested_field), dict):
            raise ParseError(
                f"Field '{nested_field}' must be a dictionary",
                json.dumps(data),
            )

    logger.info(f"Profile parsed successfully: level={data['runner_level']}")
    return data


# =============================================================================
# Weekly Review Response Parser
# =============================================================================

WEEKLY_REVIEW_REQUIRED_FIELDS = [
    "week_summary",
    "health_assessment",
    "training_analysis",
    "adaptations_needed",
    "coach_notes",
]


def parse_weekly_review_response(content: str) -> dict[str, Any]:
    """
    Parse and validate a weekly review response from Claude.

    Args:
        content: Raw response content from Claude.

    Returns:
        Validated weekly review dictionary.

    Raises:
        ParseError: If parsing or validation fails.
    """
    logger.info("Parsing weekly review response")

    data = extract_json_from_response(content)
    validate_required_fields(data, WEEKLY_REVIEW_REQUIRED_FIELDS, "weekly review")

    # Validate nested structures
    for nested_field in ["week_summary", "health_assessment", "training_analysis", "adaptations_needed"]:
        if not isinstance(data.get(nested_field), dict):
            raise ParseError(
                f"Field '{nested_field}' must be a dictionary",
                json.dumps(data),
            )

    logger.info("Weekly review parsed successfully")
    return data


# =============================================================================
# Cycle Plan Response Parser
# =============================================================================

CYCLE_PLAN_REQUIRED_FIELDS = [
    "cycle_overview",
    "periodization",
    "weekly_structure",
    "weekly_summaries",
    "key_workouts",
    "progression_rules",
]


def parse_cycle_plan_response(content: str) -> dict[str, Any]:
    """
    Parse and validate a cycle plan response from Claude.

    Args:
        content: Raw response content from Claude.

    Returns:
        Validated cycle plan dictionary.

    Raises:
        ParseError: If parsing or validation fails.
    """
    logger.info("Parsing cycle plan response")

    data = extract_json_from_response(content)
    validate_required_fields(data, CYCLE_PLAN_REQUIRED_FIELDS, "cycle plan")

    # Validate weekly_summaries is a list
    if not isinstance(data.get("weekly_summaries"), list):
        raise ParseError(
            "Field 'weekly_summaries' must be a list",
            json.dumps(data),
        )

    logger.info(
        f"Cycle plan parsed: {len(data.get('weekly_summaries', []))} weeks"
    )
    return data


# =============================================================================
# Weekly Plan Response Parser
# =============================================================================

WEEKLY_PLAN_REQUIRED_FIELDS = [
    "week_overview",
    "daily_workouts",
    "key_sessions",
    "coach_notes",
]


def parse_weekly_plan_response(content: str) -> dict[str, Any]:
    """
    Parse and validate a weekly plan response from Claude.

    Args:
        content: Raw response content from Claude.

    Returns:
        Validated weekly plan dictionary.

    Raises:
        ParseError: If parsing or validation fails.
    """
    logger.info("Parsing weekly plan response")

    data = extract_json_from_response(content)
    validate_required_fields(data, WEEKLY_PLAN_REQUIRED_FIELDS, "weekly plan")

    # Validate daily_workouts is a list with 7 days
    daily_workouts = data.get("daily_workouts", [])
    if not isinstance(daily_workouts, list):
        raise ParseError(
            "Field 'daily_workouts' must be a list",
            json.dumps(data),
        )

    if len(daily_workouts) != 7:
        logger.warning(
            f"Expected 7 daily workouts, got {len(daily_workouts)}"
        )

    logger.info(f"Weekly plan parsed: {len(daily_workouts)} workouts")
    return data


# =============================================================================
# Generic Parser
# =============================================================================


def parse_response(
    content: str,
    response_type: str,
) -> dict[str, Any]:
    """
    Parse a Claude response based on the expected type.

    Args:
        content: Raw response content from Claude.
        response_type: One of "profile", "weekly_review", "cycle_plan", "weekly_plan".

    Returns:
        Validated response dictionary.

    Raises:
        ParseError: If parsing or validation fails.
        ValueError: If response_type is unknown.
    """
    parsers = {
        "profile": parse_profile_response,
        "weekly_review": parse_weekly_review_response,
        "cycle_plan": parse_cycle_plan_response,
        "weekly_plan": parse_weekly_plan_response,
    }

    parser = parsers.get(response_type)
    if not parser:
        raise ValueError(f"Unknown response type: {response_type}")

    return parser(content)
