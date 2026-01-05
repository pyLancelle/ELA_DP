"""
Anthropic Client Wrapper
------------------------
Wrapper around the Anthropic SDK for Claude API calls.
Includes retry logic, token tracking, error handling, and usage tracking to GCS.
"""

import hashlib
import json
import logging
import time
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Optional

import anthropic
from google.cloud import storage

from .config import (
    ANTHROPIC_API_KEY,
    API_TIMEOUT_SECONDS,
    DEFAULT_MAX_TOKENS,
    DEFAULT_MODEL,
    ENABLE_USAGE_TRACKING,
    GCS_USAGE_TRACKING_PATH,
    MAX_RETRIES,
    PRICE_PER_MILLION_INPUT_TOKENS,
    PRICE_PER_MILLION_OUTPUT_TOKENS,
    RETRY_BASE_DELAY_SECONDS,
    RETRY_MULTIPLIER,
    USER_ID,
)

logger = logging.getLogger(__name__)


# =============================================================================
# Custom Exceptions
# =============================================================================


class AICoachError(Exception):
    """Base exception for AI Coach errors."""

    pass


class AnthropicAPIError(AICoachError):
    """Exception raised when Anthropic API call fails."""

    def __init__(self, message: str, original_error: Optional[Exception] = None):
        """
        Initialize AnthropicAPIError.

        Args:
            message: Error message describing what went wrong.
            original_error: The original exception that caused this error.
        """
        super().__init__(message)
        self.original_error = original_error


class AnthropicConfigError(AICoachError):
    """Exception raised when Anthropic configuration is invalid."""

    pass


# =============================================================================
# Response Data Classes
# =============================================================================


@dataclass
class TokenUsage:
    """Token usage statistics for an API call."""

    prompt_tokens: int
    completion_tokens: int

    @property
    def total_tokens(self) -> int:
        """Return total tokens used."""
        return self.prompt_tokens + self.completion_tokens


@dataclass
class ClaudeResponse:
    """Response from a Claude API call."""

    content: str
    usage: TokenUsage
    model: str
    latency_seconds: float


@dataclass
class UsageRecord:
    """Record of a Claude API call for tracking and billing analysis."""

    call_id: str
    user_id: str
    timestamp: str
    model: str
    use_case: str
    prompt_hash: str
    prompt_tokens: int
    completion_tokens: int
    total_tokens: int
    latency_seconds: float
    cost_input_usd: float
    cost_output_usd: float
    cost_total_usd: float
    status: str
    system_prompt: str
    user_prompt: str
    output: Optional[str] = None
    error_message: Optional[str] = None
    # Context fields for linking related calls
    cycle_id: Optional[str] = None
    week_number: Optional[int] = None


# =============================================================================
# Usage Tracking Functions
# =============================================================================


def _parse_gcs_path(path: str) -> tuple[str, str]:
    """
    Parse gs://bucket/prefix into (bucket, prefix).

    Args:
        path: GCS path like gs://bucket/path/

    Returns:
        Tuple of (bucket_name, prefix)
    """
    if not path.startswith("gs://"):
        raise ValueError(f"Invalid GCS path: {path}. Must start with gs://")

    path_without_scheme = path[5:]
    parts = path_without_scheme.split("/", 1)
    bucket = parts[0]
    prefix = parts[1] if len(parts) > 1 else ""

    return bucket, prefix


def _upload_usage_record(record: UsageRecord) -> Optional[str]:
    """
    Upload a usage record to GCS as JSONL.

    Args:
        record: UsageRecord to upload.

    Returns:
        GCS URI of uploaded file, or None if upload failed.
    """
    if not ENABLE_USAGE_TRACKING:
        return None

    try:
        bucket_name, prefix = _parse_gcs_path(GCS_USAGE_TRACKING_PATH)

        # Generate filename: YYYYMMDD_HHMMSS_usecase_CLAUDE.jsonl
        dt = datetime.fromisoformat(record.timestamp.replace("Z", "+00:00"))
        timestamp_str = dt.strftime("%Y%m%d_%H%M%S")
        use_case_clean = record.use_case.replace(" ", "_").replace("-", "_")
        filename = f"{timestamp_str}_CLAUDE_{use_case_clean}.jsonl"

        blob_path = f"{prefix}/{filename}".lstrip("/") if prefix else filename

        # Convert to JSONL (single line)
        record_dict = asdict(record)
        jsonl_content = json.dumps(record_dict, default=str, ensure_ascii=False) + "\n"

        # Upload to GCS
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_path)
        blob.upload_from_string(jsonl_content, content_type="application/x-ndjson")

        gcs_uri = f"gs://{bucket_name}/{blob_path}"
        logger.info(f"Usage record uploaded | gcs_uri={gcs_uri}")

        return gcs_uri

    except Exception as e:
        logger.warning(f"Failed to upload usage record | error={e}")
        return None


def _create_usage_record(
    call_id: str,
    model: str,
    use_case: str,
    system_prompt: str,
    user_prompt: str,
    usage: Optional[TokenUsage],
    latency_seconds: float,
    status: str,
    output: Optional[str] = None,
    error_message: Optional[str] = None,
    cycle_id: Optional[str] = None,
    week_number: Optional[int] = None,
) -> UsageRecord:
    """
    Create a usage record for tracking.

    Args:
        call_id: Unique identifier for this API call.
        model: Model used for the call.
        use_case: Description of what this call is for.
        system_prompt: System prompt sent to Claude.
        user_prompt: User prompt sent to Claude.
        usage: Token usage statistics (None if call failed).
        latency_seconds: Time taken for the API call.
        status: "success" or "error".
        output: Claude's response content (None if call failed).
        error_message: Error message if status is "error".
        cycle_id: Optional training cycle identifier for linking related calls.
        week_number: Optional week number within the cycle.

    Returns:
        UsageRecord ready for upload.
    """
    # Hash prompts for deduplication/analytics
    prompt_content = f"{system_prompt}|||{user_prompt}"
    prompt_hash = hashlib.sha256(prompt_content.encode()).hexdigest()[:16]

    prompt_tokens = usage.prompt_tokens if usage else 0
    completion_tokens = usage.completion_tokens if usage else 0
    total_tokens = prompt_tokens + completion_tokens

    # Calculate costs
    cost_input = (prompt_tokens / 1_000_000) * PRICE_PER_MILLION_INPUT_TOKENS
    cost_output = (completion_tokens / 1_000_000) * PRICE_PER_MILLION_OUTPUT_TOKENS
    cost_total = cost_input + cost_output

    return UsageRecord(
        call_id=call_id,
        user_id=USER_ID,
        timestamp=datetime.now(timezone.utc).isoformat(),
        model=model,
        use_case=use_case,
        prompt_hash=prompt_hash,
        prompt_tokens=prompt_tokens,
        completion_tokens=completion_tokens,
        total_tokens=total_tokens,
        latency_seconds=round(latency_seconds, 3),
        cost_input_usd=round(cost_input, 6),
        cost_output_usd=round(cost_output, 6),
        cost_total_usd=round(cost_total, 6),
        status=status,
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        output=output,
        error_message=error_message,
        cycle_id=cycle_id,
        week_number=week_number,
    )


# =============================================================================
# Client Functions
# =============================================================================


def get_client() -> anthropic.Anthropic:
    """
    Get a configured Anthropic client.

    Returns:
        Configured Anthropic client instance.

    Raises:
        AnthropicConfigError: If ANTHROPIC_API_KEY is not set.
    """
    if not ANTHROPIC_API_KEY:
        raise AnthropicConfigError(
            "ANTHROPIC_API_KEY environment variable is not set. "
            "Please set it before using the AI Coach service."
        )

    return anthropic.Anthropic(
        api_key=ANTHROPIC_API_KEY,
        timeout=API_TIMEOUT_SECONDS,
    )


def call_claude(
    system_prompt: str,
    user_prompt: str,
    use_case: str = "unknown",
    model: str = DEFAULT_MODEL,
    max_tokens: int = DEFAULT_MAX_TOKENS,
    cycle_id: Optional[str] = None,
    week_number: Optional[int] = None,
) -> ClaudeResponse:
    """
    Call Claude API with retry logic, token tracking, and usage logging to GCS.

    Args:
        system_prompt: System prompt defining Claude's behavior and context.
        user_prompt: User prompt with the actual request/data.
        use_case: Description of what this call is for (e.g., "generate_profile",
            "weekly_review"). Used for tracking and analytics.
        model: Model identifier to use. Defaults to claude-opus-4-5-20251101.
        max_tokens: Maximum tokens in the response. Defaults to 4096.
        cycle_id: Optional training cycle identifier for linking related calls.
        week_number: Optional week number within the cycle.

    Returns:
        ClaudeResponse containing the response content, token usage, and latency.

    Raises:
        AnthropicAPIError: If all retry attempts fail.
        AnthropicConfigError: If the API key is not configured.
    """
    client = get_client()
    call_id = str(uuid.uuid4())
    last_error: Optional[Exception] = None
    total_latency = 0.0

    prompt_summary = user_prompt[:100] + "..." if len(user_prompt) > 100 else user_prompt
    logger.info(
        f"Calling Claude API | call_id={call_id} | use_case={use_case} | "
        f"model={model} | max_tokens={max_tokens} | prompt_preview='{prompt_summary}'"
    )

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            start_time = time.time()

            response = client.messages.create(
                model=model,
                max_tokens=max_tokens,
                system=system_prompt,
                messages=[{"role": "user", "content": user_prompt}],
            )

            latency = time.time() - start_time
            total_latency += latency

            # Extract response content
            content = ""
            if response.content and len(response.content) > 0:
                content = response.content[0].text

            # Build token usage
            usage = TokenUsage(
                prompt_tokens=response.usage.input_tokens,
                completion_tokens=response.usage.output_tokens,
            )

            logger.info(
                f"Claude API success | call_id={call_id} | attempt={attempt} | "
                f"latency={latency:.2f}s | prompt_tokens={usage.prompt_tokens} | "
                f"completion_tokens={usage.completion_tokens} | "
                f"total_tokens={usage.total_tokens}"
            )

            # Track usage to GCS
            record = _create_usage_record(
                call_id=call_id,
                model=model,
                use_case=use_case,
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                usage=usage,
                latency_seconds=total_latency,
                status="success",
                output=content,
                cycle_id=cycle_id,
                week_number=week_number,
            )
            _upload_usage_record(record)

            return ClaudeResponse(
                content=content,
                usage=usage,
                model=model,
                latency_seconds=latency,
            )

        except anthropic.APIError as e:
            last_error = e
            latency = time.time() - start_time
            total_latency += latency
            delay = RETRY_BASE_DELAY_SECONDS * (RETRY_MULTIPLIER ** (attempt - 1))

            logger.warning(
                f"Claude API error | call_id={call_id} | attempt={attempt}/{MAX_RETRIES} | "
                f"error={type(e).__name__}: {e} | retry_delay={delay}s"
            )

            if attempt < MAX_RETRIES:
                time.sleep(delay)

    # All retries exhausted - track the failure
    error_message = f"Claude API call failed after {MAX_RETRIES} attempts"
    logger.error(f"{error_message} | call_id={call_id} | last_error={last_error}")

    record = _create_usage_record(
        call_id=call_id,
        model=model,
        use_case=use_case,
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        usage=None,
        latency_seconds=total_latency,
        status="error",
        error_message=str(last_error),
        cycle_id=cycle_id,
        week_number=week_number,
    )
    _upload_usage_record(record)

    raise AnthropicAPIError(error_message, original_error=last_error)
