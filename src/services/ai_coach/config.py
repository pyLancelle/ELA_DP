"""
AI Coach Configuration
----------------------
Configuration constants and environment variables for the AI Coach service.
"""

import os
from typing import Final

# =============================================================================
# User Configuration (Hardcoded for MVP)
# =============================================================================

USER_ID: Final[str] = "user_etienne"

# =============================================================================
# Anthropic API Configuration
# =============================================================================

ANTHROPIC_API_KEY: Final[str] = os.getenv("ANTHROPIC_API_KEY", "")
DEFAULT_MODEL: Final[str] = "claude-opus-4-5-20251101"
DEFAULT_MAX_TOKENS: Final[int] = 4096
API_TIMEOUT_SECONDS: Final[int] = 30
MAX_RETRIES: Final[int] = 3

# =============================================================================
# GCP Configuration
# =============================================================================

PROJECT_ID: Final[str] = os.getenv("GOOGLE_CLOUD_PROJECT", "polar-scene-465223-f7")
DATASET: Final[str] = os.getenv("DATASET", "dp_product_dev")
GCS_BUCKET_NAME: Final[str] = os.getenv(
    "GCS_BUCKET_NAME", "ela-dataplatform-ai-coach-contexts"
)

# =============================================================================
# Retry Configuration (Exponential Backoff)
# =============================================================================

RETRY_BASE_DELAY_SECONDS: Final[float] = 1.0
RETRY_MULTIPLIER: Final[float] = 2.0

# =============================================================================
# Usage Tracking Configuration
# =============================================================================

# GCS path for tracking Claude API usage (JSONL files)
GCS_USAGE_TRACKING_PATH: Final[str] = os.getenv(
    "GCS_USAGE_TRACKING_PATH", "gs://ela-dp-dev/claude"
)

# Enable/disable usage tracking (defaults to True)
ENABLE_USAGE_TRACKING: Final[bool] = os.getenv(
    "ENABLE_USAGE_TRACKING", "true"
).lower() == "true"

# Pricing per million tokens (Claude Opus 4.5 - January 2025)
PRICE_PER_MILLION_INPUT_TOKENS: Final[float] = 15.0
PRICE_PER_MILLION_OUTPUT_TOKENS: Final[float] = 75.0
