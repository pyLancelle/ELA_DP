"""
GCS Manager for AI Coach
------------------------
Handles upload and retrieval of context documents and training philosophy
from Google Cloud Storage.
"""

import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

from google.cloud import storage

from .config import GCS_BUCKET_NAME, USER_ID

logger = logging.getLogger(__name__)


# =============================================================================
# Custom Exceptions
# =============================================================================


class GCSManagerError(Exception):
    """Base exception for GCS Manager errors."""

    pass


class GCSUploadError(GCSManagerError):
    """Exception raised when GCS upload fails."""

    pass


class GCSRetrievalError(GCSManagerError):
    """Exception raised when GCS retrieval fails."""

    pass


# =============================================================================
# Context Management Functions
# =============================================================================


def upload_context(
    context_data: dict[str, Any],
    context_type: str = "general_training",
    user_id: str = USER_ID,
) -> dict[str, Any]:
    """
    Upload a training context document to GCS.

    Args:
        context_data: Context document data (objective, constraints, preferences,
            training_philosophy, etc.)
        context_type: Type of context ("race_goal" or "general_training")
        user_id: User identifier (defaults to hardcoded USER_ID)

    Returns:
        Dict with context_id, gcs_path, and uploaded_at

    Raises:
        GCSUploadError: If upload fails
    """
    try:
        # Generate context_id if not provided
        context_id = context_data.get("context_id", str(uuid.uuid4()))
        timestamp = datetime.now(timezone.utc)

        # Build complete context document
        context_doc = {
            "context_id": context_id,
            "user_id": user_id,
            "created_at": timestamp.isoformat(),
            "context_type": context_type,
            **context_data,
        }

        # Build GCS path: user_etienne/contexts/{timestamp}_{type}.json
        timestamp_str = timestamp.strftime("%Y%m%d_%H%M%S")
        context_type_clean = context_type.replace(" ", "_")
        filename = f"{timestamp_str}_{context_type_clean}.json"
        blob_path = f"{user_id}/contexts/{filename}"

        # Upload to GCS
        client = storage.Client()
        bucket = client.bucket(GCS_BUCKET_NAME)
        blob = bucket.blob(blob_path)

        json_content = json.dumps(context_doc, indent=2, default=str, ensure_ascii=False)
        blob.upload_from_string(json_content, content_type="application/json")

        gcs_path = f"gs://{GCS_BUCKET_NAME}/{blob_path}"
        logger.info(
            f"Context uploaded | context_id={context_id} | type={context_type} | gcs_path={gcs_path}"
        )

        return {
            "context_id": context_id,
            "gcs_path": gcs_path,
            "uploaded_at": timestamp.isoformat(),
        }

    except Exception as e:
        logger.error(f"Failed to upload context | error={type(e).__name__}: {e}", exc_info=True)
        raise GCSUploadError(f"Failed to upload context to GCS: {e}") from e


def get_context(gcs_path: str) -> dict[str, Any]:
    """
    Retrieve a context document from GCS.

    Args:
        gcs_path: Full GCS path (gs://bucket/path/to/context.json)

    Returns:
        Context document as dict

    Raises:
        GCSRetrievalError: If retrieval fails
    """
    try:
        # Parse GCS path
        if not gcs_path.startswith("gs://"):
            raise ValueError(f"Invalid GCS path: {gcs_path}. Must start with gs://")

        path_without_scheme = gcs_path[5:]
        parts = path_without_scheme.split("/", 1)
        bucket_name = parts[0]
        blob_path = parts[1] if len(parts) > 1 else ""

        # Download from GCS
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_path)

        if not blob.exists():
            raise GCSRetrievalError(f"Context not found at {gcs_path}")

        json_content = blob.download_as_text()
        context_doc = json.loads(json_content)

        logger.info(f"Context retrieved | gcs_path={gcs_path}")
        return context_doc

    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse context JSON | gcs_path={gcs_path} | error={e}")
        raise GCSRetrievalError(f"Invalid JSON in context document: {e}") from e
    except Exception as e:
        logger.error(f"Failed to retrieve context | gcs_path={gcs_path} | error={e}", exc_info=True)
        raise GCSRetrievalError(f"Failed to retrieve context from GCS: {e}") from e


def list_contexts(user_id: str = USER_ID, limit: int = 10) -> list[dict[str, Any]]:
    """
    List recent context documents for a user.

    Args:
        user_id: User identifier (defaults to hardcoded USER_ID)
        limit: Maximum number of contexts to return

    Returns:
        List of context metadata dicts (context_id, gcs_path, created_at, type)

    Raises:
        GCSRetrievalError: If listing fails
    """
    try:
        client = storage.Client()
        bucket = client.bucket(GCS_BUCKET_NAME)

        # List blobs with prefix user_id/contexts/
        prefix = f"{user_id}/contexts/"
        blobs = bucket.list_blobs(prefix=prefix, max_results=limit)

        contexts = []
        for blob in blobs:
            # Skip if not a JSON file
            if not blob.name.endswith(".json"):
                continue

            try:
                # Download and parse to extract metadata
                json_content = blob.download_as_text()
                context_doc = json.loads(json_content)

                contexts.append(
                    {
                        "context_id": context_doc.get("context_id"),
                        "gcs_path": f"gs://{GCS_BUCKET_NAME}/{blob.name}",
                        "created_at": context_doc.get("created_at"),
                        "context_type": context_doc.get("context_type"),
                        "objective": context_doc.get("objective", {}).get("type"),
                    }
                )
            except (json.JSONDecodeError, KeyError) as e:
                logger.warning(f"Skipping invalid context file {blob.name} | error={e}")
                continue

        # Sort by created_at descending (most recent first)
        contexts.sort(key=lambda x: x.get("created_at", ""), reverse=True)

        logger.info(f"Listed {len(contexts)} contexts for user {user_id}")
        return contexts

    except Exception as e:
        logger.error(f"Failed to list contexts | user_id={user_id} | error={e}", exc_info=True)
        raise GCSRetrievalError(f"Failed to list contexts from GCS: {e}") from e


# =============================================================================
# Profile Storage Functions
# =============================================================================


def upload_profile(profile_data: dict[str, Any], user_id: str = USER_ID) -> str:
    """
    Upload a runner profile to GCS for archival/backup.

    Args:
        profile_data: Complete runner profile
        user_id: User identifier

    Returns:
        GCS path where profile was stored

    Raises:
        GCSUploadError: If upload fails
    """
    try:
        timestamp = datetime.now(timezone.utc)
        timestamp_str = timestamp.strftime("%Y%m%d_%H%M%S")
        filename = f"profile_{timestamp_str}.json"
        blob_path = f"{user_id}/profiles/{filename}"

        # Upload to GCS
        client = storage.Client()
        bucket = client.bucket(GCS_BUCKET_NAME)
        blob = bucket.blob(blob_path)

        json_content = json.dumps(profile_data, indent=2, default=str, ensure_ascii=False)
        blob.upload_from_string(json_content, content_type="application/json")

        gcs_path = f"gs://{GCS_BUCKET_NAME}/{blob_path}"
        logger.info(f"Profile uploaded | gcs_path={gcs_path}")

        return gcs_path

    except Exception as e:
        logger.error(f"Failed to upload profile | error={e}", exc_info=True)
        raise GCSUploadError(f"Failed to upload profile to GCS: {e}") from e


def upload_cycle_plan(
    cycle_id: str, cycle_data: dict[str, Any], user_id: str = USER_ID
) -> str:
    """
    Upload a cycle plan to GCS.

    Args:
        cycle_id: Cycle identifier
        cycle_data: Complete cycle plan data
        user_id: User identifier

    Returns:
        GCS path where cycle plan was stored

    Raises:
        GCSUploadError: If upload fails
    """
    try:
        blob_path = f"{user_id}/cycles/{cycle_id}/cycle_plan.json"

        # Upload to GCS
        client = storage.Client()
        bucket = client.bucket(GCS_BUCKET_NAME)
        blob = bucket.blob(blob_path)

        json_content = json.dumps(cycle_data, indent=2, default=str, ensure_ascii=False)
        blob.upload_from_string(json_content, content_type="application/json")

        gcs_path = f"gs://{GCS_BUCKET_NAME}/{blob_path}"
        logger.info(f"Cycle plan uploaded | cycle_id={cycle_id} | gcs_path={gcs_path}")

        return gcs_path

    except Exception as e:
        logger.error(f"Failed to upload cycle plan | cycle_id={cycle_id} | error={e}", exc_info=True)
        raise GCSUploadError(f"Failed to upload cycle plan to GCS: {e}") from e


# =============================================================================
# Markdown Storage Functions
# =============================================================================


def upload_markdown(
    content: str,
    file_type: str,
    cycle_id: str,
    week_number: Optional[int] = None,
    user_id: str = USER_ID,
) -> str:
    """
    Upload a markdown file (plan or review) to GCS.

    Args:
        content: Markdown content
        file_type: "plan" or "review"
        cycle_id: Cycle identifier
        week_number: Week number (optional, can be inferred from content)
        user_id: User identifier

    Returns:
        GCS path where markdown was stored

    Raises:
        GCSUploadError: If upload fails
    """
    try:
        # Determine folder and filename based on type
        if file_type == "plan":
            folder = "plans"
            filename = f"week_{week_number:02d}_plan.md" if week_number else "plan.md"
        elif file_type == "review":
            folder = "reviews"
            filename = f"week_{week_number:02d}_review.md" if week_number else "review.md"
        else:
            raise ValueError(f"Invalid file_type: {file_type}. Must be 'plan' or 'review'")

        blob_path = f"{user_id}/cycles/{cycle_id}/{folder}/{filename}"

        # Upload to GCS
        client = storage.Client()
        bucket = client.bucket(GCS_BUCKET_NAME)
        blob = bucket.blob(blob_path)

        blob.upload_from_string(content, content_type="text/markdown; charset=utf-8")

        gcs_path = f"gs://{GCS_BUCKET_NAME}/{blob_path}"
        logger.info(
            f"Markdown uploaded | type={file_type} | cycle_id={cycle_id} | week={week_number} | gcs_path={gcs_path}"
        )

        return gcs_path

    except Exception as e:
        logger.error(
            f"Failed to upload markdown | type={file_type} | cycle_id={cycle_id} | error={e}",
            exc_info=True,
        )
        raise GCSUploadError(f"Failed to upload markdown to GCS: {e}") from e
