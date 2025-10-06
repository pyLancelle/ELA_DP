"""Withings connector for body composition synchronization."""

from .withings_client import (
    WithingsClient,
    sync_withings_to_garmin,
    upload_body_composition_to_garmin,
)

__all__ = [
    "WithingsClient",
    "sync_withings_to_garmin",
    "upload_body_composition_to_garmin",
]
