"""Withings connector module."""

from .withings_client import (
    WithingsClient,
    sync_withings_to_garmin,
    upload_weight_to_garmin,
)

__all__ = [
    "WithingsClient",
    "sync_withings_to_garmin",
    "upload_weight_to_garmin",
]
