"""
Garmin Adapter for the generic fetcher.
Wraps the existing GarminClient and GarminFetcher to provide a unified interface.
"""

import logging
import os
from datetime import datetime, timedelta
from typing import List

from ..base import ServiceAdapter, FetchResult

logger = logging.getLogger(__name__)


class GarminAdapter(ServiceAdapter):
    """Adapter wrapping existing GarminClient and GarminFetcher."""

    def __init__(self):
        self._fetcher = None
        self._available_types = None

    def _lazy_import(self):
        """Lazy import to avoid circular dependencies."""
        if self._available_types is None:
            from src.connectors.garmin.config import DATA_TYPES

            self._DATA_TYPES = DATA_TYPES
            self._available_types = DATA_TYPES

    @property
    def service_name(self) -> str:
        return "garmin"

    @property
    def available_data_types(self) -> List[str]:
        self._lazy_import()
        return list(self._available_types)

    def authenticate(self, data_types: List[str]) -> None:
        """
        Authenticate with Garmin Connect.

        Args:
            data_types: List of data type names (used for validation only).
        """
        self._lazy_import()

        # Validate data types
        for dt in data_types:
            if dt not in self._available_types:
                raise ValueError(
                    f"Unknown Garmin data type: {dt}. "
                    f"Available: {self._available_types}"
                )

        # Get credentials from environment
        username = os.getenv("GARMIN_USERNAME")
        password = os.getenv("GARMIN_PASSWORD")

        if not username or not password:
            missing = []
            if not username:
                missing.append("GARMIN_USERNAME")
            if not password:
                missing.append("GARMIN_PASSWORD")
            raise ValueError(f"Missing Garmin credentials: {', '.join(missing)}")

        # Import and authenticate
        from src.connectors.garmin.client import GarminClient
        from src.connectors.garmin.fetcher import GarminFetcher

        env_vars = {
            "GARMIN_USERNAME": username,
            "GARMIN_PASSWORD": password,
        }

        client_wrapper = GarminClient(env_vars)
        client = client_wrapper.get_client()
        self._fetcher = GarminFetcher(client)

        logger.info(f"Garmin authenticated for: {data_types}")

    def fetch(self, data_type: str, days: int = 1, limit: int = 50) -> FetchResult:
        """
        Fetch data for a specific data type.

        Args:
            data_type: The type of data to fetch.
            days: Number of days of data to fetch.
            limit: Not used for Garmin (kept for interface compatibility).

        Returns:
            FetchResult with the fetched data.
        """
        self._lazy_import()

        if self._fetcher is None:
            raise RuntimeError("Not authenticated. Call authenticate() first.")

        if data_type not in self._available_types:
            return FetchResult(
                service="garmin",
                data_type=data_type,
                data=[],
                timestamp=datetime.now(),
                success=False,
                error=f"Unknown data type: {data_type}",
            )

        timestamp = datetime.now()
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)

        try:
            data = self._fetcher.fetch_metric(data_type, start_date, end_date)

            # Normalize to list
            if data is None:
                data = []
            elif not isinstance(data, list):
                data = [data]

            logger.info(f"Fetched {len(data)} items for {data_type}")

            return FetchResult(
                service="garmin",
                data_type=data_type,
                data=data,
                timestamp=timestamp,
                success=True,
            )

        except Exception as e:
            logger.error(f"Error fetching {data_type}: {e}")
            return FetchResult(
                service="garmin",
                data_type=data_type,
                data=[],
                timestamp=timestamp,
                success=False,
                error=str(e),
            )
