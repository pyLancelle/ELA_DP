"""
Spotify Adapter for the generic fetcher.
Wraps the existing SpotifyConnector to provide a unified interface.
"""

import logging
import os
from datetime import datetime
from pathlib import Path
from typing import List, Optional

from ..base import ServiceAdapter, FetchResult

logger = logging.getLogger(__name__)


class SpotifyAdapter(ServiceAdapter):
    """Adapter wrapping existing SpotifyConnector."""

    def __init__(self):
        self._connector = None
        self._data_type_map = None

    def _lazy_import(self):
        """Lazy import to avoid circular dependencies."""
        if self._data_type_map is None:
            from src.connectors.spotify.spotify_fetch import (
                SpotifyConnector,
                SpotifyConfig,
                DataType,
            )

            self._SpotifyConnector = SpotifyConnector
            self._SpotifyConfig = SpotifyConfig
            self._DataType = DataType
            self._data_type_map = {dt.value: dt for dt in DataType}

    @property
    def service_name(self) -> str:
        return "spotify"

    @property
    def available_data_types(self) -> List[str]:
        self._lazy_import()
        return list(self._data_type_map.keys())

    def authenticate(self, data_types: List[str]) -> None:
        """
        Authenticate with Spotify for the given data types.

        Args:
            data_types: List of data type names to authenticate for.
        """
        self._lazy_import()

        # Validate data types
        for dt in data_types:
            if dt not in self._data_type_map:
                raise ValueError(
                    f"Unknown Spotify data type: {dt}. "
                    f"Available: {list(self._data_type_map.keys())}"
                )

        # Get credentials from environment
        client_id = os.getenv("SPOTIFY_CLIENT_ID")
        client_secret = os.getenv("SPOTIFY_CLIENT_SECRET")
        redirect_uri = os.getenv("SPOTIFY_REDIRECT_URI")
        refresh_token = os.getenv("SPOTIFY_REFRESH_TOKEN")

        missing = []
        if not client_id:
            missing.append("SPOTIFY_CLIENT_ID")
        if not client_secret:
            missing.append("SPOTIFY_CLIENT_SECRET")
        if not redirect_uri:
            missing.append("SPOTIFY_REDIRECT_URI")
        if not refresh_token:
            missing.append("SPOTIFY_REFRESH_TOKEN")

        if missing:
            raise ValueError(f"Missing Spotify credentials: {', '.join(missing)}")

        # Create config and connector
        config = self._SpotifyConfig(
            client_id=client_id,
            client_secret=client_secret,
            redirect_uri=redirect_uri,
            refresh_token=refresh_token,
            cache_path=Path(".spotify_cache"),
        )

        self._connector = self._SpotifyConnector(config)

        # Convert data type names to DataType enums
        dt_enums = [self._data_type_map[dt] for dt in data_types]
        self._connector.authenticate(dt_enums)

        logger.info(f"Spotify authenticated for: {data_types}")

    def fetch(self, data_type: str, days: int = 1, limit: int = 50) -> FetchResult:
        """
        Fetch data for a specific data type.

        Args:
            data_type: The type of data to fetch.
            days: Not used for Spotify (kept for interface compatibility).
            limit: Maximum items to fetch.

        Returns:
            FetchResult with the fetched data.
        """
        self._lazy_import()

        if self._connector is None:
            raise RuntimeError("Not authenticated. Call authenticate() first.")

        if data_type not in self._data_type_map:
            return FetchResult(
                service="spotify",
                data_type=data_type,
                data=[],
                timestamp=datetime.now(),
                success=False,
                error=f"Unknown data type: {data_type}",
            )

        dt_enum = self._data_type_map[data_type]
        timestamp = datetime.now()

        try:
            # Fetch data using the connector
            data = self._connector.fetch_data(dt_enum, limit=limit)

            # Normalize to list
            if isinstance(data, dict):
                data = [data]
            elif not isinstance(data, list):
                data = []

            logger.info(f"Fetched {len(data)} items for {data_type}")

            return FetchResult(
                service="spotify",
                data_type=data_type,
                data=data,
                timestamp=timestamp,
                success=True,
            )

        except Exception as e:
            logger.error(f"Error fetching {data_type}: {e}")
            return FetchResult(
                service="spotify",
                data_type=data_type,
                data=[],
                timestamp=timestamp,
                success=False,
                error=str(e),
            )
