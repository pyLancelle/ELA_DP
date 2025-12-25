"""
Base classes for the generic fetcher.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Dict, Any, Optional, Iterator


@dataclass
class FetchResult:
    """Result of a single data type fetch."""

    service: str
    data_type: str
    data: List[Dict[str, Any]]
    timestamp: datetime
    success: bool
    error: Optional[str] = None

    @property
    def filename(self) -> str:
        """Generate standard filename for this result."""
        ts = self.timestamp.strftime("%Y_%m_%d_%H_%M")
        return f"{ts}_{self.service}_{self.data_type}.jsonl"

    @property
    def item_count(self) -> int:
        """Return number of items fetched."""
        return len(self.data)


@dataclass
class FetchRequest:
    """Unified fetch request for any service."""

    services: List[str] = field(default_factory=list)
    scope: Dict[str, List[str]] = field(default_factory=dict)
    days: int = 1
    limit: int = 50
    destination: Optional[str] = None
    output_dir: Optional[str] = None
    keep_local: bool = False
    timezone: str = "Europe/Paris"


class ServiceAdapter(ABC):
    """Abstract base class for service adapters."""

    @property
    @abstractmethod
    def service_name(self) -> str:
        """Return service identifier (e.g., 'spotify', 'garmin')."""
        pass

    @property
    @abstractmethod
    def available_data_types(self) -> List[str]:
        """Return list of available data types for this service."""
        pass

    @abstractmethod
    def authenticate(self, data_types: List[str]) -> None:
        """
        Authenticate with the service.

        Args:
            data_types: List of data types that will be fetched (for scope selection).
        """
        pass

    @abstractmethod
    def fetch(self, data_type: str, days: int = 1, limit: int = 50) -> FetchResult:
        """
        Fetch data for a specific data type.

        Args:
            data_type: The type of data to fetch.
            days: Number of days of data (for Garmin).
            limit: Maximum items to fetch (for Spotify).

        Returns:
            FetchResult with the fetched data.
        """
        pass

    def fetch_all(
        self, data_types: List[str], days: int = 1, limit: int = 50
    ) -> Iterator[FetchResult]:
        """
        Fetch multiple data types, yielding results.

        Args:
            data_types: List of data types to fetch.
            days: Number of days of data (for Garmin).
            limit: Maximum items to fetch (for Spotify).

        Yields:
            FetchResult for each data type.
        """
        for data_type in data_types:
            yield self.fetch(data_type, days=days, limit=limit)
