"""
Base classes for the generic ingestor.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional


@dataclass
class IngestResult:
    """Result of a single ingestion operation."""

    service: str
    environment: str
    data_types: List[str]
    timestamp: datetime
    success: bool
    files_ingested: int = 0
    files_failed: int = 0
    error: Optional[str] = None


class IngestorAdapter(ABC):
    """Abstract base class for service ingestors."""

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
    def ingest(
        self,
        env: str,
        data_types: Optional[List[str]] = None,
        dry_run: bool = False,
    ) -> IngestResult:
        """
        Ingest data for specified data types.

        Args:
            env: Environment (dev/prd)
            data_types: List of data types to ingest (None = all available)
            dry_run: If True, validate but don't write to BigQuery

        Returns:
            IngestResult with ingestion status
        """
        pass
