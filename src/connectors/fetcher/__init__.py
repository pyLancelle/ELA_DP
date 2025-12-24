"""
Generic Data Fetcher
--------------------
Unified connector for fetching data from multiple services (Spotify, Garmin).
Supports direct upload to GCS.
"""

from .base import FetchRequest, FetchResult, ServiceAdapter

__all__ = ["FetchRequest", "FetchResult", "ServiceAdapter"]
