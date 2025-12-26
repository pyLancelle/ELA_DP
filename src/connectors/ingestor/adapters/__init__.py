"""
Ingestor adapters for different services.
"""

from src.connectors.ingestor.adapters.garmin import GarminIngestorAdapter
from src.connectors.ingestor.adapters.spotify import SpotifyIngestorAdapter
from src.connectors.ingestor.adapters.chess import ChessIngestorAdapter

__all__ = [
    "GarminIngestorAdapter",
    "SpotifyIngestorAdapter",
    "ChessIngestorAdapter",
]
