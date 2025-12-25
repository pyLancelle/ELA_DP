"""Service adapters for the generic fetcher."""

from .spotify import SpotifyAdapter
from .garmin import GarminAdapter

__all__ = ["SpotifyAdapter", "GarminAdapter"]
