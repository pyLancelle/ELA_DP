"""
Module de service mail modulaire pour ELA DataPlatform
"""

from .core import (
    EmailConfig,
    EmailService,
    SpotifyData,
    SpotifyArtist,
    SpotifyTrack,
    BigQueryConfig,
)
from .services import SpotifyEmailService
from .providers import BigQueryProvider
from .templates import SpotifyEmailTemplate

__all__ = [
    "EmailConfig",
    "EmailService",
    "SpotifyData",
    "SpotifyArtist",
    "SpotifyTrack",
    "BigQueryConfig",
    "SpotifyEmailService",
    "BigQueryProvider",
    "SpotifyEmailTemplate",
]

__version__ = "1.0.0"
