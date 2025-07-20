#!/usr/bin/env python3
"""
Spotify Data Connector
---------------------
A comprehensive Spotify data connector that can fetch various types of user data.
Features:
 - Recently played tracks
 - Saved tracks (liked songs)
 - Playlists
 - User profile
 - Environment variables for credentials
 - Automatic token refresh
 - Configurable output and logging
"""
import os
import sys
import argparse
import logging
from datetime import datetime
from zoneinfo import ZoneInfo
from pathlib import Path
from typing import Optional, List, Dict, Any, Union
from dataclasses import dataclass
from enum import Enum

import spotipy
from spotipy.oauth2 import SpotifyOAuth

import sys

sys.path.append(str(Path(__file__).parent.parent))
from utils import to_jsonl

# Constants
DEFAULT_LIMIT = 50
DEFAULT_TIMEZONE = "Europe/Paris"
REQUIRED_ENV_VARS = [
    "SPOTIFY_CLIENT_ID",
    "SPOTIFY_CLIENT_SECRET",
    "SPOTIFY_REDIRECT_URI",
    "SPOTIFY_REFRESH_TOKEN",
]


class DataType(Enum):
    """Supported Spotify data types."""

    RECENTLY_PLAYED = "recently_played"
    SAVED_TRACKS = "saved_tracks"
    SAVED_ALBUMS = "saved_albums"
    FOLLOWED_ARTISTS = "followed_artists"
    PLAYLISTS = "playlists"
    USER_PROFILE = "user_profile"
    TOP_TRACKS = "top_tracks"
    TOP_ARTISTS = "top_artists"


@dataclass
class SpotifyConfig:
    """Configuration for Spotify connector."""

    client_id: str
    client_secret: str
    redirect_uri: str
    refresh_token: str
    cache_path: Path
    timezone: str = DEFAULT_TIMEZONE


class SpotifyConnectorError(Exception):
    """Custom exception for Spotify connector errors."""

    pass


class SpotifyConnector:
    """Spotify data connector with support for multiple data types."""

    # Scopes required for different data types
    SCOPES = {
        DataType.RECENTLY_PLAYED: "user-read-recently-played",
        DataType.SAVED_TRACKS: "user-library-read",
        DataType.SAVED_ALBUMS: "user-library-read",
        DataType.FOLLOWED_ARTISTS: "user-follow-read",
        DataType.PLAYLISTS: "playlist-read-private playlist-read-collaborative",
        DataType.USER_PROFILE: "user-read-private user-read-email",
        DataType.TOP_TRACKS: "user-top-read",
        DataType.TOP_ARTISTS: "user-top-read",
    }

    def __init__(self, config: SpotifyConfig):
        """Initialize the Spotify connector."""
        self.config = config
        self._client: Optional[spotipy.Spotify] = None
        self._authenticated_scopes: set = set()

    def authenticate(self, data_types: List[DataType]) -> None:
        """Authenticate with Spotify for the given data types."""
        required_scopes = set()
        for data_type in data_types:
            if data_type in self.SCOPES:
                required_scopes.update(self.SCOPES[data_type].split())

        scope_string = " ".join(sorted(required_scopes))
        logging.info(f"Authenticating with scopes: {scope_string}")

        try:
            auth_manager = SpotifyOAuth(
                client_id=self.config.client_id,
                client_secret=self.config.client_secret,
                redirect_uri=self.config.redirect_uri,
                scope=scope_string,
                cache_path=str(self.config.cache_path),
            )

            # Clear any existing cache to force re-authentication with new scopes
            if self.config.cache_path.exists():
                self.config.cache_path.unlink()
                logging.debug("Cleared existing token cache")

            # Try to get token using refresh token
            try:
                token_info = auth_manager.refresh_access_token(
                    self.config.refresh_token
                )
                access_token = token_info.get("access_token")
            except Exception as refresh_error:
                logging.warning(f"Refresh token failed: {refresh_error}")
                # If refresh fails, we need to do full OAuth flow
                # For now, raise an error with instructions
                raise SpotifyConnectorError(
                    "Refresh token authentication failed. You may need to re-authorize "
                    "with the new scopes. Please check your refresh token or run the "
                    "initial OAuth flow again."
                )

            if not access_token:
                raise SpotifyConnectorError("Failed to get access token")

            self._client = spotipy.Spotify(auth=access_token)
            self._authenticated_scopes = required_scopes

            # Test the authentication by making a simple API call
            try:
                self._client.current_user()
                logging.info("✅ Authenticated successfully and verified")
            except Exception as test_error:
                raise SpotifyConnectorError(
                    f"Authentication verification failed: {test_error}"
                )

        except Exception as e:
            raise SpotifyConnectorError(f"Authentication failed: {e}") from e

    @property
    def client(self) -> spotipy.Spotify:
        """Get the authenticated Spotify client."""
        if self._client is None:
            raise SpotifyConnectorError("Not authenticated. Call authenticate() first.")
        return self._client

    def fetch_recently_played(self, limit: int = DEFAULT_LIMIT) -> List[Dict[str, Any]]:
        """Fetch recently played tracks."""
        try:
            results = self.client.current_user_recently_played(limit=limit)
            items = results.get("items", [])
            logging.info(f"Fetched {len(items)} recently played tracks")
            return items
        except Exception as e:
            raise SpotifyConnectorError(f"Error fetching recently played: {e}") from e

    def fetch_saved_tracks(self, limit: int = DEFAULT_LIMIT) -> List[Dict[str, Any]]:
        """Fetch user's saved tracks (liked songs)."""
        try:
            items = []
            offset = 0
            batch_size = min(limit, 50)  # Spotify API limit

            while len(items) < limit:
                remaining = limit - len(items)
                current_limit = min(batch_size, remaining)

                results = self.client.current_user_saved_tracks(
                    limit=current_limit, offset=offset
                )
                batch_items = results.get("items", [])

                if not batch_items:
                    break

                items.extend(batch_items)
                offset += len(batch_items)

                # If we got fewer items than requested, we've reached the end
                if len(batch_items) < current_limit:
                    break

            logging.info(f"Fetched {len(items)} saved tracks")
            return items[:limit]  # Ensure we don't exceed the limit

        except Exception as e:
            raise SpotifyConnectorError(f"Error fetching saved tracks: {e}") from e

    def fetch_saved_albums(self, limit: int = DEFAULT_LIMIT) -> List[Dict[str, Any]]:
        """Fetch user's saved albums."""
        try:
            items = []
            offset = 0
            batch_size = min(limit, 50)  # Spotify API limit

            while len(items) < limit:
                remaining = limit - len(items)
                current_limit = min(batch_size, remaining)

                results = self.client.current_user_saved_albums(
                    limit=current_limit, offset=offset
                )
                batch_items = results.get("items", [])

                if not batch_items:
                    break

                items.extend(batch_items)
                offset += len(batch_items)

                # If we got fewer items than requested, we've reached the end
                if len(batch_items) < current_limit:
                    break

            logging.info(f"Fetched {len(items)} saved albums")
            return items[:limit]  # Ensure we don't exceed the limit

        except Exception as e:
            raise SpotifyConnectorError(f"Error fetching saved albums: {e}") from e

    def fetch_followed_artists(
        self, limit: int = DEFAULT_LIMIT
    ) -> List[Dict[str, Any]]:
        """Fetch user's followed artists."""
        try:
            items = []
            after = None
            batch_size = min(limit, 50)  # Spotify API limit

            while len(items) < limit:
                remaining = limit - len(items)
                current_limit = min(batch_size, remaining)

                results = self.client.current_user_followed_artists(
                    limit=current_limit, after=after
                )

                # Extract artists from the response structure
                artists_data = results.get("artists", {})
                batch_items = artists_data.get("items", [])

                if not batch_items:
                    break

                items.extend(batch_items)

                # Get cursor for next page
                cursors = artists_data.get("cursors", {})
                after = cursors.get("after")

                # If no next cursor, we've reached the end
                if not after or len(batch_items) < current_limit:
                    break

            logging.info(f"Fetched {len(items)} followed artists")
            return items[:limit]  # Ensure we don't exceed the limit

        except Exception as e:
            raise SpotifyConnectorError(f"Error fetching followed artists: {e}") from e

    def fetch_playlists(self, limit: int = DEFAULT_LIMIT) -> List[Dict[str, Any]]:
        """Fetch user's playlists."""
        try:
            results = self.client.current_user_playlists(limit=limit)
            items = results.get("items", [])
            logging.info(f"Fetched {len(items)} playlists")
            return items
        except Exception as e:
            raise SpotifyConnectorError(f"Error fetching playlists: {e}") from e

    def fetch_user_profile(self) -> Dict[str, Any]:
        """Fetch user profile information."""
        try:
            profile = self.client.current_user()
            logging.info("Fetched user profile")
            return profile
        except Exception as e:
            raise SpotifyConnectorError(f"Error fetching user profile: {e}") from e

    def fetch_top_tracks(
        self, limit: int = DEFAULT_LIMIT, time_range: str = "medium_term"
    ) -> List[Dict[str, Any]]:
        """Fetch user's top tracks."""
        try:
            results = self.client.current_user_top_tracks(
                limit=limit, time_range=time_range
            )
            items = results.get("items", [])
            logging.info(f"Fetched {len(items)} top tracks ({time_range})")
            return items
        except Exception as e:
            raise SpotifyConnectorError(f"Error fetching top tracks: {e}") from e

    def fetch_top_artists(
        self, limit: int = DEFAULT_LIMIT, time_range: str = "medium_term"
    ) -> List[Dict[str, Any]]:
        """Fetch user's top artists."""
        try:
            results = self.client.current_user_top_artists(
                limit=limit, time_range=time_range
            )
            items = results.get("items", [])
            logging.info(f"Fetched {len(items)} top artists ({time_range})")
            return items
        except Exception as e:
            raise SpotifyConnectorError(f"Error fetching top artists: {e}") from e

    def fetch_data(
        self, data_type: DataType, **kwargs
    ) -> Union[List[Dict[str, Any]], Dict[str, Any]]:
        """Generic method to fetch data by type."""
        method_map = {
            DataType.RECENTLY_PLAYED: self.fetch_recently_played,
            DataType.SAVED_TRACKS: self.fetch_saved_tracks,
            DataType.SAVED_ALBUMS: self.fetch_saved_albums,
            DataType.FOLLOWED_ARTISTS: self.fetch_followed_artists,
            DataType.PLAYLISTS: self.fetch_playlists,
            DataType.USER_PROFILE: self.fetch_user_profile,
            DataType.TOP_TRACKS: self.fetch_top_tracks,
            DataType.TOP_ARTISTS: self.fetch_top_artists,
        }

        if data_type not in method_map:
            raise SpotifyConnectorError(f"Unsupported data type: {data_type}")

        method = method_map[data_type]
        return method(**kwargs)


def setup_logging(level: str = "INFO") -> None:
    """Configure logging format and level."""
    fmt = "%(asctime)s %(levelname)s: %(message)s"
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format=fmt,
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def load_env(dotenv_path: Path) -> None:
    """Load environment variables from .env file."""
    try:
        from dotenv import load_dotenv

        if dotenv_path.exists():
            load_dotenv(dotenv_path)
            logging.debug(f"Loaded .env from {dotenv_path}")
        else:
            logging.warning(f".env file not found at {dotenv_path}")
    except ImportError:
        logging.warning("python-dotenv not installed, skipping .env file loading")


def validate_env_vars() -> Dict[str, str]:
    """Validate and return required environment variables."""
    missing_vars = []
    env_vars = {}

    for var in REQUIRED_ENV_VARS:
        value = os.getenv(var)
        if not value:
            missing_vars.append(var)
        else:
            env_vars[var] = value

    if missing_vars:
        raise SpotifyConnectorError(
            f"Missing required environment variables: {', '.join(missing_vars)}"
        )

    return env_vars


def write_jsonl(
    data: Union[List[Dict[str, Any]], Dict[str, Any]], output_path: Path
) -> None:
    """Write data to a JSONL file."""
    try:
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # If it's a single dict (like user profile), wrap it in a list
        if isinstance(data, dict):
            data = [data]

        to_jsonl(data, jsonl_output_path=str(output_path))
        logging.info(f"📁 Dump saved to: {output_path} ({len(data)} items)")
    except Exception as e:
        raise SpotifyConnectorError(f"Failed to write JSONL file: {e}") from e


def write_latest_pointer(
    latest_file: Path, pointer_file: Optional[Path] = None
) -> None:
    """Write the latest dump filename to a pointer file."""
    if pointer_file is None:
        pointer_file = latest_file.parent / "latest_spotify_dump.txt"

    try:
        pointer_file.write_text(str(latest_file.name))
        logging.debug(f"Updated pointer file: {pointer_file}")
    except Exception as e:
        logging.warning(f"Failed to write latest pointer file: {e}")


def generate_output_filename(
    output_dir: Path, data_type: DataType, timezone: str = DEFAULT_TIMEZONE
) -> Path:
    """Generate timestamped output filename."""
    tz = ZoneInfo(timezone)
    timestamp = datetime.now(tz=tz).strftime("%Y_%m_%d_%H_%M")
    return output_dir / f"{timestamp}_spotify_{data_type.value}.jsonl"


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Fetch and dump Spotify user data.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "data_type", choices=[dt.value for dt in DataType], help="Type of data to fetch"
    )
    parser.add_argument(
        "-l",
        "--limit",
        type=int,
        default=DEFAULT_LIMIT,
        help="Number of items to fetch",
    )
    parser.add_argument(
        "-o",
        "--output-dir",
        type=Path,
        default=Path("."),
        help="Directory to save JSONL dumps",
    )
    parser.add_argument(
        "-e",
        "--env",
        type=Path,
        default=Path(__file__).parent.parent.parent.parent / ".env",
        help="Path to the .env file",
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Logging level",
    )
    parser.add_argument(
        "--timezone", default=DEFAULT_TIMEZONE, help="Timezone for timestamps"
    )
    parser.add_argument(
        "--no-pointer", action="store_true", help="Skip writing the latest pointer file"
    )
    parser.add_argument(
        "--time-range",
        choices=["short_term", "medium_term", "long_term"],
        default="medium_term",
        help="Time range for top tracks/artists (4 weeks, 6 months, or all time)",
    )

    return parser.parse_args()


def main() -> None:
    """Main entry point."""
    try:
        args = parse_args()
        setup_logging(args.log_level)

        # Load environment variables
        load_env(args.env)
        env_vars = validate_env_vars()

        # Create config and connector
        cache_path = Path(__file__).parent.parent / ".spotify-cache"
        config = SpotifyConfig(
            client_id=env_vars["SPOTIFY_CLIENT_ID"],
            client_secret=env_vars["SPOTIFY_CLIENT_SECRET"],
            redirect_uri=env_vars["SPOTIFY_REDIRECT_URI"],
            refresh_token=env_vars["SPOTIFY_REFRESH_TOKEN"],
            cache_path=cache_path,
            timezone=args.timezone,
        )

        connector = SpotifyConnector(config)
        data_type = DataType(args.data_type)

        # Authenticate for the specific data type
        try:
            connector.authenticate([data_type])
        except SpotifyConnectorError as auth_error:
            logging.error(f"Authentication failed: {auth_error}")
            logging.info("💡 Troubleshooting tips:")
            logging.info("1. Your refresh token might not have the required scopes")
            logging.info("2. Try regenerating your refresh token with all scopes:")
            logging.info(
                f"   Required scope for {data_type.value}: {connector.SCOPES.get(data_type, 'unknown')}"
            )
            logging.info("3. Check if your Spotify app has the right permissions")
            sys.exit(1)

        # Fetch data with appropriate parameters
        fetch_kwargs = {"limit": args.limit}
        if data_type in [DataType.TOP_TRACKS, DataType.TOP_ARTISTS]:
            fetch_kwargs["time_range"] = args.time_range

        data = connector.fetch_data(data_type, **fetch_kwargs)

        if not data or (isinstance(data, list) and len(data) == 0):
            logging.warning(f"No {data_type.value} found.")
            return

        # Write output
        output_file = generate_output_filename(
            args.output_dir, data_type, args.timezone
        )
        write_jsonl(data, output_file)

        if not args.no_pointer:
            write_latest_pointer(output_file)

        logging.info("✅ Script completed successfully")

    except SpotifyConnectorError as e:
        logging.error(f"Spotify connector error: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        logging.info("Script interrupted by user")
        sys.exit(130)
    except Exception as e:
        logging.error(f"Unexpected error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
