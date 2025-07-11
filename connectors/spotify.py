#!/usr/bin/env python3
"""
Optimised Spotify Recently Played Connector
-----------------------------------------
This script fetches the user's recently played tracks on Spotify and dumps them into a JSONL file.
Features:
 - Environment variables for credentials
 - Automatic token refresh without interactive prompt
 - Configurable output directory, item limit and logging
 - Clean code structure with functions and CLI
"""
import os
import sys
import argparse
import logging
from datetime import datetime
from zoneinfo import ZoneInfo
from pathlib import Path

import spotipy
from spotipy.oauth2 import SpotifyOAuth

from utils import to_jsonl  # Custom JSONL export function

# Constants
default_scope = "user-read-recently-played"
default_limit = 50
default_timezone = "Europe/Paris"


def setup_logging(level: str = "INFO") -> None:
    """Configure logging format and level."""
    fmt = "%(asctime)s %(levelname)s: %(message)s"
    logging.basicConfig(level=level.upper(), format=fmt, datefmt="%Y-%m-%d %H:%M:%S")


def load_env(dotenv_path: Path) -> None:
    """Load environment variables from .env file."""
    from dotenv import load_dotenv

    if dotenv_path.exists():
        load_dotenv(dotenv_path)
        logging.debug(f"Loaded .env from {dotenv_path}")
    else:
        logging.warning(f".env file not found at {dotenv_path}")


def get_spotify_client(
    cache_path: Path,
    refresh_token: str,
    client_id: str,
    client_secret: str,
    redirect_uri: str,
    scope: str,
) -> spotipy.Spotify:
    """Authenticate to Spotify and return a Spotify client."""
    auth_manager = SpotifyOAuth(
        client_id=client_id,
        client_secret=client_secret,
        redirect_uri=redirect_uri,
        scope=scope,
        cache_path=str(cache_path),
    )
    # Inject refresh token to cache for silent auth
    auth_manager.cache_handler.save_token_to_cache({"refresh_token": refresh_token})
    # Refresh access token
    token_info = auth_manager.refresh_access_token(refresh_token)
    access_token = token_info.get("access_token")
    if not access_token:
        logging.error("Failed to refresh Spotify access token.")
        sys.exit(1)
    logging.info("✅ Authenticated via refresh_token")
    return spotipy.Spotify(auth=access_token)


def fetch_recently_played(sp: spotipy.Spotify, limit: int) -> list:
    """Fetch the user's recently played tracks."""
    try:
        results = sp.current_user_recently_played(limit=limit)
        return results.get("items", [])
    except Exception as e:
        logging.error(f"Error fetching recently played tracks: {e}")
        sys.exit(1)


def write_jsonl(data: list, output_path: Path) -> None:
    """Write a list of dicts to a JSONL file."""
    try:
        to_jsonl(data, jsonl_output_path=str(output_path))
        logging.info(f"📁 Dump saved to: {output_path}")
    except Exception as e:
        logging.error(f"Failed to write JSONL file: {e}")
        sys.exit(1)


def write_latest_pointer(
    latest_file: Path, pointer_file: Path = Path("latest_spotify_dump.txt")
) -> None:
    """Write the latest dump filename to a pointer file."""
    try:
        pointer_file.write_text(str(latest_file.name))
        logging.debug(f"Updated pointer file: {pointer_file}")
    except Exception as e:
        logging.error(f"Failed to write latest pointer file: {e}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch and dump Spotify recently played tracks."
    )
    parser.add_argument(
        "-l",
        "--limit",
        type=int,
        default=default_limit,
        help=f"Number of items to fetch (default: {default_limit})",
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
        default=Path(__file__).parent.parent / ".env",
        help="Path to the .env file",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        help="Logging level (DEBUG, INFO, WARNING, ERROR)",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    setup_logging(args.log_level)

    load_env(args.env)

    # Read credentials
    CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID")
    CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")
    REDIRECT_URI = os.getenv("SPOTIFY_REDIRECT_URI")
    REFRESH_TOKEN = os.getenv("SPOTIFY_REFRESH_TOKEN")
    if not all([CLIENT_ID, CLIENT_SECRET, REDIRECT_URI, REFRESH_TOKEN]):
        logging.error(
            "Missing SPOTIFY_CLIENT_ID/SECRET/REDIRECT_URI/REFRESH_TOKEN in environment"
        )
        sys.exit(1)

    cache_path = Path(__file__).parent.parent / ".spotify-cache"
    sp_client = get_spotify_client(
        cache_path=cache_path,
        refresh_token=REFRESH_TOKEN,
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        redirect_uri=REDIRECT_URI,
        scope=default_scope,
    )

    items = fetch_recently_played(sp_client, args.limit)
    if not items:
        logging.warning("No recently played items found.")

    # Prepare output filename with timezone
    tz = ZoneInfo(default_timezone)
    timestamp = datetime.now(tz=tz).strftime("%Y_%m_%d_%H_%M")
    output_file = args.output_dir / f"{timestamp}_spotify_recently_played.jsonl"

    write_jsonl(items, output_file)
    write_latest_pointer(output_file)


if __name__ == "__main__":
    main()
