#!/usr/bin/env python3
"""
Chess.com Data Connector
------------------------
This script fetches comprehensive data from Chess.com and dumps it into JSONL files.
Features:
 - No authentication required (public API)
 - Player profile, statistics, and game history
 - Configurable date range and data types
 - Clean code structure with functions and CLI
"""
import sys
import argparse
import logging
import requests
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from pathlib import Path
from typing import List, Dict, Any
from dataclasses import dataclass
from enum import Enum

sys.path.append(str(Path(__file__).parent.parent))
from utils import to_jsonl

# Constants
DEFAULT_DAYS_BACK = 30
DEFAULT_TIMEZONE = "Europe/Paris"
BASE_URL = "https://api.chess.com/pub"
RATE_LIMIT_DELAY = 1.0  # Seconds between API calls


# Data types to fetch
class DataType(Enum):
    """Supported Chess.com data types."""

    PLAYER_PROFILE = "player_profile"
    PLAYER_STATS = "player_stats"
    GAMES = "games"


@dataclass
class ChessComConfig:
    """Configuration for Chess.com connector."""

    username: str
    output_dir: Path
    timezone: str = DEFAULT_TIMEZONE
    rate_limit_delay: float = RATE_LIMIT_DELAY


class ChessComConnectorError(Exception):
    """Custom exception for Chess.com connector errors."""

    pass


class ChessComConnector:
    """Chess.com data connector with support for multiple data types."""

    def __init__(self, config: ChessComConfig):
        """Initialize the Chess.com connector."""
        self.config = config
        self.session = requests.Session()
        self.session.headers.update(
            {"User-Agent": "ELA-DataPlatform/1.0 (https://github.com/ela-dataplatform)"}
        )

    def _make_request(self, endpoint: str) -> Dict[str, Any]:
        """Make a rate-limited request to Chess.com API."""
        url = f"{BASE_URL}/{endpoint.lstrip('/')}"

        try:
            logging.debug(f"Making request to: {url}")
            response = self.session.get(url)
            response.raise_for_status()

            # Rate limiting
            time.sleep(self.config.rate_limit_delay)

            return response.json()

        except requests.exceptions.HTTPError as e:
            if response.status_code == 404:
                logging.warning(f"Resource not found: {url}")
                return {}
            raise ChessComConnectorError(f"HTTP error {response.status_code}: {e}")
        except requests.exceptions.RequestException as e:
            raise ChessComConnectorError(f"Request failed: {e}")
        except ValueError as e:
            raise ChessComConnectorError(f"JSON decode error: {e}")

    def fetch_player_profile(self) -> Dict[str, Any]:
        """Fetch player profile information."""
        try:
            profile = self._make_request(f"player/{self.config.username}")
            if profile:
                profile["data_type"] = "player_profile"
                profile["fetch_timestamp"] = datetime.now().isoformat()
                logging.info(f"Fetched profile for {self.config.username}")
            return profile
        except Exception as e:
            logging.error(f"Error fetching player profile: {e}")
            return {}

    def fetch_player_stats(self) -> Dict[str, Any]:
        """Fetch player statistics."""
        try:
            stats = self._make_request(f"player/{self.config.username}/stats")
            if stats:
                stats["data_type"] = "player_stats"
                stats["fetch_timestamp"] = datetime.now().isoformat()
                logging.info(f"Fetched stats for {self.config.username}")
            return stats
        except Exception as e:
            logging.error(f"Error fetching player stats: {e}")
            return {}

    def fetch_games(
        self, start_date: datetime, end_date: datetime
    ) -> List[Dict[str, Any]]:
        """Fetch games within date range."""
        try:
            # First get archives list
            archives = self._make_request(
                f"player/{self.config.username}/games/archives"
            )
            archive_urls = archives.get("archives", [])

            # Extract the actual username from the archive URLs (handles case redirection)
            actual_username = self.config.username
            if archive_urls:
                # Extract username from first archive URL
                import re

                match = re.search(r"/player/([^/]+)/games/", archive_urls[0])
                if match:
                    actual_username = match.group(1)

            games_data = []
            current_date = start_date.replace(day=1)  # Start from first day of month
            logging.debug(f"Available archives: {archive_urls}")

            while current_date <= end_date:
                year_month = current_date.strftime("%Y/%m")
                archive_url = f"{BASE_URL}/player/{actual_username}/games/{year_month}"
                logging.debug(f"Checking for archive: {archive_url}")

                if archive_url in archive_urls:
                    try:
                        month_games = self._make_request(
                            f"player/{actual_username}/games/{year_month}"
                        )
                        games = month_games.get("games", [])

                        # Filter games by date range
                        filtered_games = []
                        logging.debug(
                            f"Processing {len(games)} games from {year_month}"
                        )
                        for game in games:
                            game_date = datetime.fromtimestamp(game.get("end_time", 0))
                            # Convert to timezone-naive for comparison
                            start_date_naive = start_date.replace(tzinfo=None)
                            end_date_naive = end_date.replace(tzinfo=None)
                            logging.debug(
                                f"Game date: {game_date}, Range: {start_date_naive} to {end_date_naive}"
                            )
                            if start_date_naive <= game_date <= end_date_naive:
                                game["data_type"] = "games"
                                game["fetch_timestamp"] = datetime.now().isoformat()
                                filtered_games.append(game)

                        games_data.extend(filtered_games)
                        logging.info(
                            f"Fetched {len(filtered_games)} games from {year_month}"
                        )

                    except Exception as e:
                        logging.warning(f"Could not fetch games for {year_month}: {e}")

                # Move to next month
                if current_date.month == 12:
                    current_date = current_date.replace(
                        year=current_date.year + 1, month=1
                    )
                else:
                    current_date = current_date.replace(month=current_date.month + 1)

            logging.info(f"Fetched total of {len(games_data)} games")
            return games_data

        except Exception as e:
            logging.error(f"Error fetching games: {e}")
            return []

    def fetch_data(self, data_type: DataType, **kwargs) -> List[Dict[str, Any]]:
        """Generic method to fetch data by type."""
        method_map = {
            DataType.PLAYER_PROFILE: lambda: (
                [self.fetch_player_profile()] if self.fetch_player_profile() else []
            ),
            DataType.PLAYER_STATS: lambda: (
                [self.fetch_player_stats()] if self.fetch_player_stats() else []
            ),
            DataType.GAMES: lambda: self.fetch_games(**kwargs),
        }

        if data_type not in method_map:
            raise ChessComConnectorError(f"Unsupported data type: {data_type}")

        method = method_map[data_type]
        return method()


def setup_logging(level: str = "INFO") -> None:
    """Configure logging format and level."""
    fmt = "%(asctime)s %(levelname)s: %(message)s"
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format=fmt,
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def write_jsonl(data: List[Dict[str, Any]], output_path: Path) -> None:
    """Write a list of dicts to a JSONL file."""
    try:
        if not data:
            logging.warning(f"No data to write for {output_path}")
            return

        output_path.parent.mkdir(parents=True, exist_ok=True)
        to_jsonl(data, jsonl_output_path=str(output_path))
        logging.info(f"📁 Dump saved to: {output_path} ({len(data)} items)")
    except Exception as e:
        raise ChessComConnectorError(f"Failed to write JSONL file: {e}") from e


def generate_output_filename(
    output_dir: Path,
    data_type: DataType,
    username: str,
    timezone: str = DEFAULT_TIMEZONE,
) -> Path:
    """Generate timestamped output filename."""
    tz = ZoneInfo(timezone)
    timestamp = datetime.now(tz=tz).strftime("%Y_%m_%d_%H_%M")
    return output_dir / f"{timestamp}_chess_{username}_{data_type.value}.jsonl"


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Fetch and dump Chess.com player data.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("username", help="Chess.com username to fetch data for")
    parser.add_argument(
        "-d",
        "--days",
        type=int,
        default=DEFAULT_DAYS_BACK,
        help="Number of days back to fetch game data",
    )
    parser.add_argument(
        "-o",
        "--output-dir",
        type=Path,
        default=Path("."),
        help="Directory to save JSONL dumps",
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
        "--data-types",
        nargs="+",
        choices=[dt.value for dt in DataType],
        default=[dt.value for dt in DataType],
        help="Data types to fetch",
    )
    parser.add_argument(
        "--rate-limit",
        type=float,
        default=RATE_LIMIT_DELAY,
        help="Delay between API calls in seconds",
    )

    return parser.parse_args()


def main() -> None:
    """Main entry point."""
    try:
        args = parse_args()
        setup_logging(args.log_level)

        # Create config and connector
        config = ChessComConfig(
            username=args.username,
            output_dir=args.output_dir,
            timezone=args.timezone,
            rate_limit_delay=args.rate_limit,
        )

        connector = ChessComConnector(config)

        # Calculate date range for games
        end_date = datetime.now()
        start_date = end_date - timedelta(days=args.days)

        logging.info(
            f"Fetching Chess.com data for {args.username} from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}"
        )

        # Fetch each data type
        for data_type_str in args.data_types:
            data_type = DataType(data_type_str)
            logging.info(f"📊 Fetching {data_type.value} data...")

            try:
                # Prepare kwargs for games data type
                fetch_kwargs = {}
                if data_type == DataType.GAMES:
                    fetch_kwargs = {"start_date": start_date, "end_date": end_date}

                data = connector.fetch_data(data_type, **fetch_kwargs)
                output_file = generate_output_filename(
                    args.output_dir, data_type, args.username, args.timezone
                )
                write_jsonl(data, output_file)

            except Exception as e:
                logging.error(f"Failed to fetch {data_type.value}: {e}")
                continue

        logging.info("✅ Script completed successfully")

    except ChessComConnectorError as e:
        logging.error(f"Chess.com connector error: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        logging.info("Script interrupted by user")
        sys.exit(130)
    except Exception as e:
        logging.error(f"Unexpected error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
