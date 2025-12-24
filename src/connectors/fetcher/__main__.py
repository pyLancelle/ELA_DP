#!/usr/bin/env python3
"""
Generic Data Fetcher
--------------------
Unified CLI for fetching data from multiple services (Spotify, Garmin).
Supports direct upload to GCS or local output.

Usage:
    # Fetch Spotify data
    python -m src.connectors.fetcher --service spotify --scope recently_played --limit 50 --destination gs://bucket/path/

    # Fetch Garmin data
    python -m src.connectors.fetcher --service garmin --scope sleep,steps --days 7 --destination gs://bucket/path/

    # Fetch both services
    python -m src.connectors.fetcher --service spotify,garmin --scope recently_played,sleep --days 7 --limit 50 --destination gs://bucket/

    # List available data types
    python -m src.connectors.fetcher --list-types
"""
import argparse
import logging
import os
import sys
from pathlib import Path
from typing import Dict, List, Set

# Add project root to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from src.connectors.fetcher.adapters import SpotifyAdapter, GarminAdapter
from src.connectors.fetcher.gcs_writer import GCSWriter, LocalWriter


def setup_logging(level: str = "INFO") -> None:
    """Configure logging format and level."""
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s: %(message)s",
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
            logging.debug(f".env file not found at {dotenv_path}")
    except ImportError:
        logging.debug("python-dotenv not installed, skipping .env file loading")


def get_adapter(service: str):
    """Get the appropriate adapter for a service."""
    if service == "spotify":
        return SpotifyAdapter()
    elif service == "garmin":
        return GarminAdapter()
    else:
        raise ValueError(f"Unknown service: {service}")


def auto_detect_service(
    data_type: str, spotify_types: Set[str], garmin_types: Set[str]
) -> str:
    """Auto-detect which service a data type belongs to."""
    if data_type in spotify_types:
        return "spotify"
    elif data_type in garmin_types:
        return "garmin"
    else:
        raise ValueError(
            f"Unknown data type: {data_type}. "
            f"Spotify types: {spotify_types}, Garmin types: {garmin_types}"
        )


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Fetch data from Spotify and/or Garmin and upload to GCS.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Fetch Spotify recently played and upload to GCS
  python -m src.connectors.fetcher --service spotify --scope recently_played --destination gs://ela-dp-dev/spotify/landing/

  # Fetch multiple Garmin metrics for 7 days
  python -m src.connectors.fetcher --service garmin --scope sleep,steps,heart_rate --days 7 --destination gs://ela-dp-dev/garmin/landing/

  # Fetch from both services
  python -m src.connectors.fetcher --service spotify,garmin --scope recently_played,sleep,steps --days 7 --limit 50 --destination gs://ela-dp-dev/

  # Local output (no GCS)
  python -m src.connectors.fetcher --service spotify --scope saved_tracks --output-dir ./output

  # List available data types
  python -m src.connectors.fetcher --list-types
        """,
    )

    # Service selection
    parser.add_argument(
        "--service",
        help="Service(s) to fetch from: spotify, garmin, or both (comma-separated)",
    )

    # Data types to fetch
    parser.add_argument(
        "--scope",
        help="Comma-separated list of data types to fetch",
    )

    # Time/limit parameters
    parser.add_argument(
        "--days",
        type=int,
        default=1,
        help="Number of days of data to fetch (for Garmin, default: 1)",
    )

    parser.add_argument(
        "--limit",
        type=int,
        default=50,
        help="Maximum items to fetch (for Spotify, default: 50)",
    )

    # Output destinations
    parser.add_argument(
        "--destination",
        help="GCS folder path for direct upload (e.g., gs://bucket/path/)",
    )

    parser.add_argument(
        "--output-dir",
        type=Path,
        help="Local directory for output (alternative to --destination)",
    )

    parser.add_argument(
        "--keep-local",
        action="store_true",
        help="Keep local copy when uploading to GCS",
    )

    parser.add_argument(
        "--local-dir",
        type=Path,
        default=None,
        help="Local directory for copies when using --keep-local",
    )

    # General options
    parser.add_argument(
        "--env",
        type=Path,
        default=Path(__file__).parent.parent.parent.parent / ".env",
        help="Path to .env file",
    )

    parser.add_argument(
        "--timezone",
        default="Europe/Paris",
        help="Timezone for timestamps (default: Europe/Paris)",
    )

    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Logging level",
    )

    parser.add_argument(
        "--list-types",
        action="store_true",
        help="List available data types for each service and exit",
    )

    return parser.parse_args()


def list_available_types() -> None:
    """Print available data types for each service."""
    print("\nAvailable data types:\n")

    print("SPOTIFY:")
    spotify = SpotifyAdapter()
    for dt in spotify.available_data_types:
        print(f"  - {dt}")

    print("\nGARMIN:")
    garmin = GarminAdapter()
    for dt in garmin.available_data_types:
        print(f"  - {dt}")

    print()


def main() -> None:
    """Main entry point."""
    args = parse_args()
    setup_logging(args.log_level)

    # Handle --list-types
    if args.list_types:
        list_available_types()
        return

    # Validate required arguments
    if not args.scope:
        logging.error(
            "--scope is required. Use --list-types to see available data types."
        )
        sys.exit(1)

    if not args.destination and not args.output_dir:
        logging.error("Either --destination (GCS) or --output-dir (local) is required.")
        sys.exit(1)

    # Load environment variables
    load_env(args.env)

    # Parse services and scope
    requested_services = set()
    if args.service:
        requested_services = {s.strip().lower() for s in args.service.split(",")}

    scope_list = [s.strip() for s in args.scope.split(",")]

    # Get available types for each service
    spotify_adapter = SpotifyAdapter()
    garmin_adapter = GarminAdapter()
    spotify_types = set(spotify_adapter.available_data_types)
    garmin_types = set(garmin_adapter.available_data_types)

    # Organize scope by service
    scope_by_service: Dict[str, List[str]] = {"spotify": [], "garmin": []}

    for data_type in scope_list:
        if requested_services:
            # User specified services - validate data types belong to them
            if "spotify" in requested_services and data_type in spotify_types:
                scope_by_service["spotify"].append(data_type)
            elif "garmin" in requested_services and data_type in garmin_types:
                scope_by_service["garmin"].append(data_type)
            else:
                # Try auto-detection
                try:
                    detected = auto_detect_service(
                        data_type, spotify_types, garmin_types
                    )
                    if detected in requested_services:
                        scope_by_service[detected].append(data_type)
                    else:
                        logging.warning(
                            f"Data type '{data_type}' belongs to '{detected}' "
                            f"but only {requested_services} were requested. Skipping."
                        )
                except ValueError as e:
                    logging.error(str(e))
                    sys.exit(1)
        else:
            # Auto-detect service from data type
            try:
                detected = auto_detect_service(data_type, spotify_types, garmin_types)
                scope_by_service[detected].append(data_type)
            except ValueError as e:
                logging.error(str(e))
                sys.exit(1)

    # Filter to services that have data types to fetch
    active_services = {s for s, types in scope_by_service.items() if types}

    if not active_services:
        logging.error("No valid data types to fetch after filtering.")
        sys.exit(1)

    logging.info(f"Services to fetch: {active_services}")
    for service, types in scope_by_service.items():
        if types:
            logging.info(f"  {service}: {types}")

    # Setup writer
    writer = None
    if args.destination:
        local_dir = args.local_dir if args.keep_local else None
        writer = GCSWriter(
            args.destination, keep_local=args.keep_local, local_dir=local_dir
        )
    elif args.output_dir:
        writer = LocalWriter(args.output_dir)

    # Fetch from each service
    success_count = 0
    error_count = 0

    for service_name in active_services:
        data_types = scope_by_service[service_name]
        adapter = get_adapter(service_name)

        try:
            # Authenticate
            adapter.authenticate(data_types)

            # Fetch each data type
            for result in adapter.fetch_all(
                data_types, days=args.days, limit=args.limit
            ):
                if result.success:
                    if writer:
                        writer.write(result)
                    success_count += 1
                    logging.info(
                        f"[{result.service}] {result.data_type}: {result.item_count} items"
                    )
                else:
                    error_count += 1
                    logging.error(
                        f"[{result.service}] {result.data_type} failed: {result.error}"
                    )

        except Exception as e:
            logging.error(f"Failed to fetch from {service_name}: {e}")
            error_count += len(data_types)

    # Summary
    logging.info(f"Completed: {success_count} successful, {error_count} failed")

    if error_count > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
