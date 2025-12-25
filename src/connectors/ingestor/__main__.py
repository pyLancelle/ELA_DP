#!/usr/bin/env python3
"""
Generic Data Ingestor
---------------------
Unified CLI for ingesting data from multiple services (Spotify, Garmin, Chess) into BigQuery.

Usage:
    # Ingest Garmin data
    python -m src.connectors.ingestor --service garmin --env dev

    # Ingest Spotify data
    python -m src.connectors.ingestor --service spotify --env dev --data-types recently_played,saved_tracks

    # Ingest multiple services
    python -m src.connectors.ingestor --service garmin,spotify --env dev

    # List available data types
    python -m src.connectors.ingestor --list-types
"""
import argparse
import logging
import os
import sys
from pathlib import Path
from typing import Dict, List, Set

# Add project root to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from src.connectors.ingestor.adapters import (
    GarminIngestorAdapter,
    SpotifyIngestorAdapter,
    ChessIngestorAdapter,
)


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
        return SpotifyIngestorAdapter()
    elif service == "garmin":
        return GarminIngestorAdapter()
    elif service == "chess":
        return ChessIngestorAdapter()
    else:
        raise ValueError(f"Unknown service: {service}")


def auto_detect_service(
    data_type: str,
    spotify_types: Set[str],
    garmin_types: Set[str],
    chess_types: Set[str],
) -> str:
    """Auto-detect which service a data type belongs to."""
    if data_type in spotify_types:
        return "spotify"
    elif data_type in garmin_types:
        return "garmin"
    elif data_type in chess_types:
        return "chess"
    else:
        raise ValueError(
            f"Unknown data type: {data_type}. "
            f"Spotify types: {spotify_types}, Garmin types: {garmin_types}, Chess types: {chess_types}"
        )


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Ingest data from Spotify, Garmin, and/or Chess into BigQuery.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Ingest all Garmin data types
  python -m src.connectors.ingestor --service garmin --env dev

  # Ingest specific Spotify data types
  python -m src.connectors.ingestor --service spotify --env dev --data-types recently_played,saved_tracks

  # Ingest from multiple services
  python -m src.connectors.ingestor --service garmin,spotify --env dev

  # Dry run (validate without writing to BigQuery)
  python -m src.connectors.ingestor --service garmin --env dev --dry-run

  # List available data types
  python -m src.connectors.ingestor --list-types
        """,
    )

    # Service selection
    parser.add_argument(
        "--service",
        help="Service(s) to ingest from: spotify, garmin, chess, or multiple (comma-separated)",
    )

    # Environment
    parser.add_argument(
        "--env",
        choices=["dev", "prd"],
        help="Environment (dev/prd)",
    )

    # Data types to ingest
    parser.add_argument(
        "--data-types",
        help="Comma-separated list of data types to ingest (default: all for the service)",
    )

    # General options
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate without writing to BigQuery",
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
    spotify = SpotifyIngestorAdapter()
    for dt in spotify.available_data_types:
        print(f"  - {dt}")

    print("\nGARMIN:")
    garmin = GarminIngestorAdapter()
    for dt in garmin.available_data_types:
        print(f"  - {dt}")

    print("\nCHESS:")
    chess = ChessIngestorAdapter()
    for dt in chess.available_data_types:
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
    if not args.service:
        logging.error(
            "--service is required. Use --list-types to see available services."
        )
        sys.exit(1)

    if not args.env:
        logging.error("--env is required (dev or prd).")
        sys.exit(1)

    # Parse services
    requested_services = {s.strip().lower() for s in args.service.split(",")}

    # Parse data types if specified
    data_types_list = []
    if args.data_types:
        data_types_list = [dt.strip() for dt in args.data_types.split(",")]

    # Get available types for each service
    spotify_adapter = SpotifyIngestorAdapter()
    garmin_adapter = GarminIngestorAdapter()
    chess_adapter = ChessIngestorAdapter()
    spotify_types = set(spotify_adapter.available_data_types)
    garmin_types = set(garmin_adapter.available_data_types)
    chess_types = set(chess_adapter.available_data_types)

    # Organize data types by service
    data_types_by_service: Dict[str, List[str]] = {
        "spotify": [],
        "garmin": [],
        "chess": [],
    }

    if data_types_list:
        # User specified data types - organize by service
        for data_type in data_types_list:
            if requested_services:
                # User specified services - validate data types belong to them
                if "spotify" in requested_services and data_type in spotify_types:
                    data_types_by_service["spotify"].append(data_type)
                elif "garmin" in requested_services and data_type in garmin_types:
                    data_types_by_service["garmin"].append(data_type)
                elif "chess" in requested_services and data_type in chess_types:
                    data_types_by_service["chess"].append(data_type)
                else:
                    # Try auto-detection
                    try:
                        detected = auto_detect_service(
                            data_type, spotify_types, garmin_types, chess_types
                        )
                        if detected in requested_services:
                            data_types_by_service[detected].append(data_type)
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
                    detected = auto_detect_service(
                        data_type, spotify_types, garmin_types, chess_types
                    )
                    data_types_by_service[detected].append(data_type)
                except ValueError as e:
                    logging.error(str(e))
                    sys.exit(1)
    else:
        # No data types specified - use all for requested services
        if "spotify" in requested_services:
            data_types_by_service["spotify"] = None  # None = all
        if "garmin" in requested_services:
            data_types_by_service["garmin"] = None  # None = all
        if "chess" in requested_services:
            data_types_by_service["chess"] = None  # None = all

    # Filter to services that have data types to ingest
    active_services = {
        s
        for s, types in data_types_by_service.items()
        if s in requested_services
        and (types is None or (isinstance(types, list) and len(types) > 0))
    }

    if not active_services:
        logging.error("No valid services or data types to ingest.")
        sys.exit(1)

    logging.info(f"Services to ingest: {active_services}")
    logging.info(f"Environment: {args.env}")
    for service, types in data_types_by_service.items():
        if service in active_services:
            if types:
                logging.info(f"  {service}: {types}")
            else:
                logging.info(f"  {service}: all data types")

    # Ingest from each service
    success_count = 0
    error_count = 0

    for service_name in active_services:
        data_types = data_types_by_service[service_name]
        adapter = get_adapter(service_name)

        try:
            logging.info(f"\n{'='*80}")
            logging.info(f"Starting ingestion for {service_name.upper()}")
            logging.info(f"{'='*80}")

            result = adapter.ingest(
                env=args.env,
                data_types=data_types,
                dry_run=args.dry_run,
            )

            if result.success:
                success_count += 1
                logging.info(
                    f"[{result.service}] Ingestion completed: "
                    f"{result.files_ingested} files ingested, "
                    f"{result.files_failed} files failed"
                )
            else:
                error_count += 1
                logging.error(f"[{result.service}] Ingestion failed: {result.error}")

        except Exception as e:
            logging.error(f"Failed to ingest from {service_name}: {e}", exc_info=True)
            error_count += 1

    # Summary
    logging.info(f"\n{'='*80}")
    logging.info(f"Ingestion Summary")
    logging.info(f"{'='*80}")
    logging.info(f"Successful services: {success_count}")
    logging.info(f"Failed services: {error_count}")

    if error_count > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
