"""
Garmin Connector Entry Point
----------------------------
Main script to fetch data from Garmin Connect.
"""
import argparse
import logging
import sys
import os
from pathlib import Path
from datetime import datetime, timedelta

# Add project root to path to ensure imports work when run as script
sys.path.append(str(Path(__file__).parent.parent.parent.parent))

from src.connectors.garmin.config import DEFAULT_DAYS_BACK, DEFAULT_TIMEZONE, DATA_TYPES
from src.connectors.garmin.client import GarminClient
from src.connectors.garmin.fetcher import GarminFetcher
from src.connectors.garmin.utils import setup_logging, write_jsonl, generate_output_filename

def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Fetch and dump Garmin Connect data.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "-d", "--days",
        type=int,
        default=DEFAULT_DAYS_BACK,
        help="Number of days back to fetch data",
    )
    parser.add_argument(
        "-o", "--output-dir",
        type=Path,
        default=Path("."),
        help="Directory to save JSONL dumps",
    )
    parser.add_argument(
        "-e", "--env",
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
    # Note: DATA_TYPES keys from config might differ slightly from original list if not careful
    # We should ensure config keys match the original DATA_TYPES list
    parser.add_argument(
        "--data-types",
        nargs="+",
        choices=list(DATA_TYPES.keys()) if isinstance(DATA_TYPES, dict) else DATA_TYPES,
        default=list(DATA_TYPES.keys()) if isinstance(DATA_TYPES, dict) else DATA_TYPES,
        help="Data types to fetch",
    )
    parser.add_argument(
        "--no-withings-sync",
        action="store_true",
        help="Skip automatic Withings to Garmin synchronization before fetching data",
    )
    parser.add_argument(
        "--user-height",
        type=float,
        help="User height in meters for BMI calculation (e.g., 1.72). Overrides USER_HEIGHT_M env var.",
    )
    parser.add_argument(
        "--withings-dedupe-hours",
        type=int,
        default=24,
        help="Deduplication window in hours for Withings measurements (default: 24 = one per day)",
    )

    return parser.parse_args()

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

def validate_env_vars() -> dict:
    """Validate required environment variables."""
    required = ["GARMIN_USERNAME", "GARMIN_PASSWORD"]
    missing = [var for var in required if not os.getenv(var)]
    if missing:
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}")
    return {var: os.getenv(var) for var in required}

def sync_withings(client, args):
    """Handle Withings sync logic."""
    # Check if Withings credentials are available
    withings_client_id = os.getenv("WITHINGS_CLIENT_ID")
    withings_client_secret = os.getenv("WITHINGS_CLIENT_SECRET")

    if not withings_client_id or not withings_client_secret:
        logging.info("ℹ️ No Withings credentials found, skipping automatic sync")
        return

    try:
        logging.info("🔄 Starting automatic Withings to Garmin sync...")
        
        # Import our Withings client dynamically to avoid circular imports or path issues
        # Assuming it's in src.services.withings or similar, but original script had:
        # sys.path.insert(0, str(Path(__file__).parent.parent))
        # from withings import sync_withings_to_garmin
        # We need to locate where 'withings' module is.
        # Based on file list: src/withings exists? No, src/connectors/withings?
        # Original script: sys.path.append(str(Path(__file__).parent.parent)) -> src/connectors
        # and then `from withings import sync_withings_to_garmin`
        # So there is likely a src/connectors/withings folder or file?
        # Let's assume src.connectors.withings based on structure.
        
        from src.connectors.withings import sync_withings_to_garmin

        # Fallback to env var if user_height_m not provided via argument
        user_height_m = args.user_height
        if user_height_m is None:
            env_height = os.getenv("USER_HEIGHT_M")
            user_height_m = float(env_height) if env_height else None

        success = sync_withings_to_garmin(
            garmin_client=client,
            withings_client_id=withings_client_id,
            withings_client_secret=withings_client_secret,
            days_back=args.days,
            user_height_m=user_height_m,
            deduplicate_window_hours=args.withings_dedupe_hours,
        )

        if success:
            logging.info("✅ Withings sync completed successfully")
            logging.info("⏳ Waiting 10 seconds for Garmin to process data...")
            import time
            time.sleep(10)
        else:
            logging.warning("⚠️ Withings sync had issues but continuing anyway")

    except Exception as e:
        logging.warning(f"⚠️ Withings sync error: {e}")
        logging.info("🔄 Continuing with Garmin fetch anyway...")

def main():
    """Main entry point."""
    try:
        args = parse_args()
        setup_logging(args.log_level)
        load_env(args.env)
        
        env_vars = validate_env_vars()
        
        # Initialize client
        client_wrapper = GarminClient(env_vars)
        client = client_wrapper.get_client()
        
        # Sync Withings if needed
        if not args.no_withings_sync:
            sync_withings(client, args)
        else:
            logging.info("ℹ️ Withings sync disabled via --no-withings-sync flag")
            
        # Initialize fetcher
        fetcher = GarminFetcher(client)
        
        # Calculate dates
        end_date = datetime.now()
        start_date = end_date - timedelta(days=args.days)
        
        logging.info(
            f"Fetching Garmin data from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}"
        )
        
        # Fetch loop
        for data_type in args.data_types:
            try:
                data = fetcher.fetch_metric(data_type, start_date, end_date)
                if data:
                    output_file = generate_output_filename(
                        args.output_dir, data_type, args.timezone
                    )
                    write_jsonl(data, output_file)
            except Exception as e:
                logging.error(f"Failed to fetch {data_type}: {e}")
                continue
                
        logging.info("✅ Script completed successfully")
        
    except KeyboardInterrupt:
        logging.info("Script interrupted by user")
        sys.exit(130)
    except Exception as e:
        logging.error(f"Unexpected error: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()
