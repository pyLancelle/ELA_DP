"""
Withings Connector Entry Point
-------------------------------
Standalone script to sync Withings body composition data to Garmin Connect.
"""
import argparse
import logging
import sys
import os
from pathlib import Path
from datetime import datetime

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent.parent.parent))

from src.connectors.withings.withings_client import sync_withings_to_garmin
from src.connectors.garmin.client import GarminClient
from src.connectors.garmin.utils import setup_logging

def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Sync Withings body composition data to Garmin Connect.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "-d", "--days",
        type=int,
        default=7,
        help="Number of days back to sync",
    )
    parser.add_argument(
        "--user-height",
        type=float,
        help="User height in meters for BMI calculation (e.g., 1.72). Overrides USER_HEIGHT_M env var.",
    )
    parser.add_argument(
        "--dedupe-hours",
        type=int,
        default=24,
        help="Deduplication window in hours (default: 24 = one measurement per day)",
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Logging level",
    )
    return parser.parse_args()

def load_env() -> None:
    """Load environment variables from .env file."""
    try:
        from dotenv import load_dotenv
        env_path = Path(__file__).parent.parent.parent.parent / ".env"
        if env_path.exists():
            load_dotenv(env_path)
            logging.debug(f"Loaded .env from {env_path}")
    except ImportError:
        logging.warning("python-dotenv not installed, skipping .env file loading")

def validate_env_vars() -> dict:
    """Validate required environment variables."""
    required_garmin = ["GARMIN_USERNAME", "GARMIN_PASSWORD"]
    required_withings = ["WITHINGS_CLIENT_ID", "WITHINGS_CLIENT_SECRET"]
    
    missing = []
    for var in required_garmin + required_withings:
        if not os.getenv(var):
            missing.append(var)
    
    if missing:
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}")
    
    return {
        "garmin": {var: os.getenv(var) for var in required_garmin},
        "withings": {
            "client_id": os.getenv("WITHINGS_CLIENT_ID"),
            "client_secret": os.getenv("WITHINGS_CLIENT_SECRET"),
        }
    }

def main():
    """Main entry point."""
    try:
        args = parse_args()
        setup_logging(args.log_level)
        load_env()
        
        logging.info("🔄 Starting Withings to Garmin sync...")
        
        env_vars = validate_env_vars()
        
        # Initialize Garmin client
        garmin_wrapper = GarminClient(env_vars["garmin"])
        garmin_client = garmin_wrapper.get_client()
        
        # Get user height
        user_height_m = args.user_height
        if user_height_m is None:
            env_height = os.getenv("USER_HEIGHT_M")
            user_height_m = float(env_height) if env_height else None
        
        # Sync Withings to Garmin
        success = sync_withings_to_garmin(
            garmin_client=garmin_client,
            withings_client_id=env_vars["withings"]["client_id"],
            withings_client_secret=env_vars["withings"]["client_secret"],
            days_back=args.days,
            user_height_m=user_height_m,
            deduplicate_window_hours=args.dedupe_hours,
        )
        
        if success:
            logging.info("✅ Withings sync completed successfully")
            sys.exit(0)
        else:
            logging.warning("⚠️ Withings sync had issues")
            sys.exit(1)
            
    except KeyboardInterrupt:
        logging.info("Script interrupted by user")
        sys.exit(130)
    except Exception as e:
        logging.error(f"Unexpected error: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()
