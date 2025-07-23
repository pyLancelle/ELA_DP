#!/usr/bin/env python3
"""
Garmin Connect Data Connector
----------------------------
This script fetches comprehensive data from Garmin Connect and dumps it into JSONL files.
Features:
 - Environment variables for credentials
 - Automatic Withings to Garmin sync before fetching (requires withings-sync package)
 - Automatic token refresh and session management
 - Configurable output directory, date range and logging
 - Comprehensive data extraction: activities, health metrics, sleep, body battery
 - Clean code structure with functions and CLI
"""
import os
import sys
import argparse
import logging
import subprocess
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from pathlib import Path
from typing import Optional, List, Dict, Any

try:
    from garminconnect import Garmin
except ImportError:
    print(
        "Error: garminconnect library not found. Install with: pip install garminconnect"
    )
    sys.exit(1)

import sys

sys.path.append(str(Path(__file__).parent.parent))
from utils import to_jsonl

# Withings sync is now automatic (requires: pip install withings-sync)

# Constants
DEFAULT_DAYS_BACK = 30
DEFAULT_TIMEZONE = "Europe/Paris"
REQUIRED_ENV_VARS = ["GARMIN_USERNAME", "GARMIN_PASSWORD"]

# Data types to fetch
DATA_TYPES = [
    "activities",
    "sleep",
    "steps",
    "heart_rate",
    "body_battery",
    "stress",
    "weight",
    "device_info",
    "training_status",
    "hrv",
    "race_predictions",
    "floors",
]


class GarminConnectorError(Exception):
    """Custom exception for Garmin connector errors."""

    pass


def sync_withings_data(username: str, password: str, days: int = 30) -> bool:
    """
    Automatically sync Withings data to Garmin Connect before fetching.

    Args:
        username: Garmin username
        password: Garmin password
        days: Number of days to sync (for historical data)

    Returns:
        True if sync successful or skipped, False if failed
    """
    # Quick check: is withings-sync even installed?
    try:
        result = subprocess.run(
            ["withings-sync", "--help"],
            capture_output=True,
            text=True,
            timeout=5,  # Reduced timeout for quick check
        )
        if result.returncode != 0:
            logging.info("ℹ️ withings-sync not available, skipping Withings sync")
            return True
    except (subprocess.TimeoutExpired, FileNotFoundError):
        logging.info("ℹ️ withings-sync not installed, skipping Withings sync")
        logging.debug("💡 Install with: pip install withings-sync")
        return True

    # Check if Withings credentials are available
    withings_client_id = os.getenv("WITHINGS_CLIENT_ID")
    withings_client_secret = os.getenv("WITHINGS_CLIENT_SECRET")

    if not withings_client_id or not withings_client_secret:
        logging.info("ℹ️ No Withings credentials found, skipping automatic sync")
        logging.debug(
            "💡 Set WITHINGS_CLIENT_ID and WITHINGS_CLIENT_SECRET to enable sync"
        )
        return True

    # All checks passed, proceed with sync
    try:
        logging.info("🔄 Starting automatic Withings to Garmin sync...")

        # Calculate from date for historical sync
        from_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

        # Build withings-sync command
        cmd = [
            "withings-sync",
            f"--garmin-username={username}",
            f"--garmin-password={password}",
            f"--fromdate={from_date}",
            "--verbose",
        ]

        # Set environment variables for Withings credentials
        env = os.environ.copy()
        env["WITHINGS_CLIENT_ID"] = withings_client_id
        env["WITHINGS_CLIENT_SECRET"] = withings_client_secret
        logging.info("✅ Using Withings credentials from environment")

        # Run withings-sync with timeout
        logging.info(
            f"🔧 Running: withings-sync --garmin-username=XXX --garmin-password=XXX --fromdate={from_date}"
        )

        result = subprocess.run(
            cmd,
            env=env,
            capture_output=True,
            text=True,
            timeout=300,  # 5 minutes timeout
        )

        if result.returncode == 0:
            logging.info("✅ Withings sync completed successfully")
            logging.info(
                "⏳ Waiting 30 seconds for Garmin to process data and reset rate limits..."
            )
            import time

            time.sleep(30)  # Give Garmin time to process data and reset rate limits
            return True
        else:
            logging.warning(f"⚠️ Withings sync failed (exit code {result.returncode})")
            if result.stderr:
                logging.warning(f"   Error: {result.stderr.strip()}")
            if result.stdout:
                logging.info(f"   Output: {result.stdout.strip()}")
            logging.info("🔄 Continuing with Garmin fetch anyway...")
            return True  # Don't fail the entire process

    except subprocess.TimeoutExpired:
        logging.warning("⏰ Withings sync timed out - continuing with Garmin fetch")
        return True
    except Exception as e:
        logging.warning(f"⚠️ Withings sync error: {e}")
        logging.info("🔄 Continuing with Garmin fetch anyway...")
        return True


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
        raise GarminConnectorError(
            f"Missing required environment variables: {', '.join(missing_vars)}"
        )

    return env_vars


def get_garmin_client(env_vars: Dict[str, str]) -> Garmin:
    """Authenticate to Garmin Connect and return a client."""
    try:
        client = Garmin(
            email=env_vars["GARMIN_USERNAME"], password=env_vars["GARMIN_PASSWORD"]
        )
        client.login()
        logging.info("✅ Authenticated to Garmin Connect")
        return client

    except Exception as e:
        raise GarminConnectorError(f"Authentication failed: {e}") from e


def fetch_activities(
    client: Garmin, start_date: datetime, end_date: datetime
) -> List[Dict[str, Any]]:
    """Fetch activities within date range."""
    try:
        activities = client.get_activities_by_date(
            start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")
        )
        logging.info(f"Fetched {len(activities)} activities")
        return activities
    except Exception as e:
        logging.error(f"Error fetching activities: {e}")
        return []


def fetch_sleep_data(
    client: Garmin, start_date: datetime, end_date: datetime
) -> List[Dict[str, Any]]:
    """Fetch sleep data within date range."""
    sleep_data = []
    current_date = start_date

    while current_date <= end_date:
        try:
            date_str = current_date.strftime("%Y-%m-%d")
            sleep_info = client.get_sleep_data(date_str)
            if sleep_info:
                sleep_info["date"] = date_str
                sleep_data.append(sleep_info)
        except Exception as e:
            logging.warning(f"No sleep data for {date_str}: {e}")

        current_date += timedelta(days=1)

    logging.info(f"Fetched sleep data for {len(sleep_data)} days")
    return sleep_data


def fetch_steps_data(
    client: Garmin, start_date: datetime, end_date: datetime
) -> List[Dict[str, Any]]:
    """Fetch daily steps data within date range."""
    steps_data = []
    current_date = start_date

    while current_date <= end_date:
        try:
            date_str = current_date.strftime("%Y-%m-%d")
            steps_info = client.get_steps_data(date_str)
            logging.debug(
                f"Steps data for {date_str}: {type(steps_info)} - {steps_info}"
            )

            if steps_info:
                # Handle the response structure properly
                if isinstance(steps_info, dict):
                    steps_info["date"] = date_str
                    steps_data.append(steps_info)
                elif isinstance(steps_info, list):
                    for step_entry in steps_info:
                        if isinstance(step_entry, dict):
                            step_entry["date"] = date_str
                            steps_data.append(step_entry)
                else:
                    # If it's not dict or list, wrap it
                    steps_data.append({"date": date_str, "data": steps_info})
        except Exception as e:
            logging.warning(f"No steps data for {date_str}: {e}")

        current_date += timedelta(days=1)

    logging.info(f"Fetched steps data for {len(steps_data)} days")
    return steps_data


def fetch_heart_rate_data(
    client: Garmin, start_date: datetime, end_date: datetime
) -> List[Dict[str, Any]]:
    """Fetch heart rate data within date range."""
    hr_data = []
    current_date = start_date

    while current_date <= end_date:
        try:
            date_str = current_date.strftime("%Y-%m-%d")
            # Try both method names as the API might have different methods
            try:
                hr_info = client.get_heart_rate_data(date_str)
            except AttributeError:
                hr_info = client.get_heart_rates(date_str)

            if hr_info:
                if isinstance(hr_info, dict):
                    hr_info["date"] = date_str
                    hr_data.append(hr_info)
                elif isinstance(hr_info, list):
                    for hr_entry in hr_info:
                        if isinstance(hr_entry, dict):
                            hr_entry["date"] = date_str
                            hr_data.append(hr_entry)
                else:
                    hr_data.append({"date": date_str, "data": hr_info})
        except Exception as e:
            logging.warning(f"No heart rate data for {date_str}: {e}")

        current_date += timedelta(days=1)

    logging.info(f"Fetched heart rate data for {len(hr_data)} days")
    return hr_data


def fetch_body_battery_data(
    client: Garmin, start_date: datetime, end_date: datetime
) -> List[Dict[str, Any]]:
    """Fetch body battery data within date range."""
    try:
        start_str = start_date.strftime("%Y-%m-%d")
        end_str = end_date.strftime("%Y-%m-%d")
        bb_data = client.get_body_battery(start_str, end_str)
        logging.info(
            f"Fetched body battery data for {len(bb_data) if isinstance(bb_data, list) else 1} entries"
        )
        return bb_data if isinstance(bb_data, list) else [bb_data] if bb_data else []
    except Exception as e:
        logging.warning(f"No body battery data available: {e}")
        return []


def fetch_stress_data(
    client: Garmin, start_date: datetime, end_date: datetime
) -> List[Dict[str, Any]]:
    """Fetch stress data within date range."""
    stress_data = []
    current_date = start_date

    while current_date <= end_date:
        try:
            date_str = current_date.strftime("%Y-%m-%d")
            stress_info = client.get_all_day_stress(date_str)
            if stress_info:
                stress_info["date"] = date_str
                stress_data.append(stress_info)
        except Exception as e:
            logging.warning(f"No stress data for {date_str}: {e}")

        current_date += timedelta(days=1)

    logging.info(f"Fetched stress data for {len(stress_data)} days")
    return stress_data


def fetch_weight_data(
    client: Garmin, start_date: datetime, end_date: datetime
) -> List[Dict[str, Any]]:
    """Fetch weight data within date range."""
    try:
        weight_data = client.get_weigh_ins(
            start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")
        )

        # Extract individual weight entries from the nested structure
        weight_entries = []
        if isinstance(weight_data, dict) and "dailyWeightSummaries" in weight_data:
            for daily_summary in weight_data["dailyWeightSummaries"]:
                # Add each weight measurement from allWeightMetrics
                if "allWeightMetrics" in daily_summary:
                    for metric in daily_summary["allWeightMetrics"]:
                        # Add the summary date for context
                        metric["summaryDate"] = daily_summary["summaryDate"]
                        weight_entries.append(metric)

            logging.info(
                f"Fetched {len(weight_entries)} weight entries from {len(weight_data.get('dailyWeightSummaries', []))} days"
            )
            return weight_entries
        else:
            # Fallback: return the raw data if it's already in the expected format
            logging.info(f"Fetched weight data (raw format): {type(weight_data)}")
            return weight_data if isinstance(weight_data, list) else [weight_data]

    except Exception as e:
        logging.warning(f"No weight data available: {e}")
        return []


def fetch_device_info(client: Garmin) -> List[Dict[str, Any]]:
    """Fetch device information."""
    try:
        devices = client.get_devices()
        logging.info(f"Fetched {len(devices)} devices")
        return devices
    except Exception as e:
        logging.warning(f"Could not fetch device info: {e}")
        return []


def fetch_training_status_data(
    client: Garmin, start_date: datetime, end_date: datetime
) -> List[Dict[str, Any]]:
    """Fetch training status data within date range."""
    training_data = []
    current_date = start_date

    while current_date <= end_date:
        try:
            date_str = current_date.strftime("%Y-%m-%d")
            training_info = client.get_training_status(date_str)
            if training_info:
                if isinstance(training_info, dict):
                    training_info["date"] = date_str
                    training_data.append(training_info)
                elif isinstance(training_info, list):
                    for training_entry in training_info:
                        if isinstance(training_entry, dict):
                            training_entry["date"] = date_str
                            training_data.append(training_entry)
                else:
                    training_data.append({"date": date_str, "data": training_info})
        except Exception as e:
            logging.warning(f"No training status data for {date_str}: {e}")

        current_date += timedelta(days=1)

    logging.info(f"Fetched training status data for {len(training_data)} days")
    return training_data


def fetch_hrv_data(
    client: Garmin, start_date: datetime, end_date: datetime
) -> List[Dict[str, Any]]:
    """Fetch HRV (Heart Rate Variability) data within date range."""
    hrv_data = []
    current_date = start_date

    while current_date <= end_date:
        try:
            date_str = current_date.strftime("%Y-%m-%d")
            hrv_info = client.get_hrv_data(date_str)
            if hrv_info:
                if isinstance(hrv_info, dict):
                    hrv_info["date"] = date_str
                    hrv_data.append(hrv_info)
                elif isinstance(hrv_info, list):
                    for hrv_entry in hrv_info:
                        if isinstance(hrv_entry, dict):
                            hrv_entry["date"] = date_str
                            hrv_data.append(hrv_entry)
                else:
                    hrv_data.append({"date": date_str, "data": hrv_info})
        except Exception as e:
            logging.warning(f"No HRV data for {date_str}: {e}")

        current_date += timedelta(days=1)

    logging.info(f"Fetched HRV data for {len(hrv_data)} days")
    return hrv_data


def fetch_race_predictions_data(
    client: Garmin, start_date: datetime, end_date: datetime
) -> List[Dict[str, Any]]:
    """Fetch race predictions data."""
    try:
        # Try different approaches to get race predictions
        race_predictions = None

        # First try without parameters (get all current predictions)
        try:
            race_predictions = client.get_race_predictions()
        except Exception as e1:
            logging.debug(f"Failed to get race predictions without params: {e1}")

            # Try with date range
            try:
                start_str = start_date.strftime("%Y-%m-%d")
                end_str = end_date.strftime("%Y-%m-%d")
                race_predictions = client.get_race_predictions(
                    startdate=start_str, enddate=end_str
                )
            except Exception as e2:
                logging.debug(f"Failed to get race predictions with date range: {e2}")

                # Try with just start date
                try:
                    race_predictions = client.get_race_predictions(startdate=start_str)
                except Exception as e3:
                    logging.debug(
                        f"Failed to get race predictions with start date: {e3}"
                    )

        if race_predictions:
            logging.info(
                f"Fetched race predictions data: {len(race_predictions) if isinstance(race_predictions, list) else 1} entries"
            )
            return (
                race_predictions
                if isinstance(race_predictions, list)
                else [race_predictions]
            )
        else:
            logging.info("No race predictions data available")
            return []
    except Exception as e:
        logging.warning(f"No race predictions data available: {e}")
        return []


def fetch_floors_data(
    client: Garmin, start_date: datetime, end_date: datetime
) -> List[Dict[str, Any]]:
    """Fetch floors/elevation data within date range."""
    floors_data = []
    current_date = start_date

    while current_date <= end_date:
        try:
            date_str = current_date.strftime("%Y-%m-%d")
            floors_info = client.get_floors(date_str)
            if floors_info:
                if isinstance(floors_info, dict):
                    floors_info["date"] = date_str
                    floors_data.append(floors_info)
                elif isinstance(floors_info, list):
                    for floors_entry in floors_info:
                        if isinstance(floors_entry, dict):
                            floors_entry["date"] = date_str
                            floors_data.append(floors_entry)
                else:
                    floors_data.append({"date": date_str, "data": floors_info})
        except Exception as e:
            logging.warning(f"No floors data for {date_str}: {e}")

        current_date += timedelta(days=1)

    logging.info(f"Fetched floors data for {len(floors_data)} days")
    return floors_data


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
        raise GarminConnectorError(f"Failed to write JSONL file: {e}") from e


def generate_output_filename(
    output_dir: Path, data_type: str, timezone: str = DEFAULT_TIMEZONE
) -> Path:
    """Generate timestamped output filename."""
    tz = ZoneInfo(timezone)
    timestamp = datetime.now(tz=tz).strftime("%Y_%m_%d_%H_%M")
    return output_dir / f"{timestamp}_garmin_{data_type}.jsonl"


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Fetch and dump Garmin Connect data.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "-d",
        "--days",
        type=int,
        default=DEFAULT_DAYS_BACK,
        help="Number of days back to fetch data",
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
        "--data-types",
        nargs="+",
        choices=DATA_TYPES,
        default=DATA_TYPES,
        help="Data types to fetch",
    )
    parser.add_argument(
        "--no-withings-sync",
        action="store_true",
        help="Skip automatic Withings to Garmin synchronization before fetching data",
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

        # Automatic Withings to Garmin sync (if enabled)
        if not args.no_withings_sync:
            sync_withings_data(
                username=env_vars["GARMIN_USERNAME"],
                password=env_vars["GARMIN_PASSWORD"],
                days=args.days,
            )
        else:
            logging.info("ℹ️ Withings sync disabled via --no-withings-sync flag")

        # Setup Garmin client
        client = get_garmin_client(env_vars)

        # Calculate date range
        end_date = datetime.now()
        start_date = end_date - timedelta(days=args.days)

        logging.info(
            f"Fetching Garmin data from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}"
        )

        # Fetch each data type
        data_fetchers = {
            "activities": lambda: fetch_activities(client, start_date, end_date),
            "sleep": lambda: fetch_sleep_data(client, start_date, end_date),
            "steps": lambda: fetch_steps_data(client, start_date, end_date),
            "heart_rate": lambda: fetch_heart_rate_data(client, start_date, end_date),
            "body_battery": lambda: fetch_body_battery_data(
                client, start_date, end_date
            ),
            "stress": lambda: fetch_stress_data(client, start_date, end_date),
            "weight": lambda: fetch_weight_data(client, start_date, end_date),
            "device_info": lambda: fetch_device_info(client),
            "training_status": lambda: fetch_training_status_data(
                client, start_date, end_date
            ),
            "hrv": lambda: fetch_hrv_data(client, start_date, end_date),
            "race_predictions": lambda: fetch_race_predictions_data(
                client, start_date, end_date
            ),
            "floors": lambda: fetch_floors_data(client, start_date, end_date),
        }

        for data_type in args.data_types:
            logging.info(f"📊 Fetching {data_type} data...")
            try:
                data = data_fetchers[data_type]()
                output_file = generate_output_filename(
                    args.output_dir, data_type, args.timezone
                )
                write_jsonl(data, output_file)
            except Exception as e:
                logging.error(f"Failed to fetch {data_type}: {e}")
                continue

        logging.info("✅ Script completed successfully")

    except GarminConnectorError as e:
        logging.error(f"Garmin connector error: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        logging.info("Script interrupted by user")
        sys.exit(130)
    except Exception as e:
        logging.error(f"Unexpected error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
