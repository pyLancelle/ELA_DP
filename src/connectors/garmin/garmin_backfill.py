#!/usr/bin/env python3
"""
Garmin Connect Historical Backfill Tool
--------------------------------------
This script performs a safe historical backfill of Garmin Connect data without
disrupting the existing daily pipeline. Designed for one-time extraction of
large date ranges (e.g., Jan 2024 - present) with proper rate limiting.

Features:
 - Monthly batch processing to respect API limits
 - Resume capability for interrupted runs
 - Special file naming to separate from daily pipeline
 - Rate limiting and error handling
 - Progress tracking and detailed logging
 - Safe local-only execution

Usage:
    python garmin_backfill.py --start-date 2024-01-01 --end-date 2024-12-31
    python garmin_backfill.py --start-date 2024-01-01 --end-date 2024-12-31 --resume
"""
import os
import sys
import argparse
import logging
import time
import json
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple
from calendar import monthrange

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

# Constants
DEFAULT_TIMEZONE = "Europe/Paris"
REQUIRED_ENV_VARS = ["GARMIN_USERNAME", "GARMIN_PASSWORD"]
RATE_LIMIT_DELAY = 2  # seconds between API calls
BATCH_DELAY = 10  # seconds between monthly batches

# Data types to fetch (same as regular fetch)
DATA_TYPES = [
    "activities",
    "activity_details",
    "activity_splits",
    "activity_weather",
    "activity_hr_zones",
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
    "endurance_score",
    "hill_score",
]


class GarminBackfillError(Exception):
    """Custom exception for Garmin backfill errors."""

    pass


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
        raise GarminBackfillError(
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
        logging.info("✅ Authenticated to Garmin Connect for backfill")
        return client

    except Exception as e:
        raise GarminBackfillError(f"Authentication failed: {e}") from e


def generate_monthly_batches(
    start_date: date, end_date: date
) -> List[Tuple[date, date]]:
    """Generate monthly date ranges for batch processing."""
    batches = []
    current_date = start_date.replace(day=1)  # Start from first day of month

    while current_date <= end_date:
        # Get last day of current month
        last_day = monthrange(current_date.year, current_date.month)[1]
        month_end = current_date.replace(day=last_day)

        # Use the smaller of month_end or actual end_date
        batch_end = min(month_end, end_date)

        # Use the larger of current_date or actual start_date
        batch_start = max(current_date, start_date)

        batches.append((batch_start, batch_end))

        # Move to first day of next month
        if current_date.month == 12:
            current_date = current_date.replace(
                year=current_date.year + 1, month=1, day=1
            )
        else:
            current_date = current_date.replace(month=current_date.month + 1, day=1)

    return batches


def create_progress_file(output_dir: Path) -> Path:
    """Create progress tracking file."""
    progress_file = output_dir / "backfill_progress.json"
    if not progress_file.exists():
        with open(progress_file, "w") as f:
            json.dump({"completed_batches": [], "failed_batches": []}, f, indent=2)
    return progress_file


def load_progress(progress_file: Path) -> Dict[str, Any]:
    """Load progress from tracking file."""
    try:
        with open(progress_file, "r") as f:
            return json.load(f)
    except Exception as e:
        logging.warning(f"Could not load progress file: {e}")
        return {"completed_batches": [], "failed_batches": []}


def save_progress(progress_file: Path, progress: Dict[str, Any]) -> None:
    """Save progress to tracking file."""
    try:
        with open(progress_file, "w") as f:
            json.dump(progress, f, indent=2, default=str)
        logging.debug(f"Progress saved to {progress_file}")
    except Exception as e:
        logging.error(f"Failed to save progress: {e}")


def generate_historical_filename(
    output_dir: Path, data_type: str, batch_start: date, batch_end: date
) -> Path:
    """Generate filename for historical data."""
    start_str = batch_start.strftime("%Y_%m_%d")
    end_str = batch_end.strftime("%Y_%m_%d")
    return output_dir / f"{start_str}_to_{end_str}_HISTORICAL_garmin_{data_type}.jsonl"


def write_jsonl(data: List[Dict[str, Any]], output_path: Path) -> None:
    """Write a list of dicts to a JSONL file."""
    try:
        if not data:
            logging.warning(f"No data to write for {output_path}")
            return

        output_path.parent.mkdir(parents=True, exist_ok=True)
        to_jsonl(data, jsonl_output_path=str(output_path))
        logging.info(f"📁 Historical dump saved: {output_path} ({len(data)} items)")
    except Exception as e:
        raise GarminBackfillError(f"Failed to write JSONL file: {e}") from e


# Import all fetch functions from the original script
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
            time.sleep(RATE_LIMIT_DELAY)  # Rate limiting
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

            if steps_info:
                if isinstance(steps_info, dict):
                    steps_info["date"] = date_str
                    steps_data.append(steps_info)
                elif isinstance(steps_info, list):
                    for step_entry in steps_info:
                        if isinstance(step_entry, dict):
                            step_entry["date"] = date_str
                            steps_data.append(step_entry)
                else:
                    steps_data.append({"date": date_str, "data": steps_info})
            time.sleep(RATE_LIMIT_DELAY)
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
            time.sleep(RATE_LIMIT_DELAY)
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
            time.sleep(RATE_LIMIT_DELAY)
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
        logging.info(f"Fetched {len(weight_data)} weight entries")
        return weight_data
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
            time.sleep(RATE_LIMIT_DELAY)
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
            time.sleep(RATE_LIMIT_DELAY)
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
        race_predictions = None

        try:
            race_predictions = client.get_race_predictions()
        except Exception as e1:
            logging.debug(f"Failed to get race predictions without params: {e1}")

            try:
                start_str = start_date.strftime("%Y-%m-%d")
                end_str = end_date.strftime("%Y-%m-%d")
                race_predictions = client.get_race_predictions(
                    startdate=start_str, enddate=end_str
                )
            except Exception as e2:
                logging.debug(f"Failed to get race predictions with date range: {e2}")

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
            time.sleep(RATE_LIMIT_DELAY)
        except Exception as e:
            logging.warning(f"No floors data for {date_str}: {e}")

        current_date += timedelta(days=1)

    logging.info(f"Fetched floors data for {len(floors_data)} days")
    return floors_data


def fetch_endurance_score_data(
    client: Garmin, start_date: datetime, end_date: datetime
) -> List[Dict[str, Any]]:
    """Fetch endurance score data within date range."""
    try:
        start_str = start_date.strftime("%Y-%m-%d")
        end_str = end_date.strftime("%Y-%m-%d")
        endurance_data = client.get_endurance_score(start_str, end_str)

        if endurance_data:
            logging.info(
                f"Fetched endurance score data: {len(endurance_data) if isinstance(endurance_data, list) else 1} entries"
            )
            return (
                endurance_data if isinstance(endurance_data, list) else [endurance_data]
            )
        else:
            logging.info("No endurance score data available")
            return []
    except Exception as e:
        logging.warning(f"No endurance score data available: {e}")
        return []


def fetch_hill_score_data(
    client: Garmin, start_date: datetime, end_date: datetime
) -> List[Dict[str, Any]]:
    """Fetch hill score data within date range."""
    try:
        start_str = start_date.strftime("%Y-%m-%d")
        end_str = end_date.strftime("%Y-%m-%d")
        hill_data = client.get_hill_score(start_str, end_str)

        if hill_data:
            logging.info(
                f"Fetched hill score data: {len(hill_data) if isinstance(hill_data, list) else 1} entries"
            )
            return hill_data if isinstance(hill_data, list) else [hill_data]
        else:
            logging.info("No hill score data available")
            return []
    except Exception as e:
        logging.warning(f"No hill score data available: {e}")
        return []


def fetch_activity_details(
    client: Garmin, start_date: datetime, end_date: datetime
) -> List[Dict[str, Any]]:
    """Fetch detailed activity data with GPS tracking."""
    try:
        # First get the basic activities list
        activities = client.get_activities_by_date(
            start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")
        )

        detailed_activities = []
        for activity in activities:
            try:
                activity_id = activity.get("activityId")
                if not activity_id:
                    logging.warning(f"Activity missing activityId: {activity}")
                    continue

                # Get detailed activity data with GPS coordinates
                details = client.get_activity_details(
                    activity_id, maxchart=2000, maxpoly=4000
                )

                # Combine basic info with detailed data
                enriched_activity = {
                    **activity,  # Basic activity info
                    "detailed_data": details,  # GPS and detailed metrics
                    "data_type": "activity_details",
                }
                detailed_activities.append(enriched_activity)

                # Rate limiting to avoid overwhelming the API
                time.sleep(RATE_LIMIT_DELAY)

            except Exception as e:
                logging.warning(
                    f"Could not fetch details for activity {activity.get('activityId', 'unknown')}: {e}"
                )
                continue

        logging.info(f"Fetched detailed data for {len(detailed_activities)} activities")
        return detailed_activities

    except Exception as e:
        logging.error(f"Error fetching activity details: {e}")
        return []


def fetch_activity_splits(
    client: Garmin, start_date: datetime, end_date: datetime
) -> List[Dict[str, Any]]:
    """Fetch activity splits/laps data."""
    try:
        # First get the basic activities list
        activities = client.get_activities_by_date(
            start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")
        )

        all_splits = []
        for activity in activities:
            try:
                activity_id = activity.get("activityId")
                if not activity_id:
                    logging.warning(f"Activity missing activityId: {activity}")
                    continue

                # Get both regular splits and typed splits
                splits = client.get_activity_splits(activity_id)
                typed_splits = client.get_activity_typed_splits(activity_id)
                split_summaries = client.get_activity_split_summaries(activity_id)

                # Combine all splits data
                activity_splits = {
                    "activityId": activity_id,
                    "activityName": activity.get("activityName", ""),
                    "activityType": activity.get("activityType", ""),
                    "startTimeLocal": activity.get("startTimeLocal", ""),
                    "splits": splits,
                    "typed_splits": typed_splits,
                    "split_summaries": split_summaries,
                    "data_type": "activity_splits",
                }
                all_splits.append(activity_splits)

                # Rate limiting
                time.sleep(RATE_LIMIT_DELAY)

            except Exception as e:
                logging.warning(
                    f"Could not fetch splits for activity {activity.get('activityId', 'unknown')}: {e}"
                )
                continue

        logging.info(f"Fetched splits data for {len(all_splits)} activities")
        return all_splits

    except Exception as e:
        logging.error(f"Error fetching activity splits: {e}")
        return []


def fetch_activity_weather(
    client: Garmin, start_date: datetime, end_date: datetime
) -> List[Dict[str, Any]]:
    """Fetch weather data for activities."""
    try:
        # First get the basic activities list
        activities = client.get_activities_by_date(
            start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")
        )

        all_weather = []
        for activity in activities:
            try:
                activity_id = activity.get("activityId")
                if not activity_id:
                    continue

                # Get weather data for this activity
                weather_data = client.get_activity_weather(activity_id)

                if weather_data:
                    activity_weather = {
                        "activityId": activity_id,
                        "activityName": activity.get("activityName", ""),
                        "activityType": activity.get("activityType", ""),
                        "startTimeLocal": activity.get("startTimeLocal", ""),
                        "weather_data": weather_data,
                        "data_type": "activity_weather",
                    }
                    all_weather.append(activity_weather)

                # Rate limiting
                time.sleep(RATE_LIMIT_DELAY)

            except Exception as e:
                logging.warning(
                    f"Could not fetch weather for activity {activity.get('activityId', 'unknown')}: {e}"
                )
                continue

        logging.info(f"Fetched weather data for {len(all_weather)} activities")
        return all_weather

    except Exception as e:
        logging.error(f"Error fetching activity weather: {e}")
        return []


def fetch_activity_hr_zones(
    client: Garmin, start_date: datetime, end_date: datetime
) -> List[Dict[str, Any]]:
    """Fetch heart rate zones data for activities."""
    try:
        # First get the basic activities list
        activities = client.get_activities_by_date(
            start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")
        )

        all_hr_zones = []
        for activity in activities:
            try:
                activity_id = activity.get("activityId")
                if not activity_id:
                    continue

                # Get heart rate zones data
                hr_zones = client.get_activity_hr_in_timezones(activity_id)

                if hr_zones:
                    activity_hr_zones = {
                        "activityId": activity_id,
                        "activityName": activity.get("activityName", ""),
                        "activityType": activity.get("activityType", ""),
                        "startTimeLocal": activity.get("startTimeLocal", ""),
                        "hr_zones_data": hr_zones,
                        "data_type": "activity_hr_zones",
                    }
                    all_hr_zones.append(activity_hr_zones)

                # Rate limiting
                time.sleep(RATE_LIMIT_DELAY)

            except Exception as e:
                logging.warning(
                    f"Could not fetch HR zones for activity {activity.get('activityId', 'unknown')}: {e}"
                )
                continue

        logging.info(f"Fetched HR zones data for {len(all_hr_zones)} activities")
        return all_hr_zones

    except Exception as e:
        logging.error(f"Error fetching activity HR zones: {e}")
        return []


def process_batch(
    client: Garmin,
    batch_start: date,
    batch_end: date,
    data_types: List[str],
    output_dir: Path,
) -> Dict[str, bool]:
    """Process a single monthly batch."""
    logging.info(f"🔄 Processing batch: {batch_start} to {batch_end}")

    start_dt = datetime.combine(batch_start, datetime.min.time())
    end_dt = datetime.combine(batch_end, datetime.max.time())

    results = {}

    # Data fetchers mapping
    data_fetchers = {
        "activities": lambda: fetch_activities(client, start_dt, end_dt),
        "activity_details": lambda: fetch_activity_details(client, start_dt, end_dt),
        "activity_splits": lambda: fetch_activity_splits(client, start_dt, end_dt),
        "activity_weather": lambda: fetch_activity_weather(client, start_dt, end_dt),
        "activity_hr_zones": lambda: fetch_activity_hr_zones(client, start_dt, end_dt),
        "sleep": lambda: fetch_sleep_data(client, start_dt, end_dt),
        "steps": lambda: fetch_steps_data(client, start_dt, end_dt),
        "heart_rate": lambda: fetch_heart_rate_data(client, start_dt, end_dt),
        "body_battery": lambda: fetch_body_battery_data(client, start_dt, end_dt),
        "stress": lambda: fetch_stress_data(client, start_dt, end_dt),
        "weight": lambda: fetch_weight_data(client, start_dt, end_dt),
        "device_info": lambda: fetch_device_info(client),
        "training_status": lambda: fetch_training_status_data(client, start_dt, end_dt),
        "hrv": lambda: fetch_hrv_data(client, start_dt, end_dt),
        "race_predictions": lambda: fetch_race_predictions_data(
            client, start_dt, end_dt
        ),
        "floors": lambda: fetch_floors_data(client, start_dt, end_dt),
        "endurance_score": lambda: fetch_endurance_score_data(client, start_dt, end_dt),
        "hill_score": lambda: fetch_hill_score_data(client, start_dt, end_dt),
    }

    for data_type in data_types:
        try:
            logging.info(
                f"📊 Fetching {data_type} data for {batch_start} to {batch_end}..."
            )
            data = data_fetchers[data_type]()

            output_file = generate_historical_filename(
                output_dir, data_type, batch_start, batch_end
            )
            write_jsonl(data, output_file)
            results[data_type] = True

            # Rate limiting between data types
            time.sleep(RATE_LIMIT_DELAY)

        except Exception as e:
            logging.error(
                f"Failed to fetch {data_type} for batch {batch_start}-{batch_end}: {e}"
            )
            results[data_type] = False

    return results


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Historical backfill of Garmin Connect data.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--start-date",
        type=lambda s: datetime.strptime(s, "%Y-%m-%d").date(),
        required=True,
        help="Start date for backfill (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end-date",
        type=lambda s: datetime.strptime(s, "%Y-%m-%d").date(),
        required=True,
        help="End date for backfill (YYYY-MM-DD)",
    )
    parser.add_argument(
        "-o",
        "--output-dir",
        type=Path,
        default=Path("data/historical/garmin/"),
        help="Directory to save historical JSONL dumps",
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
        "--data-types",
        nargs="+",
        choices=DATA_TYPES,
        default=DATA_TYPES,
        help="Data types to fetch",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume from previous incomplete run",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show batches without executing",
    )

    return parser.parse_args()


def main() -> None:
    """Main entry point for historical backfill."""
    try:
        args = parse_args()
        setup_logging(args.log_level)

        logging.info(f"🚀 Starting Garmin Historical Backfill")
        logging.info(f"📅 Date range: {args.start_date} to {args.end_date}")
        logging.info(f"📁 Output directory: {args.output_dir}")

        # Validate date range
        if args.start_date >= args.end_date:
            raise GarminBackfillError("Start date must be before end date")

        # Create output directory
        args.output_dir.mkdir(parents=True, exist_ok=True)

        # Generate monthly batches
        batches = generate_monthly_batches(args.start_date, args.end_date)
        logging.info(f"📦 Generated {len(batches)} monthly batches")

        if args.dry_run:
            logging.info("🔍 DRY RUN - Showing batches without execution:")
            for i, (batch_start, batch_end) in enumerate(batches, 1):
                logging.info(f"  Batch {i}: {batch_start} to {batch_end}")
            return

        # Load/create progress tracking
        progress_file = create_progress_file(args.output_dir)
        progress = load_progress(progress_file)

        # Filter batches if resuming
        if args.resume:
            completed_batches = set(
                tuple(map(str, batch))
                for batch in progress.get("completed_batches", [])
            )
            batches = [
                batch
                for batch in batches
                if tuple(map(str, batch)) not in completed_batches
            ]
            logging.info(f"📋 Resume mode: {len(batches)} batches remaining")

        if not batches:
            logging.info("✅ All batches already completed!")
            return

        # Load environment and authenticate
        load_env(args.env)
        env_vars = validate_env_vars()
        client = get_garmin_client(env_vars)

        # Process each batch
        total_batches = len(batches)
        successful_batches = 0
        failed_batches = 0

        for i, (batch_start, batch_end) in enumerate(batches, 1):
            logging.info(
                f"🔄 Processing batch {i}/{total_batches}: {batch_start} to {batch_end}"
            )

            try:
                results = process_batch(
                    client, batch_start, batch_end, args.data_types, args.output_dir
                )

                # Track success/failure
                if all(results.values()):
                    progress["completed_batches"].append(
                        [str(batch_start), str(batch_end)]
                    )
                    successful_batches += 1
                    logging.info(f"✅ Batch {i}/{total_batches} completed successfully")
                else:
                    progress["failed_batches"].append(
                        {
                            "batch": [str(batch_start), str(batch_end)],
                            "failed_data_types": [
                                dt for dt, success in results.items() if not success
                            ],
                        }
                    )
                    failed_batches += 1
                    logging.error(f"❌ Batch {i}/{total_batches} partially failed")

                # Save progress after each batch
                save_progress(progress_file, progress)

                # Rate limiting between batches
                if i < total_batches:
                    logging.info(
                        f"⏱️  Waiting {BATCH_DELAY} seconds before next batch..."
                    )
                    time.sleep(BATCH_DELAY)

            except Exception as e:
                logging.error(f"❌ Batch {i}/{total_batches} failed completely: {e}")
                progress["failed_batches"].append(
                    {"batch": [str(batch_start), str(batch_end)], "error": str(e)}
                )
                failed_batches += 1
                save_progress(progress_file, progress)
                continue

        # Final summary
        logging.info(f"🏁 Backfill completed!")
        logging.info(f"✅ Successful batches: {successful_batches}")
        logging.info(f"❌ Failed batches: {failed_batches}")
        logging.info(f"📊 Total processed: {successful_batches + failed_batches}")

        if failed_batches > 0:
            logging.info(f"🔄 To retry failed batches, run with --resume flag")

    except GarminBackfillError as e:
        logging.error(f"Garmin backfill error: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        logging.info("Script interrupted by user")
        sys.exit(130)
    except Exception as e:
        logging.error(f"Unexpected error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
