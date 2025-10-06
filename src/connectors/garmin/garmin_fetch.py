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
    "activity_details",
    "activity_splits",
    "activity_weather",
    "activity_hr_zones",
    "activity_exercise_sets",
    "sleep",
    "steps",
    "heart_rate",
    "body_battery",
    "stress",
    "weight",
    "body_composition",
    "user_summary",
    "daily_summary",
    "stats_and_body",
    "training_readiness",
    "rhr_daily",
    "spo2",
    "respiration",
    "intensity_minutes",
    "max_metrics",
    "all_day_events",
    "device_info",
    "training_status",
    "hrv",
    "race_predictions",
    "floors",
    "endurance_score",
    "hill_score",
]


class GarminConnectorError(Exception):
    """Custom exception for Garmin connector errors."""

    pass


def sync_withings_data(garmin_client: Garmin, days: int = 30) -> bool:
    """
    Automatically sync Withings body composition data to Garmin Connect.

    Uses our custom Withings client to avoid dependency conflicts.

    Args:
        garmin_client: Authenticated Garmin client
        days: Number of days to sync (for historical data)

    Returns:
        True if sync successful or skipped, False if failed
    """
    # Check if Withings credentials are available
    withings_client_id = os.getenv("WITHINGS_CLIENT_ID")
    withings_client_secret = os.getenv("WITHINGS_CLIENT_SECRET")
    user_height_m = os.getenv("USER_HEIGHT_M")  # Optional: for BMI calculation

    if not withings_client_id or not withings_client_secret:
        logging.info("ℹ️ No Withings credentials found, skipping automatic sync")
        logging.debug(
            "💡 Set WITHINGS_CLIENT_ID and WITHINGS_CLIENT_SECRET to enable sync"
        )
        return True

    try:
        logging.info("🔄 Starting automatic Withings to Garmin sync...")

        # Import our Withings client
        sys.path.insert(0, str(Path(__file__).parent.parent))
        from withings import sync_withings_to_garmin

        # Parse user height if provided
        height = float(user_height_m) if user_height_m else None

        # Sync Withings to Garmin
        success = sync_withings_to_garmin(
            garmin_client=garmin_client,
            withings_client_id=withings_client_id,
            withings_client_secret=withings_client_secret,
            days_back=days,
            user_height_m=height,
        )

        if success:
            logging.info("✅ Withings sync completed successfully")
            logging.info("⏳ Waiting 10 seconds for Garmin to process data...")
            import time

            time.sleep(10)  # Brief pause to let Garmin process
            return True
        else:
            logging.warning("⚠️ Withings sync had issues but continuing anyway")
            return True  # Don't fail the entire process

    except Exception as e:
        logging.warning(f"⚠️ Withings sync error: {e}")
        logging.debug(f"   Full traceback: {e}", exc_info=True)
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
                import time

                time.sleep(0.5)

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
                import time

                time.sleep(0.3)

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
                import time

                time.sleep(0.3)

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
                import time

                time.sleep(0.3)

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


def fetch_activity_exercise_sets(
    client: Garmin, start_date: datetime, end_date: datetime
) -> List[Dict[str, Any]]:
    """Fetch exercise sets data for strength training activities."""
    try:
        # First get the basic activities list
        activities = client.get_activities_by_date(
            start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")
        )

        all_exercise_sets = []
        for activity in activities:
            try:
                activity_id = activity.get("activityId")
                if not activity_id:
                    continue

                # Get exercise sets data (mainly for strength training)
                exercise_sets = client.get_activity_exercise_sets(activity_id)

                if exercise_sets:
                    activity_sets = {
                        "activityId": activity_id,
                        "activityName": activity.get("activityName", ""),
                        "activityType": activity.get("activityType", ""),
                        "startTimeLocal": activity.get("startTimeLocal", ""),
                        "exercise_sets_data": exercise_sets,
                        "data_type": "activity_exercise_sets",
                    }
                    all_exercise_sets.append(activity_sets)

                # Rate limiting
                import time

                time.sleep(0.3)

            except Exception as e:
                logging.warning(
                    f"Could not fetch exercise sets for activity {activity.get('activityId', 'unknown')}: {e}"
                )
                continue

        logging.info(
            f"Fetched exercise sets data for {len(all_exercise_sets)} activities"
        )
        return all_exercise_sets

    except Exception as e:
        logging.error(f"Error fetching activity exercise sets: {e}")
        return []


def fetch_user_summary(
    client: Garmin, start_date: datetime, end_date: datetime
) -> List[Dict[str, Any]]:
    """Fetch user daily summary (morning recap) data."""
    user_summaries = []
    current_date = start_date

    while current_date <= end_date:
        try:
            date_str = current_date.strftime("%Y-%m-%d")
            summary = client.get_user_summary(date_str)

            if summary:
                summary_data = {
                    "date": date_str,
                    "summary_data": summary,
                    "data_type": "user_summary",
                }
                user_summaries.append(summary_data)

            import time

            time.sleep(0.5)  # Slightly longer delay for summary data

        except Exception as e:
            logging.warning(f"No user summary for {date_str}: {e}")

        current_date += timedelta(days=1)

    logging.info(f"Fetched user summary data for {len(user_summaries)} days")
    return user_summaries


def fetch_daily_summary(
    client: Garmin, start_date: datetime, end_date: datetime
) -> List[Dict[str, Any]]:
    """Fetch daily summary data."""
    daily_summaries = []
    current_date = start_date

    while current_date <= end_date:
        try:
            date_str = current_date.strftime("%Y-%m-%d")

            # Try different summary methods
            summary_methods = [
                ("daily_summary", lambda: client.get_daily_summary(date_str)),
                ("stats", lambda: client.get_stats(date_str)),
            ]

            for method_name, fetch_func in summary_methods:
                try:
                    summary = fetch_func()
                    if summary:
                        summary_data = {
                            "date": date_str,
                            "summary_method": method_name,
                            "summary_data": summary,
                            "data_type": "daily_summary",
                        }
                        daily_summaries.append(summary_data)
                except Exception as e:
                    logging.debug(f"Could not fetch {method_name} for {date_str}: {e}")

            import time

            time.sleep(0.5)

        except Exception as e:
            logging.warning(f"No daily summary for {date_str}: {e}")

        current_date += timedelta(days=1)

    logging.info(f"Fetched daily summary data for {len(daily_summaries)} days")
    return daily_summaries


def fetch_stats_and_body(
    client: Garmin, start_date: datetime, end_date: datetime
) -> List[Dict[str, Any]]:
    """Fetch stats and body composition data."""
    stats_data = []
    current_date = start_date

    while current_date <= end_date:
        try:
            date_str = current_date.strftime("%Y-%m-%d")
            stats_body = client.get_stats_and_body(date_str)

            if stats_body:
                stats_data.append(
                    {
                        "date": date_str,
                        "stats_and_body_data": stats_body,
                        "data_type": "stats_and_body",
                    }
                )

            import time

            time.sleep(0.5)

        except Exception as e:
            logging.warning(f"No stats and body data for {date_str}: {e}")

        current_date += timedelta(days=1)

    logging.info(f"Fetched stats and body data for {len(stats_data)} days")
    return stats_data


def fetch_body_composition(
    client: Garmin, start_date: datetime, end_date: datetime
) -> List[Dict[str, Any]]:
    """Fetch detailed body composition data (fat %, muscle mass, etc.)."""
    try:
        start_str = start_date.strftime("%Y-%m-%d")
        end_str = end_date.strftime("%Y-%m-%d")
        body_comp_data = client.get_body_composition(start_str, end_str)

        if body_comp_data:
            logging.info(
                f"Fetched body composition data: {len(body_comp_data) if isinstance(body_comp_data, list) else 1} entries"
            )

            # Ensure it's a list and add metadata
            if isinstance(body_comp_data, list):
                for entry in body_comp_data:
                    entry["data_type"] = "body_composition"
                return body_comp_data
            else:
                body_comp_data["data_type"] = "body_composition"
                return [body_comp_data]
        else:
            logging.info("No body composition data available")
            return []

    except Exception as e:
        logging.warning(f"No body composition data available: {e}")
        return []


def fetch_training_readiness(
    client: Garmin, start_date: datetime, end_date: datetime
) -> List[Dict[str, Any]]:
    """Fetch training readiness data (recovery status)."""
    readiness_data = []
    current_date = start_date

    while current_date <= end_date:
        try:
            date_str = current_date.strftime("%Y-%m-%d")
            readiness = client.get_training_readiness(date_str)

            if readiness:
                readiness_data.append(
                    {
                        "date": date_str,
                        "training_readiness_data": readiness,
                        "data_type": "training_readiness",
                    }
                )

            import time

            time.sleep(0.3)

        except Exception as e:
            logging.warning(f"No training readiness for {date_str}: {e}")

        current_date += timedelta(days=1)

    logging.info(f"Fetched training readiness data for {len(readiness_data)} days")
    return readiness_data


def fetch_rhr_daily(
    client: Garmin, start_date: datetime, end_date: datetime
) -> List[Dict[str, Any]]:
    """Fetch daily resting heart rate data."""
    rhr_data = []
    current_date = start_date

    while current_date <= end_date:
        try:
            date_str = current_date.strftime("%Y-%m-%d")
            rhr = client.get_rhr_day(date_str)

            if rhr:
                rhr_data.append(
                    {"date": date_str, "rhr_data": rhr, "data_type": "rhr_daily"}
                )

            import time

            time.sleep(0.3)

        except Exception as e:
            logging.warning(f"No RHR data for {date_str}: {e}")

        current_date += timedelta(days=1)

    logging.info(f"Fetched RHR data for {len(rhr_data)} days")
    return rhr_data


def fetch_spo2(
    client: Garmin, start_date: datetime, end_date: datetime
) -> List[Dict[str, Any]]:
    """Fetch SpO2 (blood oxygen saturation) data."""
    spo2_data = []
    current_date = start_date

    while current_date <= end_date:
        try:
            date_str = current_date.strftime("%Y-%m-%d")
            spo2 = client.get_spo2_data(date_str)

            if spo2:
                spo2_data.append(
                    {"date": date_str, "spo2_data": spo2, "data_type": "spo2"}
                )

            import time

            time.sleep(0.3)

        except Exception as e:
            logging.warning(f"No SpO2 data for {date_str}: {e}")

        current_date += timedelta(days=1)

    logging.info(f"Fetched SpO2 data for {len(spo2_data)} days")
    return spo2_data


def fetch_respiration(
    client: Garmin, start_date: datetime, end_date: datetime
) -> List[Dict[str, Any]]:
    """Fetch respiration data."""
    respiration_data = []
    current_date = start_date

    while current_date <= end_date:
        try:
            date_str = current_date.strftime("%Y-%m-%d")
            respiration = client.get_respiration_data(date_str)

            if respiration:
                respiration_data.append(
                    {
                        "date": date_str,
                        "respiration_data": respiration,
                        "data_type": "respiration",
                    }
                )

            import time

            time.sleep(0.3)

        except Exception as e:
            logging.warning(f"No respiration data for {date_str}: {e}")

        current_date += timedelta(days=1)

    logging.info(f"Fetched respiration data for {len(respiration_data)} days")
    return respiration_data


def fetch_intensity_minutes(
    client: Garmin, start_date: datetime, end_date: datetime
) -> List[Dict[str, Any]]:
    """Fetch intensity minutes data."""
    intensity_data = []
    current_date = start_date

    while current_date <= end_date:
        try:
            date_str = current_date.strftime("%Y-%m-%d")
            intensity = client.get_intensity_minutes_data(date_str)

            if intensity:
                intensity_data.append(
                    {
                        "date": date_str,
                        "intensity_minutes_data": intensity,
                        "data_type": "intensity_minutes",
                    }
                )

            import time

            time.sleep(0.3)

        except Exception as e:
            logging.warning(f"No intensity minutes data for {date_str}: {e}")

        current_date += timedelta(days=1)

    logging.info(f"Fetched intensity minutes data for {len(intensity_data)} days")
    return intensity_data


def fetch_max_metrics(
    client: Garmin, start_date: datetime, end_date: datetime
) -> List[Dict[str, Any]]:
    """Fetch max metrics data (VO2 max, etc.)."""
    try:
        start_str = start_date.strftime("%Y-%m-%d")
        end_str = end_date.strftime("%Y-%m-%d")

        # Try to get max metrics - this might be a single call or date-range based
        max_metrics = client.get_max_metrics(start_str, end_str)

        if max_metrics:
            logging.info(
                f"Fetched max metrics data: {len(max_metrics) if isinstance(max_metrics, list) else 1} entries"
            )

            if isinstance(max_metrics, list):
                for entry in max_metrics:
                    entry["data_type"] = "max_metrics"
                return max_metrics
            else:
                max_metrics["data_type"] = "max_metrics"
                return [max_metrics]
        else:
            logging.info("No max metrics data available")
            return []

    except Exception as e:
        logging.warning(f"No max metrics data available: {e}")

        # Fallback: try without date parameters
        try:
            max_metrics = client.get_max_metrics()
            if max_metrics:
                if isinstance(max_metrics, list):
                    for entry in max_metrics:
                        entry["data_type"] = "max_metrics"
                    return max_metrics
                else:
                    max_metrics["data_type"] = "max_metrics"
                    return [max_metrics]
        except Exception as e2:
            logging.warning(f"No max metrics data available (fallback): {e2}")

        return []


def fetch_all_day_events(
    client: Garmin, start_date: datetime, end_date: datetime
) -> List[Dict[str, Any]]:
    """Fetch all-day wellness events."""
    events_data = []
    current_date = start_date

    while current_date <= end_date:
        try:
            date_str = current_date.strftime("%Y-%m-%d")
            events = client.get_all_day_events(date_str)

            if events:
                events_data.append(
                    {
                        "date": date_str,
                        "all_day_events_data": events,
                        "data_type": "all_day_events",
                    }
                )

            import time

            time.sleep(0.3)

        except Exception as e:
            logging.warning(f"No all-day events for {date_str}: {e}")

        current_date += timedelta(days=1)

    logging.info(f"Fetched all-day events data for {len(events_data)} days")
    return events_data


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

        # Setup Garmin client
        client = get_garmin_client(env_vars)

        # Automatic Withings to Garmin sync (if enabled)
        if not args.no_withings_sync:
            sync_withings_data(
                garmin_client=client,
                days=args.days,
            )
        else:
            logging.info("ℹ️ Withings sync disabled via --no-withings-sync flag")

        # Calculate date range
        end_date = datetime.now()
        start_date = end_date - timedelta(days=args.days)

        logging.info(
            f"Fetching Garmin data from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}"
        )

        # Fetch each data type
        data_fetchers = {
            "activities": lambda: fetch_activities(client, start_date, end_date),
            "activity_details": lambda: fetch_activity_details(
                client, start_date, end_date
            ),
            "activity_splits": lambda: fetch_activity_splits(
                client, start_date, end_date
            ),
            "activity_weather": lambda: fetch_activity_weather(
                client, start_date, end_date
            ),
            "activity_hr_zones": lambda: fetch_activity_hr_zones(
                client, start_date, end_date
            ),
            "activity_exercise_sets": lambda: fetch_activity_exercise_sets(
                client, start_date, end_date
            ),
            "sleep": lambda: fetch_sleep_data(client, start_date, end_date),
            "steps": lambda: fetch_steps_data(client, start_date, end_date),
            "heart_rate": lambda: fetch_heart_rate_data(client, start_date, end_date),
            "body_battery": lambda: fetch_body_battery_data(
                client, start_date, end_date
            ),
            "stress": lambda: fetch_stress_data(client, start_date, end_date),
            "weight": lambda: fetch_weight_data(client, start_date, end_date),
            "body_composition": lambda: fetch_body_composition(
                client, start_date, end_date
            ),
            "user_summary": lambda: fetch_user_summary(client, start_date, end_date),
            "daily_summary": lambda: fetch_daily_summary(client, start_date, end_date),
            "stats_and_body": lambda: fetch_stats_and_body(
                client, start_date, end_date
            ),
            "training_readiness": lambda: fetch_training_readiness(
                client, start_date, end_date
            ),
            "rhr_daily": lambda: fetch_rhr_daily(client, start_date, end_date),
            "spo2": lambda: fetch_spo2(client, start_date, end_date),
            "respiration": lambda: fetch_respiration(client, start_date, end_date),
            "intensity_minutes": lambda: fetch_intensity_minutes(
                client, start_date, end_date
            ),
            "max_metrics": lambda: fetch_max_metrics(client, start_date, end_date),
            "all_day_events": lambda: fetch_all_day_events(
                client, start_date, end_date
            ),
            "device_info": lambda: fetch_device_info(client),
            "training_status": lambda: fetch_training_status_data(
                client, start_date, end_date
            ),
            "hrv": lambda: fetch_hrv_data(client, start_date, end_date),
            "race_predictions": lambda: fetch_race_predictions_data(
                client, start_date, end_date
            ),
            "floors": lambda: fetch_floors_data(client, start_date, end_date),
            "endurance_score": lambda: fetch_endurance_score_data(
                client, start_date, end_date
            ),
            "hill_score": lambda: fetch_hill_score_data(client, start_date, end_date),
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
