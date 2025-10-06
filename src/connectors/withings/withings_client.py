#!/usr/bin/env python3
"""
Withings API Client (Direct REST API Implementation)
-----------------------------------------------------
Fetches weight data from Withings and uploads to Garmin Connect.
Uses direct REST API calls to avoid dependency conflicts.

API Documentation: https://developer.withings.com/api-reference/
"""

import os
import logging
import json
import time
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from pathlib import Path
from urllib.parse import urlencode

import requests
from requests_oauthlib import OAuth2Session


# Withings API endpoints
WITHINGS_AUTH_URL = "https://account.withings.com/oauth2_user/authorize2"
WITHINGS_TOKEN_URL = "https://wbsapi.withings.net/v2/oauth2"
WITHINGS_API_BASE = "https://wbsapi.withings.net"


class WithingsClient:
    """Client to fetch weight measurements from Withings API using direct REST calls."""

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        callback_uri: str = "https://jaroslawhartman.github.io/withings-sync/contrib/withings.html",
        credentials_file: Optional[Path] = None,
    ):
        """
        Initialize Withings client.

        Args:
            client_id: Withings application client ID
            client_secret: Withings application client secret
            callback_uri: OAuth callback URI (must match app configuration)
            credentials_file: Path to store OAuth credentials (default: ~/.withings_credentials.json)
        """
        self.client_id = client_id
        self.client_secret = client_secret
        self.callback_uri = callback_uri
        self.credentials_file = (
            credentials_file or Path.home() / ".withings_credentials.json"
        )

        self.access_token = None
        self.refresh_token = None
        self.token_expiry = None
        self.user_id = None

    def get_authorization_url(self) -> str:
        """Get the OAuth authorization URL for user to visit."""
        params = {
            "response_type": "code",
            "client_id": self.client_id,
            "redirect_uri": self.callback_uri,
            "scope": "user.metrics,user.info",
            "state": "OK",
        }
        return f"{WITHINGS_AUTH_URL}?{urlencode(params)}"

    def exchange_code_for_token(self, authorization_code: str) -> None:
        """
        Exchange authorization code for access token.

        Args:
            authorization_code: Code from OAuth redirect
        """
        data = {
            "action": "requesttoken",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "grant_type": "authorization_code",
            "code": authorization_code,
            "redirect_uri": self.callback_uri,
        }

        response = requests.post(WITHINGS_TOKEN_URL, data=data)
        result = response.json()

        if result.get("status") != 0:
            error_msg = result.get("error", "Unknown error")
            raise RuntimeError(f"Withings token exchange failed: {error_msg}")

        body = result.get("body", {})
        self.access_token = body.get("access_token")
        self.refresh_token = body.get("refresh_token")
        self.token_expiry = time.time() + body.get("expires_in", 3600)
        self.user_id = body.get("userid")

        self._save_credentials()
        logging.info("✅ Successfully obtained Withings access token")

    def _refresh_access_token(self) -> None:
        """Refresh the access token using refresh token."""
        if not self.refresh_token:
            raise RuntimeError("No refresh token available")

        data = {
            "action": "requesttoken",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "grant_type": "refresh_token",
            "refresh_token": self.refresh_token,
        }

        response = requests.post(WITHINGS_TOKEN_URL, data=data)
        result = response.json()

        if result.get("status") != 0:
            error_msg = result.get("error", "Unknown error")
            raise RuntimeError(f"Withings token refresh failed: {error_msg}")

        body = result.get("body", {})
        self.access_token = body.get("access_token")
        self.refresh_token = body.get("refresh_token")
        self.token_expiry = time.time() + body.get("expires_in", 3600)
        self.user_id = body.get("userid")

        self._save_credentials()
        logging.debug("✅ Refreshed Withings access token")

    def _save_credentials(self) -> None:
        """Save credentials to file."""
        creds = {
            "access_token": self.access_token,
            "refresh_token": self.refresh_token,
            "token_expiry": self.token_expiry,
            "user_id": self.user_id,
        }

        with open(self.credentials_file, "w") as f:
            json.dump(creds, f, indent=2)

        logging.debug(f"💾 Saved credentials to {self.credentials_file}")

    def _load_credentials(self) -> bool:
        """Load credentials from file. Returns True if successful."""
        if not self.credentials_file.exists():
            return False

        try:
            with open(self.credentials_file, "r") as f:
                creds = json.load(f)

            self.access_token = creds.get("access_token")
            self.refresh_token = creds.get("refresh_token")
            self.token_expiry = creds.get("token_expiry")
            self.user_id = creds.get("user_id")

            # Check if token is expired or about to expire (within 5 minutes)
            if self.token_expiry and time.time() >= (self.token_expiry - 300):
                logging.debug("🔄 Token expired, refreshing...")
                self._refresh_access_token()

            logging.info("✅ Loaded Withings credentials from file")
            return True

        except Exception as e:
            logging.warning(f"⚠️ Could not load credentials: {e}")
            return False

    def authenticate(self) -> None:
        """
        Authenticate to Withings API.

        Loads saved credentials or raises error if OAuth flow needed.
        """
        if self._load_credentials():
            return

        # Need OAuth flow
        logging.error("❌ No valid Withings credentials found")
        logging.error("⚠️ Interactive OAuth flow required:")
        logging.error(f"   1. Open: {self.get_authorization_url()}")
        logging.error("   2. Authorize the app")
        logging.error("   3. Copy the authorization code from the redirect URL")
        logging.error(
            "   4. Run: python -m src.connectors.withings.withings_auth_setup"
        )
        raise RuntimeError(
            "Withings OAuth credentials not found. "
            "Please run initial authentication flow."
        )

    def get_weight_measurements(
        self, start_date: datetime, end_date: Optional[datetime] = None
    ) -> List[Dict[str, Any]]:
        """
        Fetch weight measurements from Withings.

        Args:
            start_date: Start date for measurements
            end_date: End date for measurements (default: now)

        Returns:
            List of measurement dictionaries with date, weight_kg, body_fat, etc.
        """
        if not self.access_token:
            self.authenticate()

        if not end_date:
            end_date = datetime.now()

        # Prepare API request - fetch all body composition metrics
        # Measure types:
        # 1=Weight(kg), 5=Fat-Free Mass(kg), 6=Fat(%), 8=Muscle Mass(kg),
        # 76=Water(%), 77=Water Mass(kg), 88=Bone Mass(kg)
        params = {
            "action": "getmeas",
            "meastypes": "1,5,6,8,76,77,88",  # Weight + body composition
            "category": "1",  # 1 = Real measurements
            "startdate": int(start_date.timestamp()),
            "enddate": int(end_date.timestamp()),
        }

        headers = {
            "Authorization": f"Bearer {self.access_token}",
        }

        try:
            response = requests.post(
                f"{WITHINGS_API_BASE}/measure",
                params=params,
                headers=headers,
            )
            result = response.json()

            if result.get("status") != 0:
                error_code = result.get("status")
                if error_code == 401:  # Unauthorized - token expired
                    logging.debug("Token expired during request, refreshing...")
                    self._refresh_access_token()
                    # Retry with new token
                    headers["Authorization"] = f"Bearer {self.access_token}"
                    response = requests.post(
                        f"{WITHINGS_API_BASE}/measure",
                        params=params,
                        headers=headers,
                    )
                    result = response.json()

                if result.get("status") != 0:
                    error_msg = f"Status {result.get('status')}"
                    raise RuntimeError(f"Withings API error: {error_msg}")

            body = result.get("body", {})
            measuregrps = body.get("measuregrps", [])

            weight_data = []
            for grp in measuregrps:
                # Skip non-device measurements
                attrib = grp.get("attrib")
                if attrib not in (0, 2):  # 0 = device, 2 = device (ambiguous)
                    continue

                # Extract all measurements from this group
                measurement = {
                    "date": datetime.fromtimestamp(grp.get("date")),
                    "timestamp": grp.get("date"),
                }

                # Parse all measure types
                for measure in grp.get("measures", []):
                    meas_type = measure.get("type")
                    value = measure.get("value")
                    unit = measure.get("unit")

                    if value is not None and unit is not None:
                        actual_value = value * (10**unit)

                        if meas_type == 1:  # Weight (kg)
                            measurement["weight_kg"] = actual_value
                        elif meas_type == 5:  # Fat-Free Mass (kg) - Skeletal muscle
                            measurement["fat_free_mass_kg"] = actual_value
                        elif meas_type == 6:  # Body fat (%)
                            measurement["body_fat_percent"] = actual_value
                        elif meas_type == 8:  # Muscle mass non-skeletal (kg)
                            measurement["muscle_mass_kg"] = actual_value
                        elif meas_type == 76:  # Water (%)
                            measurement["body_water_percent"] = actual_value
                        elif meas_type == 77:  # Water mass (kg)
                            measurement["water_mass_kg"] = actual_value
                        elif meas_type == 88:  # Bone mass (kg)
                            measurement["bone_mass_kg"] = actual_value

                # Only add if we have at least weight
                if "weight_kg" in measurement:
                    weight_data.append(measurement)
                    logging.debug(
                        f"Found measurement on {measurement['date']}: "
                        f"weight={measurement.get('weight_kg', 0):.2f}kg "
                        f"fat={measurement.get('body_fat_percent', 0):.1f}% "
                        f"water={measurement.get('body_water_percent', 0):.1f}%"
                    )

            logging.info(
                f"✅ Fetched {len(weight_data)} weight measurements from Withings"
            )
            return weight_data

        except Exception as e:
            logging.error(f"❌ Error fetching Withings measurements: {e}")
            raise


def upload_body_composition_to_garmin(
    garmin_client,
    measurement: Dict[str, Any],
    user_height_m: Optional[float] = None,
) -> bool:
    """
    Upload body composition measurement to Garmin Connect.

    Args:
        garmin_client: Authenticated Garmin client
        measurement: Dictionary with weight_kg, body_fat_percent, etc.
        user_height_m: User height in meters for BMI calculation (optional)

    Returns:
        True if successful, False otherwise
    """
    try:
        timestamp = measurement["date"]
        # Format timestamp for Garmin (ISO format with time)
        # Garmin expects: YYYY-MM-DDTHH:MM:SS
        timestamp_str = timestamp.strftime("%Y-%m-%dT%H:%M:%S")

        # Calculate BMI if height is provided: BMI = weight(kg) / height(m)²
        bmi = None
        if user_height_m and "weight_kg" in measurement:
            bmi = measurement["weight_kg"] / (user_height_m**2)

        # Upload to Garmin with all available metrics
        garmin_client.add_body_composition(
            timestamp=timestamp_str,
            weight=measurement["weight_kg"],
            percent_fat=measurement.get("body_fat_percent"),
            percent_hydration=measurement.get("body_water_percent"),  # Body water %
            bone_mass=measurement.get("bone_mass_kg"),
            muscle_mass=measurement.get("fat_free_mass_kg"),  # Skeletal muscle mass kg
            bmi=bmi,
        )

        metrics = [f"weight={measurement['weight_kg']:.2f}kg"]
        if "body_fat_percent" in measurement:
            metrics.append(f"fat={measurement['body_fat_percent']:.1f}%")
        if "body_water_percent" in measurement:
            metrics.append(f"water={measurement['body_water_percent']:.1f}%")
        if "fat_free_mass_kg" in measurement:
            metrics.append(f"skeletal_muscle={measurement['fat_free_mass_kg']:.2f}kg")
        if "bone_mass_kg" in measurement:
            metrics.append(f"bone={measurement['bone_mass_kg']:.2f}kg")
        if bmi:
            metrics.append(f"bmi={bmi:.1f}")

        logging.debug(f"✅ Uploaded to Garmin ({timestamp_str}): {', '.join(metrics)}")
        return True

    except Exception as e:
        timestamp_str = measurement["date"].strftime("%Y-%m-%dT%H:%M:%S")
        logging.warning(
            f"⚠️ Failed to upload body composition to Garmin ({timestamp_str}): {e}"
        )
        return False


def sync_withings_to_garmin(
    garmin_client,
    withings_client_id: str,
    withings_client_secret: str,
    days_back: int = 7,
    user_height_m: Optional[float] = None,
    deduplicate_window_hours: int = 24,
) -> bool:
    """
    Sync body composition data from Withings to Garmin Connect.

    Args:
        garmin_client: Authenticated Garmin client
        withings_client_id: Withings API client ID
        withings_client_secret: Withings API client secret
        days_back: Number of days to sync (default: 7)
        user_height_m: User height in meters for BMI calculation (optional)
        deduplicate_window_hours: Keep only one measurement per time window in hours (default: 24 = one per day)

    Returns:
        True if sync was successful, False otherwise
    """
    try:
        # Initialize Withings client
        withings = WithingsClient(
            client_id=withings_client_id,
            client_secret=withings_client_secret,
        )

        # Authenticate
        withings.authenticate()

        # Fetch measurements from Withings
        start_date = datetime.now() - timedelta(days=days_back)
        measurements = withings.get_weight_measurements(start_date=start_date)

        if not measurements:
            logging.info("ℹ️  No new weight measurements to sync")
            return True

        # Deduplicate measurements from Withings (keep only one per time window)
        if deduplicate_window_hours > 0:
            deduplicated = []
            measurements_sorted = sorted(measurements, key=lambda m: m["date"])

            for measurement in measurements_sorted:
                # Check if we already have a measurement in this time window
                should_add = True
                for existing in deduplicated:
                    time_diff = abs(
                        (measurement["date"] - existing["date"]).total_seconds() / 3600
                    )
                    if time_diff < deduplicate_window_hours:
                        should_add = False
                        logging.debug(
                            f"⏭️  Skipping measurement at {measurement['date']} "
                            f"(too close to {existing['date']}, {time_diff:.1f}h apart)"
                        )
                        break

                if should_add:
                    deduplicated.append(measurement)

            filtered_count = len(measurements) - len(deduplicated)
            if filtered_count > 0:
                logging.info(
                    f"🔍 Filtered {filtered_count} duplicate measurements "
                    f"(keeping one per {deduplicate_window_hours}h window)"
                )
            measurements = deduplicated

        # Fetch existing Garmin body composition data to avoid duplicates
        try:
            start_date_str = start_date.strftime("%Y-%m-%d")
            end_date_str = datetime.now().strftime("%Y-%m-%d")
            garmin_data = garmin_client.get_body_composition(
                startdate=start_date_str, enddate=end_date_str
            )

            # Extract existing timestamps (convert to datetime for comparison)
            existing_timestamps = set()
            if garmin_data and isinstance(garmin_data, list):
                for entry in garmin_data:
                    if "samplePk" in entry:
                        # Garmin timestamp is in milliseconds
                        ts = datetime.fromtimestamp(entry["samplePk"] / 1000)
                        # Round to minute precision for comparison
                        ts_rounded = ts.replace(second=0, microsecond=0)
                        existing_timestamps.add(ts_rounded)

            logging.debug(
                f"Found {len(existing_timestamps)} existing Garmin body composition entries"
            )
        except Exception as e:
            logging.warning(
                f"⚠️ Could not fetch existing Garmin data (will sync all): {e}"
            )
            existing_timestamps = set()

        # Upload to Garmin (skip duplicates)
        success_count = 0
        skipped_count = 0
        for measurement in measurements:
            # Round measurement timestamp to minute precision
            meas_time = measurement["date"].replace(second=0, microsecond=0)

            # Check if entry already exists
            if meas_time in existing_timestamps:
                logging.debug(
                    f"⏭️  Skipping duplicate entry for {meas_time.strftime('%Y-%m-%d %H:%M')}"
                )
                skipped_count += 1
                continue

            if upload_body_composition_to_garmin(
                garmin_client=garmin_client,
                measurement=measurement,
                user_height_m=user_height_m,
            ):
                success_count += 1

        logging.info(
            f"✅ Synced {success_count}/{len(measurements)} body composition measurements to Garmin "
            f"({skipped_count} duplicates skipped)"
        )
        return success_count > 0 or skipped_count > 0

    except RuntimeError as e:
        # OAuth credentials not found - expected on first run
        logging.error(str(e))
        return False
    except Exception as e:
        logging.error(f"❌ Withings to Garmin sync failed: {e}")
        logging.debug("Full traceback:", exc_info=True)
        return False
