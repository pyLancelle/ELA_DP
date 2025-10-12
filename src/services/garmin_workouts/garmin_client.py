"""Garmin Connect Client for workout upload"""

import json
import logging
import os
from typing import Optional

import garth

logger = logging.getLogger(__name__)


class GarminClient:
    """Client for Garmin Connect API"""

    def __init__(self, username: Optional[str] = None, password: Optional[str] = None):
        """Initialize Garmin client

        Args:
            username: Garmin username (email), defaults to GARMIN_USERNAME env var
            password: Garmin password, defaults to GARMIN_PASSWORD env var
        """
        self.username = username or os.getenv("GARMIN_USERNAME")
        self.password = password or os.getenv("GARMIN_PASSWORD")

        if not self.username or not self.password:
            raise ValueError("Garmin credentials not provided")

        self._authenticated = False

    def __enter__(self):
        """Context manager entry"""
        self.authenticate()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        pass

    def authenticate(self):
        """Authenticate with Garmin Connect"""
        if self._authenticated:
            return

        logger.info("Authenticating with Garmin Connect...")

        try:
            garth.login(self.username, self.password)
            self._authenticated = True
            logger.info("Successfully authenticated with Garmin Connect")
        except Exception as e:
            raise Exception(f"Authentication failed: {e}")

    def save_workout(self, workout_data: dict) -> dict:
        """Upload a workout to Garmin Connect

        Args:
            workout_data: Workout data in Garmin format

        Returns:
            Response from Garmin Connect API
        """
        if not self._authenticated:
            self.authenticate()

        endpoint = "/workout-service/workout"

        response = garth.connectapi(endpoint, method="POST", json=workout_data)

        logger.info(f"Workout uploaded successfully: {response.get('workoutId')}")
        return response

    def list_workouts(self, limit: int = 10) -> list:
        """List workouts from Garmin Connect

        Args:
            limit: Maximum number of workouts to retrieve

        Returns:
            List of workouts
        """
        if not self._authenticated:
            self.authenticate()

        endpoint = "/workout-service/workouts"
        params = {"limit": limit}

        response = garth.connectapi(endpoint, params=params)
        return response

    def get_workout(self, workout_id: int) -> dict:
        """Get a specific workout from Garmin Connect

        Args:
            workout_id: Workout ID

        Returns:
            Workout data
        """
        if not self._authenticated:
            self.authenticate()

        endpoint = f"/workout-service/workout/{workout_id}"

        response = garth.connectapi(endpoint)
        return response

    def schedule_workout(self, workout_id: int, date: str) -> None:
        """Schedule a workout on Garmin Connect calendar

        Args:
            workout_id: Workout ID to schedule
            date: Date to schedule the workout (format: YYYY-MM-DD, e.g., "2025-10-15")
        """
        if not self._authenticated:
            self.authenticate()

        endpoint = f"/workout-service/schedule/{workout_id}"
        json_data = {"date": date}

        garth.connectapi(endpoint, method="POST", json=json_data)

        logger.info(f"Workout {workout_id} scheduled for {date}")

    def delete_workout_schedule(self, workout_id: int) -> None:
        """Remove a scheduled workout from Garmin Connect calendar

        Args:
            workout_id: Workout ID to unschedule
        """
        if not self._authenticated:
            self.authenticate()

        endpoint = f"/workout-service/schedule/{workout_id}"

        garth.connectapi(endpoint, method="DELETE")

        logger.info(f"Workout {workout_id} schedule removed")
