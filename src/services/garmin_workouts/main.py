"""Main script to create and upload a workout to Garmin Connect"""

import json
import logging
import os
import sys

from src.services.garmin_workouts.garmin_client import GarminClient
from src.services.garmin_workouts.workout_builder import create_simple_running_workout

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)


def main():
    """Create and upload a simple running workout to Garmin Connect"""
    try:
        # Create the workout
        logger.info("Creating simple running workout...")
        workout_data = create_simple_running_workout()

        # Log the workout structure
        logger.info(f"Workout created: {workout_data['workoutName']}")
        logger.info(f"Description: {workout_data['description']}")
        logger.info(
            f"Number of steps: {len(workout_data['workoutSegments'][0]['workoutSteps'])}"
        )

        # Save workout data to file for inspection
        output_file = "simple_running_workout.json"
        with open(output_file, "w") as f:
            json.dump(workout_data, f, indent=2)
        logger.info(f"Workout data saved to {output_file}")

        # Upload to Garmin Connect
        logger.info("Uploading workout to Garmin Connect...")

        with GarminClient() as client:
            result = client.save_workout(workout_data)
            workout_id = result.get("workoutId")

            logger.info(f"✅ Workout uploaded successfully!")
            logger.info(f"   Workout ID: {workout_id}")
            logger.info(
                f"   View at: https://connect.garmin.com/modern/workout/{workout_id}"
            )

        return 0

    except Exception as e:
        logger.error(f"❌ Error: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
