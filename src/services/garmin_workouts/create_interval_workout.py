"""Create an interval workout with 8x30" hard/30" recovery"""

import json
import logging
import sys
from datetime import datetime, timedelta

from src.services.garmin_workouts.garmin_client import GarminClient
from src.services.garmin_workouts.workout_builder import WorkoutBuilder

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)


def create_interval_workout() -> dict:
    """Create an interval workout with 8x30" hard/30" recovery

    Workout structure:
    - 20 minutes warm-up (6:00-6:30/km)
    - 8 x (30" hard at 3:30-4:00/km + 30" recovery at 6:00-7:00/km)
    - 10 minutes cool-down (6:00-6:30/km)

    Returns:
        Workout data dictionary
    """
    builder = WorkoutBuilder(
        name="8x30-30 Intervals",
        sport_type=WorkoutBuilder.SPORT_TYPE_RUNNING,
        description="20' échauffement, 8x30\" à fond + 30\" récup, 10' cool down",
    )

    # Warm-up: 20 minutes at easy pace (6:00-6:30/km)
    builder.add_warmup(duration_minutes=20, pace_min_km_low=6.0, pace_min_km_high=6.5)

    # Main interval set: 8 x (30" hard + 30" recovery)
    for i in range(8):
        # 30 seconds hard (interval) at 3:30-4:00/km
        builder.add_workout(
            duration_minutes=0.5, pace_min_km_low=3.5, pace_min_km_high=4.0
        )

        # 30 seconds recovery at 6:00-7:00/km
        builder.add_recovery(
            duration_minutes=0.5, pace_min_km_low=6.0, pace_min_km_high=7.0
        )

    # Cool-down: 10 minutes at easy pace (6:00-6:30/km)
    builder.add_cooldown(duration_minutes=10, pace_min_km_low=6.0, pace_min_km_high=6.5)

    return builder.build()


def main():
    """Create and upload the interval workout to Garmin Connect"""
    try:
        # Create the workout
        logger.info("Creating interval workout (8x30-30)...")
        workout_data = create_interval_workout()

        # Log the workout structure
        logger.info(f"Workout created: {workout_data['workoutName']}")
        logger.info(f"Description: {workout_data['description']}")
        logger.info(
            f"Number of steps: {len(workout_data['workoutSegments'][0]['workoutSteps'])}"
        )

        # Save workout data to file for inspection
        output_file = "interval_8x30_workout.json"
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

            # Schedule the workout for tomorrow
            tomorrow = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
            logger.info(f"Scheduling workout for {tomorrow}...")
            client.schedule_workout(workout_id, tomorrow)

            logger.info(f"✅ Workout scheduled for {tomorrow}!")
            logger.info(f"   Check your Garmin Connect calendar")

        return 0

    except Exception as e:
        logger.error(f"❌ Error: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
