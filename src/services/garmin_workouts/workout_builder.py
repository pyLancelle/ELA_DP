"""Workout Builder for Garmin Connect"""

from enum import IntEnum
from typing import List, Optional


class WorkoutStepType(IntEnum):
    """Workout step types"""

    WARMUP = 1
    COOLDOWN = 2
    INTERVAL = 3
    RECOVERY = 4
    REST = 5
    REPEAT = 6


class WorkoutStepDurationType(IntEnum):
    """Duration types for workout steps"""

    LAP_BUTTON = 1
    TIME = 2
    DISTANCE = 3
    HEART_RATE_LESS_THAN = 4
    HEART_RATE_GREATER_THAN = 5
    CALORIES = 6
    POWER_LESS_THAN = 7
    POWER_GREATER_THAN = 8
    TRAINING_PEAKS_TSS = 9
    REPETITION_TIME = 10


class WorkoutStepTargetType(IntEnum):
    """Target types for workout steps"""

    NO_TARGET = 1
    POWER_ZONE = 2
    CADENCE_ZONE = 3
    HEART_RATE_ZONE = 4
    SPEED_ZONE = 5
    PACE_ZONE = 6
    GRADE = 7
    RESISTANCE = 8


class WorkoutStepIntensity(IntEnum):
    """Intensity levels for workout steps"""

    ACTIVE = 0
    REST = 1
    WARMUP = 2
    COOLDOWN = 3


class WorkoutStep:
    """Represents a single workout step"""

    def __init__(
        self,
        step_type: WorkoutStepType,
        duration_type: WorkoutStepDurationType,
        duration_value: float,
        target_type: WorkoutStepTargetType = WorkoutStepTargetType.NO_TARGET,
        target_value_low: Optional[float] = None,
        target_value_high: Optional[float] = None,
        intensity: Optional[WorkoutStepIntensity] = None,
    ):
        self.step_type = step_type
        self.duration_type = duration_type
        self.duration_value = duration_value
        self.target_type = target_type
        self.target_value_low = target_value_low
        self.target_value_high = target_value_high
        self.intensity = intensity or self._default_intensity()

    def _default_intensity(self) -> WorkoutStepIntensity:
        """Get default intensity based on step type"""
        intensity_map = {
            WorkoutStepType.WARMUP: WorkoutStepIntensity.WARMUP,
            WorkoutStepType.COOLDOWN: WorkoutStepIntensity.COOLDOWN,
            WorkoutStepType.REST: WorkoutStepIntensity.REST,
            WorkoutStepType.RECOVERY: WorkoutStepIntensity.REST,
        }
        return intensity_map.get(self.step_type, WorkoutStepIntensity.ACTIVE)

    def to_dict(self, step_order: int) -> dict:
        """Convert step to Garmin API format"""

        # Convert enum name to Garmin API format (e.g., PACE_ZONE -> pace.zone)
        def format_key(name: str) -> str:
            return name.lower().replace("_", ".")

        step_data = {
            "type": "ExecutableStepDTO",
            "stepId": None,
            "stepOrder": step_order,
            "childStepId": None,
            "description": None,
            "stepType": {
                "stepTypeId": self.step_type,
                "stepTypeKey": format_key(self.step_type.name),
            },
            "endCondition": {
                "conditionTypeKey": format_key(self.duration_type.name),
                "conditionTypeId": self.duration_type,
            },
            "endConditionValue": self.duration_value,
            "targetType": {
                "workoutTargetTypeId": self.target_type,
                "workoutTargetTypeKey": format_key(self.target_type.name),
            },
            "targetValueOne": self.target_value_low,
            "targetValueTwo": self.target_value_high,
            "zoneNumber": None,
            "secondaryTargetType": None,
            "secondaryTargetValueOne": None,
            "secondaryTargetValueTwo": None,
            "secondaryZoneNumber": None,
            "intensity": self.intensity.name.lower(),
        }
        return step_data


class WorkoutBuilder:
    """Builder for creating Garmin workouts"""

    SPORT_TYPE_RUNNING = 1

    def __init__(
        self,
        name: str,
        sport_type: int = SPORT_TYPE_RUNNING,
        description: Optional[str] = None,
    ):
        self.name = name
        self.sport_type = sport_type
        self.description = description or ""
        self.steps: List[WorkoutStep] = []

    @staticmethod
    def pace_to_mps(min_per_km: float) -> float:
        """Convert pace from min/km to meters per second

        Args:
            min_per_km: Pace in minutes per kilometer (e.g., 5.0 for 5:00/km)

        Returns:
            Speed in meters per second
        """
        return 1000 / (min_per_km * 60)

    def add_warmup(
        self,
        duration_minutes: float,
        pace_min_km_low: Optional[float] = None,
        pace_min_km_high: Optional[float] = None,
    ) -> "WorkoutBuilder":
        """Add a warmup step

        Args:
            duration_minutes: Duration in minutes
            pace_min_km_low: Lower pace bound in min/km (faster pace, e.g., 5.0 for 5:00/km)
            pace_min_km_high: Upper pace bound in min/km (slower pace, e.g., 5.5 for 5:30/km)

        Returns:
            Self for chaining
        """
        target_type = WorkoutStepTargetType.NO_TARGET
        target_low = None
        target_high = None

        if pace_min_km_low and pace_min_km_high:
            target_type = WorkoutStepTargetType.PACE_ZONE
            target_low = self.pace_to_mps(pace_min_km_low)
            target_high = self.pace_to_mps(pace_min_km_high)

        step = WorkoutStep(
            step_type=WorkoutStepType.WARMUP,
            duration_type=WorkoutStepDurationType.TIME,
            duration_value=duration_minutes * 60,  # Convert to seconds
            target_type=target_type,
            target_value_low=target_low,
            target_value_high=target_high,
        )
        self.steps.append(step)
        return self

    def add_workout(
        self,
        duration_minutes: float,
        pace_min_km_low: Optional[float] = None,
        pace_min_km_high: Optional[float] = None,
    ) -> "WorkoutBuilder":
        """Add a workout interval step

        Args:
            duration_minutes: Duration in minutes
            pace_min_km_low: Lower pace bound in min/km (faster pace, e.g., 4.0 for 4:00/km)
            pace_min_km_high: Upper pace bound in min/km (slower pace, e.g., 4.5 for 4:30/km)

        Returns:
            Self for chaining
        """
        target_type = WorkoutStepTargetType.NO_TARGET
        target_low = None
        target_high = None

        if pace_min_km_low and pace_min_km_high:
            target_type = WorkoutStepTargetType.PACE_ZONE
            target_low = self.pace_to_mps(pace_min_km_low)
            target_high = self.pace_to_mps(pace_min_km_high)

        step = WorkoutStep(
            step_type=WorkoutStepType.INTERVAL,
            duration_type=WorkoutStepDurationType.TIME,
            duration_value=duration_minutes * 60,  # Convert to seconds
            target_type=target_type,
            target_value_low=target_low,
            target_value_high=target_high,
        )
        self.steps.append(step)
        return self

    def add_cooldown(
        self,
        duration_minutes: float,
        pace_min_km_low: Optional[float] = None,
        pace_min_km_high: Optional[float] = None,
    ) -> "WorkoutBuilder":
        """Add a cooldown step

        Args:
            duration_minutes: Duration in minutes
            pace_min_km_low: Lower pace bound in min/km (faster pace, e.g., 6.0 for 6:00/km)
            pace_min_km_high: Upper pace bound in min/km (slower pace, e.g., 7.0 for 7:00/km)

        Returns:
            Self for chaining
        """
        target_type = WorkoutStepTargetType.NO_TARGET
        target_low = None
        target_high = None

        if pace_min_km_low and pace_min_km_high:
            target_type = WorkoutStepTargetType.PACE_ZONE
            target_low = self.pace_to_mps(pace_min_km_low)
            target_high = self.pace_to_mps(pace_min_km_high)

        step = WorkoutStep(
            step_type=WorkoutStepType.COOLDOWN,
            duration_type=WorkoutStepDurationType.TIME,
            duration_value=duration_minutes * 60,  # Convert to seconds
            target_type=target_type,
            target_value_low=target_low,
            target_value_high=target_high,
        )
        self.steps.append(step)
        return self

    def add_recovery(
        self,
        duration_minutes: float,
        pace_min_km_low: Optional[float] = None,
        pace_min_km_high: Optional[float] = None,
    ) -> "WorkoutBuilder":
        """Add a recovery step

        Args:
            duration_minutes: Duration in minutes
            pace_min_km_low: Lower pace bound in min/km (faster pace)
            pace_min_km_high: Upper pace bound in min/km (slower pace)

        Returns:
            Self for chaining
        """
        target_type = WorkoutStepTargetType.NO_TARGET
        target_low = None
        target_high = None

        if pace_min_km_low and pace_min_km_high:
            target_type = WorkoutStepTargetType.PACE_ZONE
            target_low = self.pace_to_mps(pace_min_km_low)
            target_high = self.pace_to_mps(pace_min_km_high)

        step = WorkoutStep(
            step_type=WorkoutStepType.RECOVERY,
            duration_type=WorkoutStepDurationType.TIME,
            duration_value=duration_minutes * 60,  # Convert to seconds
            target_type=target_type,
            target_value_low=target_low,
            target_value_high=target_high,
        )
        self.steps.append(step)
        return self

    def build(self) -> dict:
        """Build the workout data structure for Garmin API

        Returns:
            Workout data dictionary
        """
        workout_segments = []

        # Create workout segment with all steps
        segment_steps = []
        for i, step in enumerate(self.steps, start=1):
            segment_steps.append(step.to_dict(step_order=i))

        workout_segment = {
            "segmentOrder": 1,
            "sportType": {
                "sportTypeId": self.sport_type,
                "sportTypeKey": (
                    "running" if self.sport_type == self.SPORT_TYPE_RUNNING else "other"
                ),
            },
            "workoutSteps": segment_steps,
        }
        workout_segments.append(workout_segment)

        # Build complete workout
        workout_data = {
            "workoutName": self.name,
            "description": self.description,
            "sportType": {
                "sportTypeId": self.sport_type,
                "sportTypeKey": (
                    "running" if self.sport_type == self.SPORT_TYPE_RUNNING else "other"
                ),
            },
            "workoutSegments": workout_segments,
        }

        return workout_data


def create_simple_running_workout() -> dict:
    """Create a simple running workout with warm-up, workout, and cool-down

    Returns:
        Workout data dictionary
    """
    builder = WorkoutBuilder(
        name="Simple Running Workout",
        sport_type=WorkoutBuilder.SPORT_TYPE_RUNNING,
        description="Un workout simple avec échauffement, séance et retour au calme",
    )

    # Warm-up: 10 minutes
    builder.add_warmup(duration_minutes=10)

    # Main workout: 20 minutes
    builder.add_workout(duration_minutes=20)

    # Cool-down: 5 minutes
    builder.add_cooldown(duration_minutes=5)

    return builder.build()
