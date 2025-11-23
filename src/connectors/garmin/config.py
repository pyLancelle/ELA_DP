"""
Garmin Connector Configuration
------------------------------
Defines the metrics to fetch and their corresponding API methods.
Now loads configuration from metrics.yaml.
"""
import yaml
from pathlib import Path
import logging

# Load metrics configuration from YAML file
metrics_file = Path(__file__).parent / "metrics.yaml"

try:
    with open(metrics_file, "r") as f:
        METRICS_CONFIG = yaml.safe_load(f)
except Exception as e:
    logging.error(f"Failed to load metrics.yaml: {e}")
    # Fallback to empty or raise error? Raising error is safer.
    raise RuntimeError(f"Could not load metrics configuration: {e}")

DEFAULT_DAYS_BACK = 30
DEFAULT_TIMEZONE = "Europe/Paris"
REQUIRED_ENV_VARS = ["GARMIN_USERNAME", "GARMIN_PASSWORD"]
DATA_TYPES = [k for k in METRICS_CONFIG.keys() if k != "ingestion"]
