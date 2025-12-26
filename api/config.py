import os

PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT", "polar-scene-465223-f7")
DATASET = "dp_product_dev"

VALID_PERIODS = [
    "yesterday",
    "last_7_days",
    "last_30_days",
    "last_365_days",
    "all_time",
]

DEFAULT_LIMIT = 10
MAX_LIMIT = 100
