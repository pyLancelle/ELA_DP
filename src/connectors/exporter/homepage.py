"""
Export homepage data from BigQuery to GCS as JSON.
"""

import json
import os
from datetime import date, datetime, timezone
from decimal import Decimal
from google.cloud import bigquery, storage

PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT", "polar-scene-465223-f7")
DATASET = "dp_product_dev"
GCS_BUCKET = os.getenv("GCS_EXPORT_BUCKET", "ela-dp-frontend")


def json_serializer(obj):
    """Custom JSON serializer for types not serializable by default."""
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError(f"Type {type(obj)} not serializable")


def get_bq_client():
    return bigquery.Client(project=PROJECT_ID)


def query_to_dict(client: bigquery.Client, query: str) -> list[dict]:
    """Execute query and return list of dicts."""
    results = client.query(query).result()
    return [dict(row) for row in results]


def query_to_single(client: bigquery.Client, query: str) -> dict | None:
    """Execute query and return single dict or None."""
    rows = query_to_dict(client, query)
    return rows[0] if rows else None


def fetch_homepage_data(client: bigquery.Client) -> dict:
    """Fetch all homepage data from BigQuery."""

    queries = {
        "music_time_daily": f"""
            SELECT data, avg_duration_formatted
            FROM `{PROJECT_ID}.{DATASET}.pct_homepage__music_time_daily`
        """,
        "race_predictions": f"""
            SELECT distance, `current_date`, `current_time`, previous_date, previous_time, diff_seconds
            FROM `{PROJECT_ID}.{DATASET}.pct_homepage__race_prediction`
            ORDER BY CASE distance WHEN '5K' THEN 1 WHEN '10K' THEN 2 WHEN '21K' THEN 3 WHEN '42K' THEN 4 END
        """,
        "running_weekly": f"""
            SELECT date, day_of_week, total_distance_km, aerobic_score, anaerobic_score
            FROM `{PROJECT_ID}.{DATASET}.pct_homepage__running_weekly`
            ORDER BY date DESC
        """,
        "running_weekly_volume": f"""
            SELECT week_start, number_of_runs, total_distance_km
            FROM `{PROJECT_ID}.{DATASET}.pct_homepage__running_weekly_volume`
            ORDER BY week_start DESC
        """,
        "sleep_stages": f"""
            SELECT date, start_time, end_time, level_name
            FROM `{PROJECT_ID}.{DATASET}.pct_homepage__sleep_stages`
            ORDER BY start_time
        """,
        "top_artists": f"""
            SELECT rank, artistname, total_duration, play_count, artistexternalurl, albumimageurl, artistid
            FROM `{PROJECT_ID}.{DATASET}.pct_homepage__top_artist`
            ORDER BY rank
        """,
        "top_tracks": f"""
            SELECT rank, trackname, all_artist_names, total_duration, play_count, trackExternalUrl, albumimageurl, trackid
            FROM `{PROJECT_ID}.{DATASET}.pct_homepage__top_track`
            ORDER BY rank
        """,
        "vo2max_trend": f"""
            SELECT `current_date`, current_vo2max, weekly_vo2max_array, vo2max_delta_6_months
            FROM `{PROJECT_ID}.{DATASET}.pct_homepage__vo2max_trend`
        """,
        "sleep_scores": f"""
            SELECT average, daily
            FROM `{PROJECT_ID}.{DATASET}.pct_homepage__sleep_scores`
        """,
        "body_battery": f"""
            SELECT average_gain, daily
            FROM `{PROJECT_ID}.{DATASET}.pct_homepage__body_battery`
        """,
        "hrv": f"""
            SELECT average, baseline, daily
            FROM `{PROJECT_ID}.{DATASET}.pct_homepage__hrv`
        """,
        "resting_hr": f"""
            SELECT average, daily
            FROM `{PROJECT_ID}.{DATASET}.pct_homepage__resting_hr`
        """,
        "steps": f"""
            SELECT average, goal, daily
            FROM `{PROJECT_ID}.{DATASET}.pct_homepage__steps`
        """,
        "stress_daily": f"""
            SELECT average_stress, daily
            FROM `{PROJECT_ID}.{DATASET}.pct_homepage__stress_daily`
        """,
    }

    # Fields that return a single object vs a list
    single_fields = {
        "music_time_daily",
        "vo2max_trend",
        "sleep_scores",
        "body_battery",
        "hrv",
        "resting_hr",
        "steps",
        "stress_daily",
    }

    data = {}
    for key, query in queries.items():
        print(f"  Fetching {key}...")
        if key in single_fields:
            data[key] = query_to_single(client, query)
        else:
            data[key] = query_to_dict(client, query)

    # Add metadata
    data["_generated_at"] = datetime.now(timezone.utc).isoformat()

    return data


def upload_to_gcs(data: dict, bucket_name: str, blob_name: str) -> str:
    """Upload JSON data to GCS and return public URL."""
    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    json_content = json.dumps(
        data, default=json_serializer, ensure_ascii=False, indent=2
    )

    blob.upload_from_string(json_content, content_type="application/json")

    return f"gs://{bucket_name}/{blob_name}"


def export_homepage(bucket_name: str | None = None, dry_run: bool = False) -> str:
    """
    Export homepage data to GCS.

    Args:
        bucket_name: GCS bucket name (defaults to GCS_EXPORT_BUCKET env var)
        dry_run: If True, print JSON to stdout instead of uploading

    Returns:
        GCS URI of uploaded file
    """
    bucket = bucket_name or GCS_BUCKET

    print("Fetching homepage data from BigQuery...")
    client = get_bq_client()
    data = fetch_homepage_data(client)

    if dry_run:
        print("\n--- DRY RUN: JSON output ---")
        print(json.dumps(data, default=json_serializer, ensure_ascii=False, indent=2))
        return "dry-run"

    print(f"\nUploading to gs://{bucket}/homepage.json...")
    uri = upload_to_gcs(data, bucket, "homepage.json")
    print(f"Done: {uri}")

    return uri


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Export homepage data to GCS")
    parser.add_argument("--bucket", help="GCS bucket name", default=None)
    parser.add_argument(
        "--dry-run", action="store_true", help="Print JSON instead of uploading"
    )

    args = parser.parse_args()
    export_homepage(bucket_name=args.bucket, dry_run=args.dry_run)
