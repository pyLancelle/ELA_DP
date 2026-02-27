"""
Export activities data from BigQuery to GCS as JSON.
Generates:
- activities_list.json: List of all activities for cards display
- activities_recent.json: Recent activities summary
- activity_{id}.json: Detail for each activity
"""

import json
import os
from datetime import date, datetime, timezone
from decimal import Decimal
from google.cloud import bigquery, storage

PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT", "polar-scene-465223-f7")
DATASET = "dp_product_dev"
DATASET_HUB = "dp_hub_dev"
GCS_BUCKET = os.getenv("GCS_EXPORT_BUCKET", "ela-dp-export")


def json_serializer(obj):
    """Custom JSON serializer for types not serializable by default."""
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError(f"Type {type(obj)} not serializable")


def get_bq_client():
    return bigquery.Client(project=PROJECT_ID)


def query_to_list(client: bigquery.Client, query: str) -> list[dict]:
    """Execute query and return list of dicts."""
    results = client.query(query).result()
    return [dict(row) for row in results]


def fetch_activities_list(client: bigquery.Client) -> list[dict]:
    """Fetch activities list for cards display."""
    query = f"""
        SELECT
            activityId,
            activityName,
            startTimeGMT,
            typeKey,
            distance_km,
            duration_minutes,
            averageHR,
            hrZone1_pct,
            hrZone2_pct,
            hrZone3_pct,
            hrZone4_pct,
            hrZone5_pct,
            polyline_simplified
        FROM `{PROJECT_ID}.{DATASET}.pct_activites__list`
        ORDER BY startTimeGMT DESC
    """
    return query_to_list(client, query)


def fetch_activities_recent(client: bigquery.Client) -> list[dict]:
    """Fetch recent activities summary."""
    query = f"""
        SELECT
            activityId,
            activityName,
            startTimeGMT,
            ROUND(distance / 1000, 2) AS distance_km,
            CAST(ROUND(duration / 60, 0) AS INT64) AS duration_minutes,
            averageSpeed,
            typeKey
        FROM `{PROJECT_ID}.{DATASET}.pct_activites__last_run`
        ORDER BY startTimeGMT DESC
    """
    return query_to_list(client, query)


def fetch_activity_ids(client: bigquery.Client, limit: int | None = None) -> list[int]:
    """Fetch activity IDs, optionally limited to the N most recent."""
    limit_clause = f"LIMIT {limit}" if limit else ""
    query = f"""
        SELECT DISTINCT activityId
        FROM `{PROJECT_ID}.{DATASET}.pct_activites__last_run`
        ORDER BY activityId DESC
        {limit_clause}
    """
    results = query_to_list(client, query)
    return [r["activityId"] for r in results]


def fetch_activity_detail(client: bigquery.Client, activity_id: int) -> dict | None:
    """Fetch detail for a single activity."""
    query = f"""
        SELECT
            activityId,
            activityName,
            startTimeGMT,
            endTimeGMT,
            typeKey,
            distance,
            duration,
            elapsedDuration,
            elevationGain,
            elevationLoss,
            minElevation,
            maxElevation,
            averageSpeed,
            averageHR,
            maxHR,
            calories,
            hasPolyline,
            aerobicTrainingEffect,
            anaerobicTrainingEffect,
            activityTrainingLoad,
            fastestSplits,
            hr_zones,
            power_zones,
            kilometer_laps,
            training_intervals,
            time_series,
            tracks_played
        FROM `{PROJECT_ID}.{DATASET}.pct_activites__last_run`
        WHERE activityId = {activity_id}
        LIMIT 1
    """
    results = query_to_list(client, query)
    if not results:
        return None

    data = results[0]

    # Fetch GPS coordinates from hub
    poly_query = f"""
        SELECT p.lat AS lat, p.lon AS lng
        FROM `{PROJECT_ID}.{DATASET_HUB}.hub_health__svc_activities`,
             UNNEST(polyline) AS p
        WHERE activityId = {activity_id}
        ORDER BY p.time
    """
    poly_results = query_to_list(client, poly_query)
    data["coordinates"] = poly_results if poly_results else None

    return data


def upload_to_gcs(data: dict | list, bucket_name: str, blob_name: str) -> str:
    """Upload JSON data to GCS and return GCS URI."""
    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    json_content = json.dumps(
        data, default=json_serializer, ensure_ascii=False, indent=2
    )

    blob.upload_from_string(json_content, content_type="application/json")

    return f"gs://{bucket_name}/{blob_name}"


def export_activities(
    bucket_name: str | None = None,
    dry_run: bool = False,
    skip_details: bool = False,
    limit: int | None = None,
) -> list[str]:
    """
    Export activities data to GCS.

    Args:
        bucket_name: GCS bucket name (defaults to GCS_EXPORT_BUCKET env var)
        dry_run: If True, print summary instead of uploading
        skip_details: If True, skip individual activity detail exports
        limit: If set, only export the N most recent activity details

    Returns:
        List of GCS URIs of uploaded files
    """
    bucket = bucket_name or GCS_BUCKET

    print("Fetching activities data from BigQuery...")
    client = get_bq_client()

    uris = []

    # 1. Export activities list (always full)
    print("  Fetching activities_list...")
    activities_list = fetch_activities_list(client)
    list_data = {
        "activities": activities_list,
        "_generated_at": datetime.now(timezone.utc).isoformat(),
    }

    if dry_run:
        print(
            f"    Would export activities_list.json ({len(activities_list)} activities)"
        )
    else:
        uri = upload_to_gcs(list_data, bucket, "activities_list.json")
        uris.append(uri)
        print(f"    Done: {uri}")

    # 2. Export activities recent (always full)
    print("  Fetching activities_recent...")
    activities_recent = fetch_activities_recent(client)
    recent_data = {
        "activities": activities_recent,
        "_generated_at": datetime.now(timezone.utc).isoformat(),
    }

    if dry_run:
        print(
            f"    Would export activities_recent.json ({len(activities_recent)} activities)"
        )
    else:
        uri = upload_to_gcs(recent_data, bucket, "activities_recent.json")
        uris.append(uri)
        print(f"    Done: {uri}")

    # 3. Export individual activity details
    if skip_details:
        print("  Skipping activity details (--skip-details)")
    else:
        print("  Fetching activity IDs...")
        activity_ids = fetch_activity_ids(client, limit=limit)
        limit_msg = f" (limited to {limit})" if limit else ""
        print(f"    Found {len(activity_ids)} activities{limit_msg}")

        for i, activity_id in enumerate(activity_ids, 1):
            print(f"  [{i}/{len(activity_ids)}] Exporting activity {activity_id}...")

            if dry_run:
                print(f"    Would export activity_{activity_id}.json")
                continue

            detail = fetch_activity_detail(client, activity_id)
            if detail:
                detail["_generated_at"] = datetime.now(timezone.utc).isoformat()
                uri = upload_to_gcs(detail, bucket, f"activity_{activity_id}.json")
                uris.append(uri)

    if not dry_run:
        print(f"\nExported {len(uris)} files to gs://{bucket}/")

    return uris


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Export activities data to GCS")
    parser.add_argument("--bucket", help="GCS bucket name", default=None)
    parser.add_argument(
        "--dry-run", action="store_true", help="Print summary instead of uploading"
    )
    parser.add_argument(
        "--skip-details", action="store_true", help="Skip individual activity exports"
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Only export the N most recent activity details",
        default=None,
    )

    args = parser.parse_args()

    export_activities(
        bucket_name=args.bucket,
        dry_run=args.dry_run,
        skip_details=args.skip_details,
        limit=args.limit,
    )
