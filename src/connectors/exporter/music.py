"""
Export music rankings data from BigQuery to GCS as JSON.
Generates one file per period: music_classement_{period}.json
"""

import json
import os
from datetime import date, datetime, timezone
from decimal import Decimal
from google.cloud import bigquery, storage

PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT", "polar-scene-465223-f7")
DATASET = "dp_product_dev"
GCS_BUCKET = os.getenv("GCS_EXPORT_BUCKET", "ela-dp-export")

VALID_PERIODS = [
    "yesterday",
    "last_7_days",
    "last_30_days",
    "last_365_days",
    "all_time",
]
DEFAULT_LIMIT = 100  # Export all, frontend will slice


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


def fetch_top_artists(
    client: bigquery.Client, period: str, limit: int = DEFAULT_LIMIT
) -> list[dict]:
    """Fetch top artists for a given period."""
    query = f"""
        SELECT
            rank,
            artistname as name,
            play_count,
            total_duration,
            albumimageurl as image_url,
            artistexternalurl as external_url
        FROM `{PROJECT_ID}.{DATASET}.pct_classement__top_artist_by_period`
        WHERE period = '{period}'
        ORDER BY rank ASC
        LIMIT {limit}
    """
    return query_to_list(client, query)


def fetch_top_tracks(
    client: bigquery.Client, period: str, limit: int = DEFAULT_LIMIT
) -> list[dict]:
    """Fetch top tracks for a given period."""
    query = f"""
        SELECT
            rank,
            trackname as name,
            all_artist_names as artist_name,
            play_count,
            total_duration,
            albumimageurl as image_url,
            trackexternalurl as external_url
        FROM `{PROJECT_ID}.{DATASET}.pct_classement__top_track_by_period`
        WHERE period = '{period}'
        ORDER BY rank ASC
        LIMIT {limit}
    """
    return query_to_list(client, query)


def fetch_top_albums(
    client: bigquery.Client, period: str, limit: int = DEFAULT_LIMIT
) -> list[dict]:
    """Fetch top albums for a given period."""
    query = f"""
        SELECT
            rank,
            albumname as name,
            all_artist_names as artist_name,
            play_count,
            total_duration,
            albumimageurl as image_url,
            albumexternalurl as external_url
        FROM `{PROJECT_ID}.{DATASET}.pct_classement__top_album_by_period`
        WHERE period = '{period}'
        ORDER BY rank ASC
        LIMIT {limit}
    """
    return query_to_list(client, query)


def fetch_music_classement(client: bigquery.Client, period: str) -> dict:
    """Fetch all music rankings for a given period."""
    print(f"    Fetching top_artists...")
    top_artists = fetch_top_artists(client, period)
    print(f"    Fetching top_tracks...")
    top_tracks = fetch_top_tracks(client, period)
    print(f"    Fetching top_albums...")
    top_albums = fetch_top_albums(client, period)

    return {
        "period": period,
        "top_artists": top_artists,
        "top_tracks": top_tracks,
        "top_albums": top_albums,
        "_generated_at": datetime.now(timezone.utc).isoformat(),
    }


def upload_to_gcs(data: dict, bucket_name: str, blob_name: str) -> str:
    """Upload JSON data to GCS and return GCS URI."""
    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    json_content = json.dumps(
        data, default=json_serializer, ensure_ascii=False, indent=2
    )

    blob.upload_from_string(json_content, content_type="application/json")

    return f"gs://{bucket_name}/{blob_name}"


def export_music_classement(
    bucket_name: str | None = None,
    periods: list[str] | None = None,
    dry_run: bool = False,
) -> list[str]:
    """
    Export music classement data to GCS.
    Generates one file per period: music_classement_{period}.json

    Args:
        bucket_name: GCS bucket name (defaults to GCS_EXPORT_BUCKET env var)
        periods: List of periods to export (defaults to all)
        dry_run: If True, print JSON to stdout instead of uploading

    Returns:
        List of GCS URIs of uploaded files
    """
    bucket = bucket_name or GCS_BUCKET
    periods_to_export = periods or VALID_PERIODS

    print("Fetching music classement data from BigQuery...")
    client = get_bq_client()

    uris = []
    for period in periods_to_export:
        print(f"  Processing period: {period}")
        data = fetch_music_classement(client, period)

        if dry_run:
            print(f"\n--- DRY RUN: music_classement_{period}.json ---")
            print(
                json.dumps(data, default=json_serializer, ensure_ascii=False, indent=2)[
                    :2000
                ]
            )
            print("... (truncated)")
            continue

        blob_name = f"music_classement_{period}.json"
        print(f"    Uploading to gs://{bucket}/{blob_name}...")
        uri = upload_to_gcs(data, bucket, blob_name)
        uris.append(uri)
        print(f"    Done: {uri}")

    if not dry_run:
        print(f"\nExported {len(uris)} files to gs://{bucket}/")

    return uris


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Export music classement data to GCS")
    parser.add_argument("--bucket", help="GCS bucket name", default=None)
    parser.add_argument(
        "--periods",
        help=f"Comma-separated periods to export (default: all). Valid: {VALID_PERIODS}",
        default=None,
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Print JSON instead of uploading"
    )

    args = parser.parse_args()

    periods = args.periods.split(",") if args.periods else None
    export_music_classement(
        bucket_name=args.bucket, periods=periods, dry_run=args.dry_run
    )
