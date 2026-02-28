"""
Export artist focus data from BigQuery to GCS as JSON.
Generates:
  - artist_focus_index.json              → liste de tous les artistes (autocomplete)
  - artist_focus_{artist_id}.json        → profil complet par artiste
"""

import json
import os
from collections import defaultdict
from datetime import date, datetime, timezone
from decimal import Decimal

from google.cloud import bigquery, storage

PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT", "polar-scene-465223-f7")
DATASET = "dp_product_dev"
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
    return [dict(row) for row in client.query(query).result()]


def fetch_overview(client: bigquery.Client) -> list[dict]:
    return query_to_list(
        client,
        f"SELECT * FROM `{PROJECT_ID}.{DATASET}.pct_focus_artist__overview` ORDER BY total_plays DESC",
    )


def fetch_top_tracks(client: bigquery.Client) -> list[dict]:
    return query_to_list(
        client,
        f"SELECT * FROM `{PROJECT_ID}.{DATASET}.pct_focus_artist__top_tracks` ORDER BY artist_id, track_rank ASC",
    )


def fetch_albums(client: bigquery.Client) -> list[dict]:
    return query_to_list(
        client,
        f"SELECT * FROM `{PROJECT_ID}.{DATASET}.pct_focus_artist__albums` ORDER BY artist_id, total_duration_ms DESC",
    )


def fetch_calendar(client: bigquery.Client) -> list[dict]:
    return query_to_list(
        client,
        f"SELECT * FROM `{PROJECT_ID}.{DATASET}.pct_focus_artist__listening_calendar` ORDER BY artist_id, listen_date ASC",
    )


def fetch_heatmap(client: bigquery.Client) -> list[dict]:
    return query_to_list(
        client,
        f"SELECT * FROM `{PROJECT_ID}.{DATASET}.pct_focus_artist__listening_heatmap` ORDER BY artist_id, day_of_week, hour_of_day ASC",
    )


def fetch_evolution(client: bigquery.Client) -> list[dict]:
    return query_to_list(
        client,
        f"SELECT * FROM `{PROJECT_ID}.{DATASET}.pct_focus_artist__evolution` ORDER BY artist_id, year_month ASC",
    )


def group_by_artist(rows: list[dict]) -> dict[str, list[dict]]:
    """Group a flat list of rows into a dict keyed by artist_id."""
    grouped: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        grouped[row["artist_id"]].append(row)
    return grouped


def upload_to_gcs(data: dict, bucket_name: str, blob_name: str) -> str:
    client = storage.Client(project=PROJECT_ID)
    blob = client.bucket(bucket_name).blob(blob_name)
    blob.upload_from_string(
        json.dumps(data, default=json_serializer, ensure_ascii=False, indent=2),
        content_type="application/json",
    )
    return f"gs://{bucket_name}/{blob_name}"


def export_artist_focus(
    bucket_name: str | None = None,
    dry_run: bool = False,
) -> list[str]:
    """
    Export focus-artist data to GCS.

    Fetches all 6 product tables in bulk (one query per table), groups rows
    by artist_id in Python, then writes one JSON file per artist plus an index.

    Args:
        bucket_name: GCS bucket (defaults to GCS_EXPORT_BUCKET env var)
        dry_run:     Print summary instead of uploading

    Returns:
        List of GCS URIs written
    """
    bucket = bucket_name or GCS_BUCKET
    client = get_bq_client()

    print("  Fetching overview...")
    overview_rows = fetch_overview(client)
    print(f"    → {len(overview_rows)} artists")

    print("  Fetching top_tracks...")
    tracks_by_artist = group_by_artist(fetch_top_tracks(client))

    print("  Fetching albums...")
    albums_by_artist = group_by_artist(fetch_albums(client))

    print("  Fetching listening_calendar...")
    calendar_by_artist = group_by_artist(fetch_calendar(client))

    print("  Fetching listening_heatmap...")
    heatmap_by_artist = group_by_artist(fetch_heatmap(client))

    print("  Fetching evolution...")
    evolution_by_artist = group_by_artist(fetch_evolution(client))

    generated_at = datetime.now(timezone.utc).isoformat()

    # Build index (lightweight: id + name + image + global stats)
    index = {
        "artists": [
            {
                "artist_id": r["artist_id"],
                "artist_name": r["artist_name"],
                "image_url": r.get("image_url"),
                "total_plays": r["total_plays"],
                "total_duration": r["total_duration"],
                "current_streak": r["current_streak"],
            }
            for r in overview_rows
        ],
        "_generated_at": generated_at,
    }

    if dry_run:
        print(f"\n--- DRY RUN: artist_focus_index.json ({len(overview_rows)} artists) ---")
        print(json.dumps(index, default=json_serializer, ensure_ascii=False, indent=2)[:1000])
        print("... (truncated)")
        if overview_rows:
            sample_id = overview_rows[0]["artist_id"]
            sample = _build_artist_payload(
                sample_id, overview_rows[0],
                tracks_by_artist, albums_by_artist,
                calendar_by_artist, heatmap_by_artist,
                evolution_by_artist, generated_at,
            )
            print(f"\n--- DRY RUN: artist_focus/{sample_id}.json ---")
            print(json.dumps(sample, default=json_serializer, ensure_ascii=False, indent=2)[:1000])
            print("... (truncated)")
        return []

    uris = []

    # Upload index
    blob_name = "artist_focus/index.json"
    print(f"\n  Uploading {blob_name}...")
    uris.append(upload_to_gcs(index, bucket, blob_name))

    # Upload one file per artist
    for overview in overview_rows:
        artist_id = overview["artist_id"]
        artist_name = overview["artist_name"]
        payload = _build_artist_payload(
            artist_id, overview,
            tracks_by_artist, albums_by_artist,
            calendar_by_artist, heatmap_by_artist,
            evolution_by_artist, generated_at,
        )
        blob_name = f"artist_focus/{artist_id}.json"
        uri = upload_to_gcs(payload, bucket, blob_name)
        uris.append(uri)
        print(f"    {artist_name} → {uri}")

    print(f"\n  Exported {len(uris)} files to gs://{bucket}/artist_focus/")
    return uris


def _build_artist_payload(
    artist_id: str,
    overview: dict,
    tracks_by_artist: dict,
    albums_by_artist: dict,
    calendar_by_artist: dict,
    heatmap_by_artist: dict,
    evolution_by_artist: dict,
    generated_at: str,
) -> dict:
    return {
        "overview": overview,
        "top_tracks": tracks_by_artist.get(artist_id, []),
        "albums": albums_by_artist.get(artist_id, []),
        "calendar": calendar_by_artist.get(artist_id, []),
        "heatmap": heatmap_by_artist.get(artist_id, []),
        "evolution": evolution_by_artist.get(artist_id, []),
        "_generated_at": generated_at,
    }


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Export artist focus data to GCS")
    parser.add_argument("--bucket", help="GCS bucket name", default=None)
    parser.add_argument(
        "--dry-run", action="store_true", help="Print JSON instead of uploading"
    )

    args = parser.parse_args()
    export_artist_focus(bucket_name=args.bucket, dry_run=args.dry_run)
