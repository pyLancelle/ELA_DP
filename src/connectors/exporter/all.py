"""
Unified exporter for Cloud Run.
Exports data based on scope: garmin or spotify.

Usage:
    python -m src.connectors.exporter.all --scope garmin --bucket ela-dp-export
    python -m src.connectors.exporter.all --scope spotify --bucket ela-dp-export
"""

import argparse
import os

from src.connectors.exporter.homepage import export_homepage
from src.connectors.exporter.music import export_music_classement
from src.connectors.exporter.activities import export_activities
from src.connectors.exporter.artist_focus import export_artist_focus

GCS_BUCKET = os.getenv("GCS_EXPORT_BUCKET", "ela-dp-export")
ACTIVITIES_LIMIT = int(os.getenv("ACTIVITIES_LIMIT", "5"))


def export_garmin(
    bucket: str, dry_run: bool = False, limit: int | None = None
) -> list[str]:
    """
    Export Garmin-related data.
    - activities_list.json (full)
    - activities_recent.json (full)
    - activity_{id}.json (last N only, default 5)
    """
    print("=" * 50)
    print("GARMIN EXPORT")
    print("=" * 50)

    activities_limit = limit if limit is not None else ACTIVITIES_LIMIT

    return export_activities(
        bucket_name=bucket,
        dry_run=dry_run,
        limit=activities_limit,
    )


def export_spotify(bucket: str, dry_run: bool = False) -> list[str]:
    """
    Export Spotify-related data.
    - homepage.json
    - music_classement_{period}.json (all periods)
    - artist_focus_index.json + artist_focus_{id}.json (all artists)
    """
    print("=" * 50)
    print("SPOTIFY EXPORT")
    print("=" * 50)

    uris = []

    # Homepage
    print("\n[1/3] Exporting homepage...")
    uri = export_homepage(bucket_name=bucket, dry_run=dry_run)
    if uri != "dry-run":
        uris.append(uri)

    # Music classement
    print("\n[2/3] Exporting music classement...")
    music_uris = export_music_classement(bucket_name=bucket, dry_run=dry_run)
    uris.extend(music_uris)

    # Artist focus profiles
    print("\n[3/3] Exporting artist focus profiles...")
    artist_uris = export_artist_focus(bucket_name=bucket, dry_run=dry_run)
    uris.extend(artist_uris)

    return uris


def export_all(
    scope: str, bucket: str, dry_run: bool = False, limit: int | None = None
) -> list[str]:
    """
    Export data based on scope.

    Args:
        scope: 'garmin' or 'spotify'
        bucket: GCS bucket name
        dry_run: If True, print summary instead of uploading
        limit: For garmin scope, limit activity details export to N most recent

    Returns:
        List of GCS URIs of uploaded files
    """
    if scope == "garmin":
        return export_garmin(bucket, dry_run, limit=limit)
    elif scope == "spotify":
        return export_spotify(bucket, dry_run)
    else:
        raise ValueError(f"Unknown scope: {scope}. Valid values: 'garmin', 'spotify'")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Unified exporter for Cloud Run",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Export Garmin data (activities)
  python -m src.connectors.exporter.all --scope garmin --bucket ela-dp-export

  # Export Spotify data (homepage + music)
  python -m src.connectors.exporter.all --scope spotify --bucket ela-dp-export

  # Dry run
  python -m src.connectors.exporter.all --scope spotify --dry-run
        """,
    )
    parser.add_argument(
        "--scope",
        required=True,
        choices=["garmin", "spotify"],
        help="Export scope: 'garmin' (activities) or 'spotify' (homepage + music)",
    )
    parser.add_argument("--bucket", help="GCS bucket name", default=GCS_BUCKET)
    parser.add_argument(
        "--dry-run", action="store_true", help="Print summary instead of uploading"
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Limit activity details export to N most recent (garmin scope only)",
        default=None,
    )

    args = parser.parse_args()

    print(
        f"Exporter: scope={args.scope}, bucket={args.bucket}, dry_run={args.dry_run}, limit={args.limit or ACTIVITIES_LIMIT}"
    )
    print()

    uris = export_all(
        scope=args.scope, bucket=args.bucket, dry_run=args.dry_run, limit=args.limit
    )

    print()
    print("=" * 50)
    print(f"EXPORT COMPLETE: {len(uris)} files")
    print("=" * 50)
