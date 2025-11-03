#!/usr/bin/env python3
"""
Spotify Auto-Ingestion Script
==============================

Intelligent ingestion script that:
1. Scans GCS landing folder for files
2. Detects data type from filename pattern
3. Routes to appropriate ingest_v2 config
4. Processes all data types in one run

Usage:
    python -m src.connectors.spotify.spotify_ingest_auto --env dev
    python -m src.connectors.spotify.spotify_ingest_auto --env prd --dry-run
"""

import argparse
import logging
import re
import sys
from collections import defaultdict
from typing import Dict, List, Tuple
from pathlib import Path

from google.cloud import storage
from google.api_core import exceptions as gcp_exceptions


# =============================================================================
# FILE PATTERN DETECTION
# =============================================================================

# Mapping: filename pattern → config name
FILE_PATTERNS = {
    r'.*_recently_played\.jsonl$': 'recently_played',
    r'.*_saved_tracks\.jsonl$': 'saved_tracks',
    r'.*_saved_albums\.jsonl$': 'saved_albums',
}

# Configs that are ready for ingest_v2
SUPPORTED_CONFIGS = {
    'recently_played',
    'saved_tracks',
    'saved_albums',
}


def detect_data_type(filename: str) -> str | None:
    """
    Detect data type from filename pattern.

    Args:
        filename: Name of the file (e.g., "2025_10_28_spotify_recently_played.jsonl")

    Returns:
        Data type (e.g., "recently_played") or None if not recognized
    """
    for pattern, data_type in FILE_PATTERNS.items():
        if re.match(pattern, filename, re.IGNORECASE):
            return data_type
    return None


def is_supported(data_type: str) -> bool:
    """Check if data type has a v2 config available."""
    return data_type in SUPPORTED_CONFIGS


# =============================================================================
# GCS FILE SCANNING
# =============================================================================

def scan_landing_folder(bucket_name: str, landing_path: str) -> Dict[str, List[str]]:
    """
    Scan GCS landing folder and group files by data type.

    Args:
        bucket_name: GCS bucket name (e.g., "ela-dp-dev")
        landing_path: Landing folder path (e.g., "spotify/landing")

    Returns:
        Dictionary mapping data_type → list of GCS URIs
    """
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    # List all .jsonl files in landing
    blobs = bucket.list_blobs(prefix=landing_path)

    files_by_type = defaultdict(list)
    unsupported_files = []

    for blob in blobs:
        if not blob.name.endswith('.jsonl'):
            continue

        filename = Path(blob.name).name
        gcs_uri = f"gs://{bucket_name}/{blob.name}"

        data_type = detect_data_type(filename)

        if data_type is None:
            logging.warning(f"⚠️  Unknown file pattern: {filename}")
            unsupported_files.append(gcs_uri)
            continue

        if not is_supported(data_type):
            logging.info(f"ℹ️  Config not yet available for '{data_type}': {filename}")
            unsupported_files.append(gcs_uri)
            continue

        files_by_type[data_type].append(gcs_uri)

    # Log summary
    logging.info(f"\n{'='*80}")
    logging.info("GCS LANDING FOLDER SCAN RESULTS")
    logging.info(f"{'='*80}")
    logging.info(f"Bucket: gs://{bucket_name}/{landing_path}")
    logging.info(f"Total supported data types found: {len(files_by_type)}")

    for data_type, files in sorted(files_by_type.items()):
        logging.info(f"  • {data_type}: {len(files)} file(s)")

    if unsupported_files:
        logging.info(f"  • unsupported/unknown: {len(unsupported_files)} file(s)")

    logging.info(f"{'='*80}\n")

    return dict(files_by_type)


# =============================================================================
# INGESTION ORCHESTRATION
# =============================================================================

def run_ingestion(data_type: str, env: str, dry_run: bool = False) -> Tuple[bool, str]:
    """
    Run ingest_v2 for a specific data type.

    Args:
        data_type: Data type config name (e.g., "recently_played")
        env: Environment (dev/prd)
        dry_run: If True, don't actually insert or move files

    Returns:
        Tuple of (success, error_message)
    """
    import subprocess

    cmd = [
        sys.executable,
        "-m", "src.connectors.spotify.spotify_ingest_v2",
        "--config", data_type,
        "--env", env
    ]

    if dry_run:
        cmd.append("--dry-run")

    logging.info(f"\n{'='*80}")
    logging.info(f"INGESTING: {data_type.upper()}")
    logging.info(f"{'='*80}")
    logging.info(f"Command: {' '.join(cmd)}\n")

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=600  # 10 minutes timeout
        )

        # Forward stdout/stderr
        if result.stdout:
            print(result.stdout)
        if result.stderr:
            print(result.stderr, file=sys.stderr)

        if result.returncode == 0:
            logging.info(f"✅ Successfully ingested {data_type}\n")
            return True, ""
        else:
            error_msg = f"Failed with exit code {result.returncode}"
            logging.error(f"❌ {error_msg}\n")
            return False, error_msg

    except subprocess.TimeoutExpired:
        error_msg = "Ingestion timed out after 10 minutes"
        logging.error(f"❌ {error_msg}\n")
        return False, error_msg
    except Exception as e:
        error_msg = f"Unexpected error: {e}"
        logging.error(f"❌ {error_msg}\n")
        return False, error_msg


def run_auto_ingestion(env: str, dry_run: bool = False) -> int:
    """
    Main orchestration: scan landing, detect types, run ingestions.

    Args:
        env: Environment (dev/prd)
        dry_run: If True, don't actually insert or move files

    Returns:
        Exit code (0 = success, 1 = errors occurred)
    """
    logging.info(f"\n{'='*80}")
    logging.info("SPOTIFY AUTO-INGESTION")
    logging.info(f"{'='*80}")
    logging.info(f"Environment: {env.upper()}")
    logging.info(f"Dry run: {dry_run}")
    logging.info(f"{'='*80}\n")

    # Determine bucket and paths based on environment
    bucket_name = f"ela-dp-{env}"
    landing_path = "spotify/landing"

    try:
        # Phase 1: Scan landing folder
        files_by_type = scan_landing_folder(bucket_name, landing_path)

        if not files_by_type:
            logging.warning("⚠️  No files found in landing folder")
            return 0

        # Phase 2: Run ingestion for each data type
        results = {}
        for data_type in sorted(files_by_type.keys()):
            success, error_msg = run_ingestion(data_type, env, dry_run)
            results[data_type] = (success, error_msg)

        # Phase 3: Summary
        logging.info(f"\n{'='*80}")
        logging.info("AUTO-INGESTION SUMMARY")
        logging.info(f"{'='*80}")

        success_count = sum(1 for success, _ in results.values() if success)
        failure_count = len(results) - success_count

        logging.info(f"Total data types processed: {len(results)}")
        logging.info(f"  ✅ Successful: {success_count}")
        logging.info(f"  ❌ Failed: {failure_count}")
        logging.info("")

        for data_type, (success, error_msg) in sorted(results.items()):
            status = "✅" if success else "❌"
            logging.info(f"{status} {data_type}")
            if error_msg:
                logging.info(f"     Error: {error_msg}")

        logging.info(f"{'='*80}\n")

        # Return exit code
        return 0 if failure_count == 0 else 1

    except gcp_exceptions.NotFound:
        logging.error(f"❌ GCS bucket not found: gs://{bucket_name}")
        return 1
    except gcp_exceptions.Forbidden:
        logging.error(f"❌ Access denied to bucket: gs://{bucket_name}")
        logging.error("   Check GCP_SERVICE_ACCOUNT_KEY permissions")
        return 1
    except Exception as e:
        logging.error(f"❌ Fatal error: {e}")
        import traceback
        logging.debug(traceback.format_exc())
        return 1


# =============================================================================
# CLI
# =============================================================================

def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Spotify Auto-Ingestion: Automatically detect and ingest all data types',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Ingest all Spotify data types in dev
  python -m src.connectors.spotify.spotify_ingest_auto --env dev

  # Dry run in prod (no actual insertion)
  python -m src.connectors.spotify.spotify_ingest_auto --env prd --dry-run

  # With verbose logging
  python -m src.connectors.spotify.spotify_ingest_auto --env dev --log-level DEBUG
        """
    )

    parser.add_argument(
        '--env',
        required=True,
        choices=['dev', 'prd'],
        help='Target environment (dev or prd)'
    )

    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Dry run mode: parse and validate without inserting or moving files'
    )

    parser.add_argument(
        '--log-level',
        default='INFO',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        help='Logging level (default: INFO)'
    )

    args = parser.parse_args()

    # Setup logging
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format='%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # Run auto-ingestion
    exit_code = run_auto_ingestion(args.env, args.dry_run)
    sys.exit(exit_code)


if __name__ == '__main__':
    main()
