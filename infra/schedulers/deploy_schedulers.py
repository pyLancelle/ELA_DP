#!/usr/bin/env python3
"""
Deploy Cloud Schedulers from YAML configuration.

Usage:
    python3 deploy_schedulers.py          # Deploy all schedulers
    python3 deploy_schedulers.py --dry-run # Show what would be deployed
"""

import yaml
import subprocess
import sys
import argparse

PROJECT = "polar-scene-465223-f7"
LOCATION = "europe-west1"
SERVICE_ACCOUNT = "185493502538-compute@developer.gserviceaccount.com"


def load_config():
    """Load schedulers from YAML file."""
    with open("infra/schedulers/schedulers.yaml", "r") as f:
        return yaml.safe_load(f)


def scheduler_exists(name):
    """Check if scheduler already exists."""
    result = subprocess.run(
        [
            "gcloud",
            "scheduler",
            "jobs",
            "describe",
            name,
            "--location",
            LOCATION,
            "--project",
            PROJECT,
            "--format",
            "value(name)",
        ],
        capture_output=True,
        text=True,
    )
    return result.returncode == 0


def create_or_update_scheduler(scheduler, dry_run=False):
    """Create or update a scheduler."""
    name = scheduler["name"]
    workflow = scheduler["workflow"]

    uri = (
        f"https://workflowexecutions.googleapis.com/v1/"
        f"projects/{PROJECT}/locations/{LOCATION}/workflows/{workflow}/executions"
    )

    exists = scheduler_exists(name)
    action = "update" if exists else "create"

    cmd = [
        "gcloud",
        "scheduler",
        "jobs",
        action,
        "http",
        name,
        "--location",
        LOCATION,
        "--project",
        PROJECT,
        "--schedule",
        scheduler["schedule"],
        "--time-zone",
        scheduler["timezone"],
        "--uri",
        uri,
        "--http-method",
        "POST",
        "--oauth-service-account-email",
        SERVICE_ACCOUNT,
        "--description",
        scheduler["description"],
    ]

    if dry_run:
        print(f"[DRY RUN] Would {action} scheduler: {name}")
        print(f"  Schedule: {scheduler['schedule']}")
        print(f"  Workflow: {workflow}")
        print()
        return

    print(f"{'Updating' if exists else 'Creating'} scheduler: {name}")
    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode == 0:
        print(f"✅ {name} {action}d successfully")
    else:
        print(f"❌ Failed to {action} {name}")
        print(result.stderr)
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description="Deploy Cloud Schedulers")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be deployed without deploying",
    )
    args = parser.parse_args()

    config = load_config()
    schedulers = config["schedulers"]

    print(
        f"🕐 {'[DRY RUN] ' if args.dry_run else ''}Deploying {len(schedulers)} schedulers...\n"
    )

    for scheduler in schedulers:
        create_or_update_scheduler(scheduler, dry_run=args.dry_run)

    if args.dry_run:
        print("🔍 Dry run completed. No changes were made.")
    else:
        print("\n🎉 All schedulers deployed!")


if __name__ == "__main__":
    main()
