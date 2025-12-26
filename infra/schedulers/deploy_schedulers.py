#!/usr/bin/env python3

import json
import subprocess
import sys

PROJECT = "polar-scene-465223-f7"
LOCATION = "europe-west1"
SERVICE_ACCOUNT = "185493502538-compute@developer.gserviceaccount.com"


def load_config():
    with open("infra/schedulers/schedulers.json", "r") as f:
        return json.load(f)


def scheduler_exists(name):
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


def create_or_update_scheduler(scheduler):
    name = scheduler["name"]
    workflow = scheduler["workflow"]

    uri = f"https://workflowexecutions.googleapis.com/v1/projects/{PROJECT}/locations/{LOCATION}/workflows/{workflow}/executions"

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

    print(f"{'Updating' if exists else 'Creating'} scheduler: {name}")
    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode == 0:
        print(f"✅ {name} {action}d successfully")
    else:
        print(f"❌ Failed to {action} {name}")
        print(result.stderr)
        sys.exit(1)


def main():
    config = load_config()

    print(f"🕐 Deploying {len(config['schedulers'])} schedulers...\n")

    for scheduler in config["schedulers"]:
        create_or_update_scheduler(scheduler)

    print("\n🎉 All schedulers deployed!")


if __name__ == "__main__":
    main()
