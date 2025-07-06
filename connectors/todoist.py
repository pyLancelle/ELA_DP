#!/usr/bin/env python3


"""
Todoist data extraction utility.
Fetches projects, sections, tasks, and activity logs, and exports them as CSV files.
Follows PEP8 and project coding standards.
Documentation: https://developer.todoist.com/rest/v2/
"""

import os
import requests
import pandas as pd
import json
from dotenv import load_dotenv
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Any


__all__ = [
    "fetch_projects",
    "fetch_sections",
    "fetch_tasks",
    "fetch_activity_logs",
    "main",
]

# Endpoints
TODOIST_API_URL = "https://api.todoist.com/rest/v2/tasks"
TODOIST_PROJECTS_URL = "https://api.todoist.com/rest/v2/projects"
TODOIST_SECTIONS_URL = "https://api.todoist.com/rest/v2/sections"
TODOIST_ACTIVITY_URL = "https://api.todoist.com/sync/v9/activity/get"


def get_token() -> str:
    """Load Todoist API token from environment or .env file."""
    load_dotenv()
    token = os.getenv("TODOIST_API_TOKEN")
    if not token:
        raise RuntimeError(
            "Vous devez définir la variable d'environnement TODOIST_API_TOKEN"
        )
    return token


def fetch_projects(token: Optional[str] = None) -> List[Any]:
    """Fetch all Todoist projects."""
    if token is None:
        token = get_token()
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(TODOIST_PROJECTS_URL, headers=headers)
    response.raise_for_status()
    return response.json()


def fetch_sections(
    token: Optional[str] = None, project_id: Optional[str] = None
) -> List[Any]:
    """Fetch all sections for a given project (or all if project_id is None)."""
    if token is None:
        token = get_token()
    headers = {"Authorization": f"Bearer {token}"}
    params = {"project_id": str(project_id)} if project_id is not None else {}
    response = requests.get(TODOIST_SECTIONS_URL, headers=headers, params=params)
    response.raise_for_status()
    return response.json()


def fetch_tasks(
    token: Optional[str] = None, limit: int = 10, project_id: Optional[str] = None
) -> List[Any]:
    """Fetch tasks, optionally filtered by project, with a limit."""
    if token is None:
        token = get_token()
    headers = {"Authorization": f"Bearer {token}"}
    params = {"limit": limit}
    if project_id is not None:
        params["project_id"] = str(project_id)
    response = requests.get(TODOIST_API_URL, headers=headers, params=params)
    response.raise_for_status()
    tasks = response.json()
    if not isinstance(tasks, list):
        raise RuntimeError(f"Réponse inattendue : {tasks!r}")
    return tasks


def fetch_activity_logs(
    token: Optional[str] = None, limit: int = 30, object_type: Optional[str] = None
) -> List[Any]:
    """Fetch activity logs, optionally filtered by object type."""
    if token is None:
        token = get_token()
    headers = {"Authorization": f"Bearer {token}"}
    params = {"limit": limit}
    if object_type:
        params["object_type"] = object_type
    response = requests.get(TODOIST_ACTIVITY_URL, headers=headers, params=params)
    response.raise_for_status()
    data = response.json()
    return data.get("events", [])


def dump_nested_csv(df: pd.DataFrame, filename: str) -> None:
    """Dump DataFrame to CSV, serializing nested fields as JSON strings."""
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, (dict, list))).any():
            df[col] = df[col].apply(
                lambda x: (
                    json.dumps(x, ensure_ascii=False)
                    if isinstance(x, (dict, list))
                    else x
                )
            )
    df.to_csv(filename, index=False)


def main() -> None:
    """Main entry point: fetch all Todoist data and export as CSV files in the data folder."""
    today = datetime.today().strftime("%Y_%m_%d_")
    data_dir = Path(__file__).parent.parent / "data"
    data_dir.mkdir(parents=True, exist_ok=True)

    print("Fetching projects...")
    token = get_token()
    projects = fetch_projects(token=token)
    dump_nested_csv(
        pd.DataFrame(projects),
        str(data_dir / f"{today}todoist_projects.csv"),
    )

    print("Fetching sections...")
    all_sections = []
    for project in projects:
        sections = fetch_sections(token=token, project_id=project["id"])
        for section in sections:
            section["project_id"] = project["id"]
        all_sections.extend(sections)
    dump_nested_csv(
        pd.DataFrame(all_sections),
        str(data_dir / f"{today}todoist_sections.csv"),
    )

    print("Fetching tasks...")
    tasks = fetch_tasks(token=token, limit=200)
    df_tasks = pd.DataFrame(tasks)
    if "created_at" in df_tasks.columns:
        df_tasks["created_at"] = pd.to_datetime(df_tasks["created_at"])
        if df_tasks["created_at"].dt.tz is not None:
            df_tasks["created_at"] = df_tasks["created_at"].dt.tz_localize(None)
    dump_nested_csv(
        df_tasks,
        str(data_dir / f"{today}todoist_tasks.csv"),
    )

    print("Fetching activity logs...")
    logs = fetch_activity_logs(token=token, limit=100)
    dump_nested_csv(
        pd.DataFrame(logs),
        str(data_dir / f"{today}todoist_activity_logs.csv"),
    )

    print("All data fetched and saved to CSV files in the data folder.")


if __name__ == "__main__":
    main()
