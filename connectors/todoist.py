import requests
from datetime import datetime
from pathlib import Path
from utils import get_token, get_settings, to_jsonl, setup_logger
import logging

TODOIST_API_URL = "https://api.todoist.com/rest/v2/tasks"
TODOIST_PROJECTS_URL = "https://api.todoist.com/rest/v2/projects"
TODOIST_SECTIONS_URL = "https://api.todoist.com/rest/v2/sections"
TODOIST_ACTIVITY_URL = "https://api.todoist.com/sync/v9/activity/get"


def fetch_todoist(endpoint, token, params=None):
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(endpoint, headers=headers, params=params)
    resp.raise_for_status()
    return resp.json()


def fetch_projects(token):
    return fetch_todoist(TODOIST_PROJECTS_URL, token)


def fetch_sections(token, project_id):
    return fetch_todoist(TODOIST_SECTIONS_URL, token, params={"project_id": project_id})


def fetch_tasks(token, limit):
    return fetch_todoist(TODOIST_API_URL, token, params={"limit": limit})


def fetch_activity_logs(token, limit):
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(TODOIST_ACTIVITY_URL, headers=headers, params={"limit": limit})
    resp.raise_for_status()
    return resp.json().get("events", [])


def main():
    setup_logger()
    config = get_settings()
    token = get_token("TODOIST_API_TOKEN")
    today = datetime.today().strftime("%Y_%m_%d_")
    data_dir = Path(__file__).parent.parent / "data"
    data_dir.mkdir(parents=True, exist_ok=True)

    # Projects
    logging.info("Fetching projects...")
    projects = fetch_projects(token)
    to_jsonl(projects, str(data_dir / f"{today}todoist_projects.jsonl"))

    # Sections
    logging.info("Fetching sections...")
    all_sections = []
    for project in projects:
        sections = fetch_sections(token, project["id"])
        for section in sections:
            section["project_id"] = project["id"]
        all_sections.extend(sections)
    to_jsonl(all_sections, str(data_dir / f"{today}todoist_sections.jsonl"))

    # Tasks
    limit = config.get("todoist", {}).get("tasks_limit", 200)
    logging.info(f"Fetching up to {limit} tasks...")
    tasks = fetch_tasks(token, limit)
    to_jsonl(tasks, str(data_dir / f"{today}todoist_tasks.jsonl"))

    # Activity logs
    activity_limit = config.get("todoist", {}).get("activity_limit", 200)
    logging.info(f"Fetching up to {activity_limit} activity logs...")
    logs = fetch_activity_logs(token, activity_limit)
    to_jsonl(logs, str(data_dir / f"{today}todoist_activity_logs.jsonl"))

    logging.info("All data fetched and saved to JSONL.")


if __name__ == "__main__":
    main()
