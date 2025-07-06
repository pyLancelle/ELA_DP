#!/usr/bin/env python3
"""
connectors/todoist/fetch_todoist.py

Exemple de connecteur Todoist : fetch des tâches actives
Documentation API : https://developer.todoist.com/rest/v2/
"""

import os
import requests
import pandas as pd
import json
from typing import List, Dict
from dotenv import load_dotenv
import sys


# Endpoints
TODOIST_API_URL = "https://api.todoist.com/rest/v2/tasks"
TODOIST_PROJECTS_URL = "https://api.todoist.com/rest/v2/projects"
TODOIST_SECTIONS_URL = "https://api.todoist.com/rest/v2/sections"
TODOIST_ACTIVITY_URL = "https://api.todoist.com/sync/v9/activity/get"



def get_token() -> str:
    load_dotenv()
    token = os.getenv("TODOIST_API_TOKEN")
    if not token:
        raise RuntimeError("Vous devez définir la variable d'environnement TODOIST_API_TOKEN")
    return token

def fetch_projects(token=None) -> list:
    if token is None:
        token = get_token()
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(TODOIST_PROJECTS_URL, headers=headers)
    response.raise_for_status()
    return response.json()

def fetch_sections(token=None, project_id: str = None) -> list:
    if token is None:
        token = get_token()
    headers = {"Authorization": f"Bearer {token}"}
    params = {"project_id": project_id} if project_id else {}
    response = requests.get(TODOIST_SECTIONS_URL, headers=headers, params=params)
    response.raise_for_status()
    return response.json()

def fetch_tasks(token=None, limit: int = 10, project_id: str = None) -> list:
    if token is None:
        token = get_token()
    headers = {"Authorization": f"Bearer {token}"}
    params = {"limit": limit}
    if project_id:
        params["project_id"] = project_id
    response = requests.get(TODOIST_API_URL, headers=headers, params=params)
    response.raise_for_status()
    tasks = response.json()
    if not isinstance(tasks, list):
        raise RuntimeError(f"Réponse inattendue : {tasks!r}")
    return tasks

def fetch_activity_logs(token=None, limit: int = 30, object_type: str = None) -> list:
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




from datetime import datetime

def dump_nested_csv(df, filename):
    """Dump DataFrame to CSV, serializing nested fields as JSON strings."""
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, (dict, list))).any():
            df[col] = df[col].apply(lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, (dict, list)) else x)
    df.to_csv(filename, index=False)

def main():
    today = datetime.today().strftime("%Y_%m_%d_")
    data_dir = os.path.join(os.path.dirname(__file__), '..', 'data')
    os.makedirs(data_dir, exist_ok=True)

    print("Fetching projects...")
    token = get_token()
    projects = fetch_projects(token=token)
    dump_nested_csv(pd.DataFrame(projects), os.path.join(data_dir, f"{today}todoist_projects.csv"))

    print("Fetching sections...")
    all_sections = []
    for project in projects:
        sections = fetch_sections(token=token, project_id=project["id"])
        for section in sections:
            section["project_id"] = project["id"]
        all_sections.extend(sections)
    dump_nested_csv(pd.DataFrame(all_sections), os.path.join(data_dir, f"{today}todoist_sections.csv"))

    print("Fetching tasks...")
    tasks = fetch_tasks(token=token, limit=200)
    df_tasks = pd.DataFrame(tasks)
    if "created_at" in df_tasks.columns:
        df_tasks["created_at"] = pd.to_datetime(df_tasks["created_at"])
        if df_tasks["created_at"].dt.tz is not None:
            df_tasks["created_at"] = df_tasks["created_at"].dt.tz_localize(None)
    dump_nested_csv(df_tasks, os.path.join(data_dir, f"{today}todoist_tasks.csv"))

    print("Fetching activity logs...")
    logs = fetch_activity_logs(token=token, limit=100)
    dump_nested_csv(pd.DataFrame(logs), os.path.join(data_dir, f"{today}todoist_activity_logs.csv"))

    print("All data fetched and saved to CSV files in the data folder.")


if __name__ == "__main__":
    main()