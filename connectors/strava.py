"""
Strava data extraction utility.
Fetches all activities, comments, and kudos, and exports them as CSV files.
PEP8 compliant.
"""

import os
import requests
import pandas as pd
from dotenv import load_dotenv

STRAVA_API_URL = "https://www.strava.com/api/v3"


def get_token():
    """Load Strava API token from environment or .env file."""
    load_dotenv()
    token = os.getenv("STRAVA_API_TOKEN")
    if not token:
        raise RuntimeError("STRAVA_API_TOKEN is not set in environment or .env file.")
    return token


def fetch_activities(token, per_page=200, max_pages=10):
    url = f"{STRAVA_API_URL}/athlete/activities"
    headers = {"Authorization": f"Bearer {token}"}
    all_activities = []
    for page in range(1, max_pages + 1):
        params = {"per_page": per_page, "page": page}
        resp = requests.get(url, headers=headers, params=params)
        resp.raise_for_status()
        activities = resp.json()
        if not activities:
            break
        all_activities.extend(activities)
    return all_activities


def fetch_activity_comments(token, activity_id):
    url = f"{STRAVA_API_URL}/activities/{activity_id}/comments"
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    return resp.json()


def fetch_activity_kudos(token, activity_id):
    url = f"{STRAVA_API_URL}/activities/{activity_id}/kudos"
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    return resp.json()


import json
from datetime import datetime


def dump_nested_csv(df, filename):
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


def main():
    token = get_token()
    today = datetime.today().strftime("%Y_%m_%d_")
    data_dir = os.path.join(os.path.dirname(__file__), "..", "data")
    os.makedirs(data_dir, exist_ok=True)

    print("Fetching activities...")
    activities = fetch_activities(token)
    dump_nested_csv(
        pd.DataFrame(activities),
        os.path.join(data_dir, f"{today}strava_activities.csv"),
    )

    print("Fetching comments and kudos for each activity...")
    all_comments = []
    all_kudos = []
    for activity in activities:
        activity_id = activity.get("id")
        if not activity_id:
            continue
        comments = fetch_activity_comments(token, activity_id)
        for comment in comments:
            comment["activity_id"] = activity_id
        all_comments.extend(comments)
        kudos = fetch_activity_kudos(token, activity_id)
        for kudo in kudos:
            kudo["activity_id"] = activity_id
        all_kudos.extend(kudos)
    dump_nested_csv(
        pd.DataFrame(all_comments),
        os.path.join(data_dir, f"{today}strava_comments.csv"),
    )
    dump_nested_csv(
        pd.DataFrame(all_kudos), os.path.join(data_dir, f"{today}strava_kudos.csv")
    )
    print("All Strava data fetched and saved to CSV files in the data folder.")


if __name__ == "__main__":
    main()
