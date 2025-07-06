import os
import requests
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime, timedelta
import yaml
import json
from utils import dump_nested_csv

STRAVA_API_URL = "https://www.strava.com/api/v3"


def get_settings():
    settings_path = os.path.join(os.path.dirname(__file__), "settings.yaml")
    with open(settings_path) as f:
        return yaml.safe_load(f)


def get_strava_credentials():
    load_dotenv()
    access_token = os.getenv("STRAVA_ACCESS_TOKEN")
    if not access_token:
        raise RuntimeError("Missing STRAVA_ACCESS_TOKEN in environment or .env file.")
    return access_token


def fetch_activities(access_token, days):
    url = f"{STRAVA_API_URL}/athlete/activities"
    headers = {"Authorization": f"Bearer {access_token}"}
    after = int((datetime.now() - timedelta(days=days)).timestamp())
    resp = requests.get(url, headers=headers, params={"after": after})
    resp.raise_for_status()
    return resp.json()


def fetch_activity_kudos(access_token, activity_id):
    url = f"{STRAVA_API_URL}/activities/{activity_id}/kudos"
    headers = {"Authorization": f"Bearer {access_token}"}
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    return resp.json()


def fetch_activity_comments(access_token, activity_id):
    url = f"{STRAVA_API_URL}/activities/{activity_id}/comments"
    headers = {"Authorization": f"Bearer {access_token}"}
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    return resp.json()


def fetch_activity_laps(access_token, activity_id):
    url = f"https://www.strava.com/api/v3/activities/{activity_id}/laps"
    headers = {"Authorization": f"Bearer {access_token}"}
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    return resp.json()


def fetch_activity_streams(access_token, activity_id, keys=None):
    url = f"https://www.strava.com/api/v3/activities/{activity_id}/streams"
    headers = {"Authorization": f"Bearer {access_token}"}
    params = {"keys": keys or "latlng,time,heartrate,velocity_smooth,cadence,altitude"}
    resp = requests.get(url, headers=headers, params=params)
    resp.raise_for_status()
    return resp.json()


def main():
    settings = get_settings()
    print(settings)
    access_token = get_strava_credentials()
    today = datetime.today().strftime("%Y_%m_%d_")
    data_dir = os.path.join(os.path.dirname(__file__), "..", "data")
    os.makedirs(data_dir, exist_ok=True)

    # 1. Fetch activities
    days = settings.get("strava").get("activities_days")
    print("Fetching activities...")
    activities = fetch_activities(access_token, days)
    df_activities = pd.DataFrame(activities)
    dump_nested_csv(
        df_activities, os.path.join(data_dir, f"{today}strava_activities.csv")
    )
    print(f"{len(activities)} activities fetched and saved.")

    # 2. Fetch kudos & comments for N activities
    max_kudos = settings.get("strava", {}).get("kudos_max_activities", 10)
    max_comments = settings.get("strava", {}).get("comments_max_activities", 10)
    max_laps = settings.get("strava", {}).get("laps_max_activities", 10)
    max_streams = settings.get("strava", {}).get("streams_max_activities", 10)

    all_kudos = []
    all_comments = []
    all_laps = []
    all_streams = []
    for i, activity in df_activities.head(max(max_kudos, max_comments)).iterrows():
        activity_id = activity.get("id")
        if not activity_id:
            continue
        if i < max_kudos:
            kudos = fetch_activity_kudos(access_token, activity_id)
            for kudo in kudos:
                kudo["activity_id"] = activity_id
            all_kudos.extend(kudos)
        if i < max_comments:
            comments = fetch_activity_comments(access_token, activity_id)
            for comment in comments:
                comment["activity_id"] = activity_id
            all_comments.extend(comments)
        if i < max_laps:
            laps = fetch_activity_laps(access_token, activity_id)
            for lap in laps:
                lap["activity_id"] = activity_id
            all_laps.extend(laps)
        if i < max_streams:
            streams = fetch_activity_streams(access_token, activity_id)
            for stream in streams:
                stream["activity_id"] = activity_id
            all_streams.extend(streams)

    dump_nested_csv(
        pd.DataFrame(all_kudos), os.path.join(data_dir, f"{today}strava_kudos.csv")
    )
    dump_nested_csv(
        pd.DataFrame(all_comments),
        os.path.join(data_dir, f"{today}strava_comments.csv"),
    )
    dump_nested_csv(
        pd.DataFrame(all_laps), os.path.join(data_dir, f"{today}strava_laps.csv")
    )
    dump_nested_csv(
        pd.DataFrame(all_streams), os.path.join(data_dir, f"{today}strava_streams.csv")
    )
    print("Kudos and comments fetched and saved.")


if __name__ == "__main__":
    main()
