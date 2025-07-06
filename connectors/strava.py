"""
Strava data extraction utility.
Fetches all activities, comments, and kudos, and exports them as CSV files.
PEP8 compliant.
"""

import os
import requests
import pandas as pd
from dotenv import load_dotenv
import json
from datetime import datetime

STRAVA_API_URL = "https://www.strava.com/api/v3"


def refresh_access_token(client_id, client_secret, refresh_token):
    """Refresh the Strava access token using the refresh token and update .env."""
    url = "https://www.strava.com/oauth/token"
    data = {
        "client_id": client_id,
        "client_secret": client_secret,
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
    }
    resp = requests.post(url, data=data, timeout=20)
    try:
        resp.raise_for_status()
    except requests.exceptions.HTTPError:
        print("Failed to refresh token. Response from Strava:")
        print(f"Status code: {resp.status_code}")
        print(f"Response body: {resp.text}")
        raise
    response = resp.json()
    access_token = response["access_token"]
    new_refresh_token = response.get("refresh_token", refresh_token)
    # Update .env file with both tokens
    update_env_file(access_token, new_refresh_token)
    print(
        f"Tokens updated in .env: access_token={access_token[:8]}..., refresh_token={new_refresh_token[:8]}..."
    )
    return access_token, new_refresh_token


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


def get_strava_credentials():
    """Load Strava API credentials from environment or .env file."""
    load_dotenv()
    client_id = os.getenv("STRAVA_CLIENT_ID")
    client_secret = os.getenv("STRAVA_CLIENT_SECRET")
    access_token = os.getenv("STRAVA_ACCESS_TOKEN")
    refresh_token = os.getenv("STRAVA_REFRESH_TOKEN")
    missing = [
        k
        for k, v in {
            "STRAVA_CLIENT_ID": client_id,
            "STRAVA_CLIENT_SECRET": client_secret,
            "STRAVA_ACCESS_TOKEN": access_token,
            "STRAVA_REFRESH_TOKEN": refresh_token,
        }.items()
        if not v
    ]
    if missing:
        raise RuntimeError(
            f"Missing Strava credentials in environment or .env file: {', '.join(missing)}"
        )
    return client_id, client_secret, access_token, refresh_token


def fetch_activities(access_token, per_page=200, max_pages=10):
    url = f"{STRAVA_API_URL}/athlete/activities"
    headers = {"Authorization": f"Bearer {access_token}"}
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


def fetch_activity_comments(access_token, activity_id):
    url = f"{STRAVA_API_URL}/activities/{activity_id}/comments"
    headers = {"Authorization": f"Bearer {access_token}"}
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    return resp.json()


def fetch_activity_kudos(access_token, activity_id):
    url = f"{STRAVA_API_URL}/activities/{activity_id}/kudos"
    headers = {"Authorization": f"Bearer {access_token}"}
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    return resp.json()


import json
from datetime import datetime


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


def update_env_file(new_access_token, new_refresh_token):
    """Update the .env file with new access and refresh tokens."""
    env_path = os.path.join(os.path.dirname(__file__), "..", ".env")
    lines = []
    with open(env_path, "r") as f:
        for line in f:
            if line.startswith("STRAVA_ACCESS_TOKEN="):
                lines.append(f"STRAVA_ACCESS_TOKEN={new_access_token}\n")
            elif line.startswith("STRAVA_REFRESH_TOKEN="):
                lines.append(f"STRAVA_REFRESH_TOKEN={new_refresh_token}\n")
            else:
                lines.append(line)
    with open(env_path, "w") as f:
        f.writelines(lines)


def main():
    client_id, client_secret, access_token, refresh_token = get_strava_credentials()
    today = datetime.today().strftime("%Y_%m_%d_")
    data_dir = os.path.join(os.path.dirname(__file__), "..", "data")
    os.makedirs(data_dir, exist_ok=True)

    print("Fetching activities...")
    try:
        activities = fetch_activities(access_token)
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 401:
            print("Access token expired or invalid, refreshing token...")
            access_token, refresh_token = refresh_access_token(
                client_id, client_secret, refresh_token
            )
            update_env_file(access_token, refresh_token)
            print(
                f"Token refreshed. Retrying fetch with access_token: {access_token[:8]}... (truncated)"
            )
            # Use the new access_token for all subsequent calls
            try:
                activities = fetch_activities(access_token)
            except requests.exceptions.HTTPError as e2:
                print("Token refresh failed or new token is also unauthorized.")
                raise
        else:
            raise
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
        comments = fetch_activity_comments(access_token, activity_id)
        for comment in comments:
            comment["activity_id"] = activity_id
        all_comments.extend(comments)
        kudos = fetch_activity_kudos(access_token, activity_id)
        for kudo in kudos:
            kudo["activity_id"] = activity_id
        all_kudos.extend(kudos)
    dump_nested_csv(
        pd.DataFrame(all_comments),
        os.path.join(data_dir, f"{today}strava_comments.csv"),
    )
    dump_nested_csv(
        pd.DataFrame(all_kudos),
        os.path.join(data_dir, f"{today}strava_kudos.csv"),
    )
    print("All Strava data fetched and saved to CSV files in the data folder.")


if __name__ == "__main__":
    main()
