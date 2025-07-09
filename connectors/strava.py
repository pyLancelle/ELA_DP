import os
import json
import time
from datetime import datetime, timedelta
from dotenv import load_dotenv
from stravalib import Client
from pathlib import Path

# Configuration
TOKEN_FILE = os.path.join(os.path.dirname(__file__), "strava_tokens.json")
DATA_DIR = Path(__file__).parent.parent / "data"
DAYS = 10  # Nombre de jours en arrière pour récupérer les activités

STREAM_TYPES = [
    "time",
    "distance",
    "latlng",
    "heartrate",
    "velocity_smooth",
    "cadence",
    "altitude",
    "temp",
    "moving",
    "grade_smooth",
    "watts",
]

REDIRECT_URI = os.getenv("STRAVA_REDIRECT_URI", "http://localhost")
SCOPES = ["activity:read_all", "profile:read_all"]


def init_client():
    load_dotenv()
    client = Client()
    cid = os.getenv("STRAVA_CLIENT_ID")
    csec = os.getenv("STRAVA_CLIENT_SECRET")
    env_ref = os.getenv("STRAVA_REFRESH_TOKEN")
    if not os.path.exists(TOKEN_FILE):
        url = client.authorization_url(
            client_id=cid, redirect_uri=REDIRECT_URI, scope=SCOPES
        )
        print("🔗 Ouvrez et autorisez :", url)
        code = input("Code de redirection : ").strip()
        toks = client.exchange_code_for_token(
            client_id=cid, client_secret=csec, code=code
        )
        with open(TOKEN_FILE, "w") as f:
            json.dump(toks, f)
    else:
        toks = json.load(open(TOKEN_FILE))
    now = int(time.time())
    if not toks.get("access_token") or toks.get("expires_at", 0) < now + 60:
        toks = client.refresh_access_token(
            client_id=cid,
            client_secret=csec,
            refresh_token=toks.get("refresh_token", env_ref),
        )
        with open(TOKEN_FILE, "w") as f:
            json.dump(toks, f)
    client.access_token = toks["access_token"]
    client.refresh_token = toks["refresh_token"]
    client.token_expires_at = toks["expires_at"]
    return client


def dump_jsonl(records, path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False, default=str) + "\n")
    print(f"✅ Dump : {path} ({len(records)} lignes)")


def main():
    client = init_client()
    since = datetime.utcnow() - timedelta(days=DAYS)
    print(f"📅 Récupération depuis {since.isoformat()}...")
    acts = list(client.get_activities(after=since))
    print(f"✅ {len(acts)} activités chargées.")

    prefix = datetime.utcnow().strftime("%Y_%m_%d_")
    os.makedirs(DATA_DIR, exist_ok=True)

    # Activités brutes
    dump_jsonl(
        [a.model_dump() for a in acts],
        os.path.join(DATA_DIR, f"{prefix}strava_activities.jsonl"),
    )

    # Kudos, Comments, Laps, Streams
    kudos, comments, laps, streams = [], [], [], []
    for idx, a in enumerate(acts):
        try:
            print(f"⏳ Activité {a.id} ({idx+1}/{len(acts)})")
            # Kudos
            try:
                kudos += [k.model_dump() for k in client.get_activity_kudos(a.id)]
            except Exception as e:
                print(f"⚠️ Kudos erreur pour {a.id} : {e}")
            # Comments
            try:
                comments += [c.model_dump() for c in client.get_activity_comments(a.id)]
            except Exception as e:
                print(f"⚠️ Comments erreur pour {a.id} : {e}")
            # Laps
            try:
                laps += [lap.model_dump() for lap in client.get_activity_laps(a.id)]
            except Exception as e:
                print(f"⚠️ Laps erreur pour {a.id} : {e}")
            # Streams
            try:
                sdict = client.get_activity_streams(a.id, types=STREAM_TYPES)
                streams.append(
                    {
                        "activity_id": a.id,
                        "streams": {k: v.model_dump() for k, v in sdict.items()},
                    }
                )
            except Exception as e:
                print(f"⚠️ Streams erreur pour {a.id} : {e}")
        except Exception as outer_e:
            print(f"❌ Activité {a.id} plantée : {outer_e}")

    dump_jsonl(kudos, os.path.join(DATA_DIR, f"{prefix}strava_kudos.jsonl"))
    dump_jsonl(comments, os.path.join(DATA_DIR, f"{prefix}strava_comments.jsonl"))
    dump_jsonl(laps, os.path.join(DATA_DIR, f"{prefix}strava_laps.jsonl"))
    dump_jsonl(streams, os.path.join(DATA_DIR, f"{prefix}strava_streams.jsonl"))

    # Optionnel : Dump tes gears (chaussures, vélo...) et clubs
    gears = []
    try:
        athlete = client.get_athlete()
        if hasattr(athlete, "bikes"):
            gears += [g.model_dump() for g in athlete.bikes]
        if hasattr(athlete, "shoes"):
            gears += [g.model_dump() for g in athlete.shoes]
        dump_jsonl(gears, os.path.join(DATA_DIR, f"{prefix}strava_gears.jsonl"))
    except Exception as e:
        print(f"⚠️ Gears non récupérés : {e}")

    clubs = []
    try:
        clubs = [c.model_dump() for c in client.get_athlete_clubs()]
        dump_jsonl(clubs, os.path.join(DATA_DIR, f"{prefix}strava_clubs.jsonl"))
    except Exception as e:
        print(f"⚠️ Clubs non récupérés : {e}")

    print("🎉 Dump terminé.")


if __name__ == "__main__":
    main()
