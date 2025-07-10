#!/usr/bin/env python3
import os
from datetime import datetime
from dotenv import load_dotenv
import spotipy
from spotipy.oauth2 import SpotifyOAuth

from utils import to_jsonl  # ta fonction d’export JSONL

# ─── 1. Chargement des creds & du refresh_token ────────────────────────────────
dotenv_path = os.path.join(os.path.dirname(__file__), "..", ".env")
load_dotenv(dotenv_path)

CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID")
CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")
REDIRECT_URI = os.getenv("SPOTIFY_REDIRECT_URI")
REFRESH_TOKEN = os.getenv("SPOTIFY_REFRESH_TOKEN")

if not all([CLIENT_ID, CLIENT_SECRET, REDIRECT_URI, REFRESH_TOKEN]):
    raise Exception(
        "Il manque SPOTIFY_CLIENT_ID/SECRET/REDIRECT_URI/REFRESH_TOKEN dans le .env"
    )

# ─── 2. Prépa de l’OAuth sans prompt interactif ─────────────────────────────────
cache_path = os.path.join(os.path.dirname(__file__), "..", ".spotify-cache")

auth_manager = SpotifyOAuth(
    client_id=CLIENT_ID,
    client_secret=CLIENT_SECRET,
    redirect_uri=REDIRECT_URI,
    scope="user-read-recently-played",
    cache_path=cache_path,
)

# on “injecte” direct le refresh_token pour bypass le prompt
auth_manager.cache_handler.save_token_to_cache({"refresh_token": REFRESH_TOKEN})

# on rafraîchit l’access_token en silent mode
token_info = auth_manager.refresh_access_token(REFRESH_TOKEN)
access_token = token_info["access_token"]

# ready to rock
sp = spotipy.Spotify(auth=access_token)
print("✅ Authentifié via refresh_token, let’s go !")

# ─── 3. Récupération des pistes récentes & dump ────────────────────────────────
results = sp.current_user_recently_played(limit=50)

timestamp = datetime.now().strftime("%Y_%m_%d_%H_%M")
output_file = f"{timestamp}_spotify_recently_played_raw.jsonl"

to_jsonl(
    results,
    jsonl_output_path=output_file,
    key="items",
)

print(f"📁 Dump brut enregistré dans : {output_file}")

with open("latest_spotify_dump.txt", "w") as f:
    f.write(output_file)
