import os
import json
from datetime import datetime
from dotenv import load_dotenv
import spotipy
from spotipy.oauth2 import SpotifyOAuth

from utils import to_jsonl

# Charger les variables d'environnement
dotenv_path = os.path.join(os.path.dirname(__file__), "..", ".env")

load_dotenv(dotenv_path)
CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID")
CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")
REDIRECT_URI = os.getenv("SPOTIFY_REDIRECT_URI")

if not CLIENT_ID or not CLIENT_SECRET or not REDIRECT_URI:
    raise Exception(
        "Vérifie que SPOTIPY_CLIENT_ID, SPOTIPY_CLIENT_SECRET et SPOTIPY_REDIRECT_URI sont bien définis dans le .env"
    )

# Initialiser l'authentification avec le scope requis
sp = spotipy.Spotify(
    auth_manager=SpotifyOAuth(
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        redirect_uri=REDIRECT_URI,
        scope="user-read-recently-played",
    )
)

# Récupérer les pistes récentes (limite 50 par défaut, max 50)
results = sp.current_user_recently_played(limit=50)

# Dump JSON BRUT
timestamp_str = datetime.now().strftime("%Y_%m_%d_%H_%M")
output_file = f"data/{timestamp_str}_spotify_recently_played_raw.jsonl"

to_jsonl(
    results,
    jsonl_output_path=output_file,
    key="items",
)

print(f"Dump brut enregistré dans : {output_file}")
with open("data/latest_spotify_dump.txt", "w") as f:
    f.write(
        f"data/{timestamp_str}_spotify_recently_played_raw.jsonl"
    )  # mon_fichier_de_sortie = nom réel du fichier (avec chemin relatif)
