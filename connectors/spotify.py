import os
import json
from datetime import datetime
from dotenv import load_dotenv
import spotipy
from spotipy.oauth2 import SpotifyOAuth

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
timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
output_file = f"data/spotify_recently_played_raw_{timestamp_str}.json"

with open(output_file, "w", encoding="utf-8") as f:
    json.dump(results, f, ensure_ascii=False, indent=2)

print(f"Dump brut enregistré dans : {output_file}")
