from datetime import datetime
from typing import List, Dict, Any
import pandas as pd

from ..core import (
    EmailService,
    EmailConfig,
    BigQueryConfig,
    SpotifyData,
    SpotifyArtist,
    SpotifyTrack,
)
from ..providers import BigQueryProvider
from ..templates import SpotifyEmailTemplate


class SpotifyEmailService:
    """Service spécialisé pour les emails Spotify"""

    # Requêtes BigQuery
    ARTISTS_QUERY = """
    SELECT rank, artist_name, time_listened, total_plays, artist_url, artist_image_url
    FROM `polar-scene-465223-f7.dp_product_dev.pct_emailing__spotify_top10_artists`
    ORDER BY rank
    LIMIT 10
    """

    TRACKS_QUERY = """
    SELECT rank, artist_name, track_name, time_listened, total_plays, track_url, album_cover_url
    FROM `polar-scene-465223-f7.dp_product_dev.pct_emailing__spotify_top10_tracks`
    ORDER BY rank
    LIMIT 10
    """

    def __init__(
        self, email_config: EmailConfig, bigquery_config: BigQueryConfig, to_email: str
    ):
        self.email_service = EmailService(email_config)
        self.bigquery_provider = BigQueryProvider(bigquery_config)
        self.to_email = to_email

    def _extract_spotify_id(self, url: str) -> str:
        """Extrait l'ID Spotify depuis une URL"""
        if not url:
            return "unknown"
        return url.split("/")[-1]

    def _convert_artists_dataframe(self, df: pd.DataFrame) -> List[SpotifyArtist]:
        """Convertit un DataFrame d'artistes en liste de SpotifyArtist"""
        artists = []
        for _, row in df.iterrows():
            artist_id = self._extract_spotify_id(row.get("artist_url", ""))
            artists.append(
                SpotifyArtist(
                    name=row["artist_name"],
                    time_display=row["time_listened"],
                    play_count=row.get("total_plays", 0),
                    artist_id=artist_id,
                    artist_url=row.get("artist_url", "#"),
                    image_url=row.get("artist_image_url"),
                )
            )
        return artists

    def _convert_tracks_dataframe(self, df: pd.DataFrame) -> List[SpotifyTrack]:
        """Convertit un DataFrame de titres en liste de SpotifyTrack"""
        tracks = []
        for _, row in df.iterrows():
            track_id = self._extract_spotify_id(row.get("track_url", ""))
            tracks.append(
                SpotifyTrack(
                    artist_name=row["artist_name"],
                    track_name=row["track_name"],
                    play_display=row["time_listened"],
                    play_count=row.get("total_plays", 0),
                    track_id=track_id,
                    track_url=row.get("track_url", "#"),
                    image_url=row.get("album_cover_url"),
                )
            )
        return tracks

    def _get_fallback_data(self) -> SpotifyData:
        """Retourne des données de test en cas d'échec BigQuery"""
        print("📊 Utilisation des données de test...")

        artists = [
            SpotifyArtist(
                name="Justice",
                time_display="3h27",
                play_count=45,
                artist_id="1gBUSTmm0PfPasR7RyYTNI",
                artist_url="https://open.spotify.com/artist/1gBUSTmm0PfPasR7RyYTNI",
                image_url="https://i.scdn.co/image/ab6761610000e5eb0c68f6c95232e716e2a2c394",
            ),
            SpotifyArtist(
                name="twenty one pilots",
                time_display="2h11",
                play_count=38,
                artist_id="3YQKmKGau1PzlVlkL5iodx",
                artist_url="https://open.spotify.com/artist/3YQKmKGau1PzlVlkL5iodx",
                image_url="https://i.scdn.co/image/ab6761610000e5eb196972172c37d934d9ca32d6",
            ),
            SpotifyArtist(
                name="Daft Punk",
                time_display="2h06",
                play_count=35,
                artist_id="4tZwfgrHOc3mvqYlEYSvVi",
                artist_url="https://open.spotify.com/artist/4tZwfgrHOc3mvqYlEYSvVi",
                image_url="https://i.scdn.co/image/ab6761610000e5ebae07171f989e2cb3b0662487",
            ),
            SpotifyArtist(
                name="Dire Straits",
                time_display="2h03",
                play_count=32,
                artist_id="0WwSkZ7LtFUFjGjMZBMt6T",
                artist_url="https://open.spotify.com/artist/0WwSkZ7LtFUFjGjMZBMt6T",
                image_url="https://i.scdn.co/image/ab6761610000e5eb941445b7d9c4ca7c2ce77832",
            ),
            SpotifyArtist(
                name="Dua Lipa",
                time_display="1h43",
                play_count=28,
                artist_id="6M2wZ9GZgrQXHCFfjv46we",
                artist_url="https://open.spotify.com/artist/6M2wZ9GZgrQXHCFfjv46we",
                image_url="https://i.scdn.co/image/ab6761610000e5eb5a00969a4698c3132a15fbb0",
            ),
        ]

        tracks = [
            SpotifyTrack(
                artist_name="Justice",
                track_name="D.A.N.C.E.",
                play_display="15x",
                play_count=15,
                track_id="5W3cjX2J3tjhG8zb6u0qHn",
                track_url="https://open.spotify.com/track/5W3cjX2J3tjhG8zb6u0qHn",
                image_url="https://i.scdn.co/image/ab67616d0000b273e0b64c0840ddeef14d44a1c9",
            ),
            SpotifyTrack(
                artist_name="Daft Punk",
                track_name="One More Time",
                play_display="13x",
                play_count=13,
                track_id="0DiWol3AO6WpXZgp0goxAV",
                track_url="https://open.spotify.com/track/0DiWol3AO6WpXZgp0goxAV",
                image_url="https://i.scdn.co/image/ab67616d0000b273195710000825d1fb8a5c8bca",
            ),
            SpotifyTrack(
                artist_name="twenty one pilots",
                track_name="Heathens",
                play_display="12x",
                play_count=12,
                track_id="6I1kxjDaEiMBbdUUhBjlTe",
                track_url="https://open.spotify.com/track/6I1kxjDaEiMBbdUUhBjlTe",
                image_url="https://i.scdn.co/image/ab67616d0000b273cdb645498cd3d8a2db4d05e1",
            ),
            SpotifyTrack(
                artist_name="The Weeknd",
                track_name="Blinding Lights",
                play_display="11x",
                play_count=11,
                track_id="0VjIjW4GlUZAMYd2vXMi3b",
                track_url="https://open.spotify.com/track/0VjIjW4GlUZAMYd2vXMi3b",
                image_url="https://i.scdn.co/image/ab67616d0000b2738863bc11d2aa12b54f5aeb36",
            ),
            SpotifyTrack(
                artist_name="Dire Straits",
                track_name="Sultans of Swing",
                play_display="10x",
                play_count=10,
                track_id="09YKLmVHUjFB5ZUjOsZiLh",
                track_url="https://open.spotify.com/track/09YKLmVHUjFB5ZUjOsZiLh",
                image_url="https://i.scdn.co/image/ab67616d0000b273a91c10fe9472d9bd89802e5a",
            ),
        ]

        return SpotifyData(artists=artists, tracks=tracks)

    def get_spotify_data(self) -> SpotifyData:
        """Récupère les données Spotify depuis BigQuery"""
        try:
            print("🔍 Exécution des requêtes BigQuery...")

            # Exécuter les requêtes
            artists_df = self.bigquery_provider.execute_query(self.ARTISTS_QUERY)
            tracks_df = self.bigquery_provider.execute_query(self.TRACKS_QUERY)

            print(f"📊 Trouvé: {len(artists_df)} artistes, {len(tracks_df)} titres")

            if len(artists_df) == 0 or len(tracks_df) == 0:
                raise Exception("Aucune donnée trouvée")

            # Convertir en modèles Pydantic
            artists = self._convert_artists_dataframe(artists_df)
            tracks = self._convert_tracks_dataframe(tracks_df)

            return SpotifyData(artists=artists, tracks=tracks)

        except Exception as e:
            print(f"⚠️  Erreur BigQuery: {e}")
            return self._get_fallback_data()

    def send_weekly_email(self) -> bool:
        """Envoie l'email hebdomadaire Spotify (méthode legacy)"""
        return self.send_email_to(self.to_email)

    def send_email_to(self, recipient: str) -> bool:
        """Envoie l'email Spotify à un destinataire spécifique"""
        try:
            # 1. Récupérer les données
            print("📊 Récupération des données Spotify...")
            spotify_data = self.get_spotify_data()

            # 2. Générer le contenu
            print("🎨 Génération de l'email mobile-optimisé...")
            current_week = datetime.now().isocalendar()[1]
            previous_week = current_week - 1 if current_week > 1 else 52
            subject = f"🎶 SPOTIFY WEEKLY OVERVIEW - W{previous_week:02d} 🎶"

            html_content = SpotifyEmailTemplate.generate_html_email(spotify_data)
            text_content = SpotifyEmailTemplate.generate_text_email(spotify_data)

            # 3. Créer et envoyer l'email
            email_content = self.email_service.create_email_content(
                to_email=recipient,
                subject=subject,
                text_content=text_content,
                html_content=html_content,
            )

            print("📧 Envoi en cours...")
            success = self.email_service.send_email(email_content)

            if success:
                print(f"🥇 Top artiste: {spotify_data.artists[0].name}")
                print(
                    f"🎵 Top titre: {spotify_data.tracks[0].artist_name} - {spotify_data.tracks[0].track_name}"
                )

            return success

        except Exception as e:
            print(f"❌ Erreur lors de l'envoi: {e}")
            return False

    def test_services(self) -> Dict[str, bool]:
        """Test tous les services"""
        results = {}

        print("🧪 Test de la connexion BigQuery...")
        results["bigquery"] = self.bigquery_provider.test_connection()

        print("🧪 Test de la connexion SMTP...")
        results["smtp"] = self.email_service.test_connection()

        return results
