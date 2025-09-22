from pydantic import BaseModel, Field, ConfigDict
from typing import Optional, List
from datetime import datetime


class EmailConfig(BaseModel):
    """Configuration pour l'envoi d'emails"""

    model_config = ConfigDict(validate_assignment=True)

    smtp_server: str = Field(..., description="Serveur SMTP")
    smtp_port: int = Field(587, description="Port SMTP")
    email_from: str = Field(..., description="Adresse email expéditeur")
    email_password: str = Field(..., description="Mot de passe application")
    use_tls: bool = Field(True, description="Utiliser TLS")


class SpotifyArtist(BaseModel):
    """Modèle pour un artiste Spotify"""

    model_config = ConfigDict(validate_assignment=True)

    name: str = Field(..., description="Nom de l'artiste")
    time_display: str = Field(..., description="Temps d'écoute formaté")
    play_count: int = Field(0, description="Nombre de lectures")
    artist_id: str = Field(..., description="ID Spotify de l'artiste")
    artist_url: str = Field(..., description="URL Spotify de l'artiste")
    image_url: Optional[str] = Field(None, description="URL de l'image de l'artiste")


class SpotifyTrack(BaseModel):
    """Modèle pour un titre Spotify"""

    model_config = ConfigDict(validate_assignment=True)

    artist_name: str = Field(..., description="Nom de l'artiste")
    track_name: str = Field(..., description="Nom du titre")
    play_display: str = Field(..., description="Affichage du nombre de lectures")
    play_count: int = Field(0, description="Nombre de lectures")
    track_id: str = Field(..., description="ID Spotify du titre")
    track_url: str = Field(..., description="URL Spotify du titre")
    image_url: Optional[str] = Field(None, description="URL de l'image du titre")


class SpotifyData(BaseModel):
    """Données complètes Spotify"""

    model_config = ConfigDict(validate_assignment=True)

    artists: List[SpotifyArtist] = Field(..., description="Liste des artistes")
    tracks: List[SpotifyTrack] = Field(..., description="Liste des titres")


class EmailContent(BaseModel):
    """Contenu d'un email"""

    model_config = ConfigDict(validate_assignment=True)

    subject: str = Field(..., description="Sujet de l'email")
    text_content: str = Field(..., description="Contenu texte")
    html_content: str = Field(..., description="Contenu HTML")
    to_email: str = Field(..., description="Destinataire")


class BigQueryConfig(BaseModel):
    """Configuration BigQuery"""

    model_config = ConfigDict(validate_assignment=True)

    credentials_path: str = Field(
        ..., description="Chemin vers le fichier de credentials"
    )
    project_id: Optional[str] = Field(
        None, description="ID du projet (auto-détecté si None)"
    )


class GarminActivity(BaseModel):
    """Modèle pour une activité Garmin"""

    model_config = ConfigDict(validate_assignment=True)

    activity_name: str = Field(..., description="Nom de l'activité")
    activity_type: str = Field(..., description="Type d'activité (course, vélo, etc.)")
    date: str = Field(..., description="Date de l'activité")
    distance: Optional[float] = Field(None, description="Distance en km")
    duration: str = Field(..., description="Durée formatée")
    calories: Optional[int] = Field(None, description="Calories brûlées")
    avg_heart_rate: Optional[int] = Field(
        None, description="Fréquence cardiaque moyenne"
    )


class GarminStats(BaseModel):
    """Modèle pour les statistiques Garmin de la semaine"""

    model_config = ConfigDict(validate_assignment=True)

    total_activities: int = Field(..., description="Nombre total d'activités")
    total_distance: float = Field(..., description="Distance totale en km")
    total_duration: str = Field(..., description="Durée totale formatée")
    total_calories: int = Field(..., description="Calories totales")
    avg_heart_rate: Optional[int] = Field(None, description="FC moyenne sur la période")


class GarminData(BaseModel):
    """Données complètes Garmin"""

    model_config = ConfigDict(validate_assignment=True)

    activities: List[GarminActivity] = Field(..., description="Liste des activités")
    stats: GarminStats = Field(..., description="Statistiques de la période")


class StravaActivity(BaseModel):
    """Modèle pour une activité Strava"""

    model_config = ConfigDict(validate_assignment=True)

    name: str = Field(..., description="Nom de l'activité")
    type: str = Field(..., description="Type d'activité")
    date: str = Field(..., description="Date de l'activité")
    distance: Optional[float] = Field(None, description="Distance en km")
    moving_time: str = Field(..., description="Temps de mouvement")
    elevation_gain: Optional[float] = Field(None, description="Dénivelé en m")
    average_speed: Optional[float] = Field(None, description="Vitesse moyenne km/h")


class StravaStats(BaseModel):
    """Modèle pour les statistiques Strava de la semaine"""

    model_config = ConfigDict(validate_assignment=True)

    total_activities: int = Field(..., description="Nombre total d'activités")
    total_distance: float = Field(..., description="Distance totale en km")
    total_time: str = Field(..., description="Temps total")
    total_elevation: float = Field(..., description="Dénivelé total en m")


class StravaData(BaseModel):
    """Données complètes Strava"""

    model_config = ConfigDict(validate_assignment=True)

    activities: List[StravaActivity] = Field(..., description="Liste des activités")
    stats: StravaStats = Field(..., description="Statistiques de la période")
