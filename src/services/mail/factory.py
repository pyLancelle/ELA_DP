"""
Factory Pattern pour les services d'email
Permet de créer dynamiquement le bon service selon le périmètre demandé
"""

from typing import Union
from .core import EmailConfig, BigQueryConfig
from .services import SpotifyEmailService
from .services.garmin_service import GarminEmailService
from .services.strava_service import StravaEmailService


class EmailServiceFactory:
    """Factory pour créer les services d'email selon le périmètre"""

    # Mapping des périmètres vers les classes de service
    _SERVICES = {
        "spotify_weekly": SpotifyEmailService,
        "garmin_weekly": GarminEmailService,
        "strava_weekly": StravaEmailService,
    }

    @classmethod
    def create_service(
        cls,
        perimeter: str,
        email_config: EmailConfig,
        bigquery_config: BigQueryConfig,
        environment: str = "prd",
    ) -> Union[SpotifyEmailService]:
        """
        Crée le service d'email approprié selon le périmètre

        Args:
            perimeter: Type de service (spotify_weekly, garmin_weekly, etc.)
            email_config: Configuration SMTP
            bigquery_config: Configuration BigQuery
            environment: Environnement (dev/prd)

        Returns:
            Instance du service email correspondant

        Raises:
            ValueError: Si le périmètre n'est pas supporté
        """
        if perimeter not in cls._SERVICES:
            available = ", ".join(cls._SERVICES.keys())
            raise ValueError(
                f"Périmètre '{perimeter}' non supporté. "
                f"Périmètres disponibles: {available}"
            )

        service_class = cls._SERVICES[perimeter]

        # Création du service selon le type
        if perimeter == "spotify_weekly":
            return service_class(
                email_config=email_config,
                bigquery_config=bigquery_config,
                to_email="",  # Sera écrasé par send_email_to()
            )

        elif perimeter in ["garmin_weekly", "strava_weekly"]:
            # Services avec support d'environnement
            return service_class(
                email_config=email_config,
                bigquery_config=bigquery_config,
                environment=environment,
            )

        else:
            # Fallback générique
            return service_class(
                email_config=email_config, bigquery_config=bigquery_config, to_email=""
            )

    @classmethod
    def get_available_perimeters(cls) -> list[str]:
        """Retourne la liste des périmètres disponibles"""
        return list(cls._SERVICES.keys())

    @classmethod
    def register_service(cls, perimeter: str, service_class):
        """
        Enregistre un nouveau service (pour extensibilité)

        Args:
            perimeter: Nom du périmètre
            service_class: Classe du service
        """
        cls._SERVICES[perimeter] = service_class
