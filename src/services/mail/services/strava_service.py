from datetime import datetime, timedelta
from typing import List, Dict
import pandas as pd

from ..core import (
    EmailService,
    EmailConfig,
    BigQueryConfig,
    StravaData,
    StravaActivity,
    StravaStats,
)
from ..providers import BigQueryProvider
from ..templates.strava_template import StravaEmailTemplate


class StravaEmailService:
    """Service spécialisé pour les emails Strava"""

    # Requêtes BigQuery pour les activités Strava (à adapter selon vos tables)
    ACTIVITIES_QUERY = """
    SELECT
        name,
        type,
        FORMAT_DATE('%d/%m/%Y', start_date) as date,
        distance_km as distance,
        CONCAT(
            CAST(EXTRACT(HOUR FROM moving_time) AS STRING), 'h',
            LPAD(CAST(EXTRACT(MINUTE FROM moving_time) AS STRING), 2, '0')
        ) as moving_time,
        total_elevation_gain as elevation_gain,
        average_speed_kmh as average_speed
    FROM `polar-scene-465223-f7.dp_lake_dev.lake_strava__activities`
    WHERE start_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
        AND start_date < CURRENT_DATE()
        AND distance_km > 0
    ORDER BY start_date DESC
    LIMIT 10
    """

    STATS_QUERY = """
    SELECT
        COUNT(*) as total_activities,
        ROUND(SUM(distance_km), 1) as total_distance,
        CONCAT(
            CAST(SUM(EXTRACT(HOUR FROM moving_time)) AS STRING), 'h',
            LPAD(CAST(SUM(EXTRACT(MINUTE FROM moving_time)) AS STRING), 2, '0')
        ) as total_time,
        ROUND(SUM(total_elevation_gain)) as total_elevation
    FROM `polar-scene-465223-f7.dp_lake_dev.lake_strava__activities`
    WHERE start_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
        AND start_date < CURRENT_DATE()
        AND distance_km > 0
    """

    def __init__(
        self,
        email_config: EmailConfig,
        bigquery_config: BigQueryConfig,
        environment: str = "prd",
    ):
        self.email_service = EmailService(email_config)
        self.bigquery_provider = BigQueryProvider(bigquery_config)
        self.environment = environment

    def _convert_activities_dataframe(self, df: pd.DataFrame) -> List[StravaActivity]:
        """Convertit un DataFrame d'activités en liste de StravaActivity"""
        activities = []
        for _, row in df.iterrows():
            activities.append(
                StravaActivity(
                    name=row["name"],
                    type=row["type"],
                    date=row["date"],
                    distance=row.get("distance"),
                    moving_time=row["moving_time"],
                    elevation_gain=row.get("elevation_gain"),
                    average_speed=row.get("average_speed"),
                )
            )
        return activities

    def _convert_stats_dataframe(self, df: pd.DataFrame) -> StravaStats:
        """Convertit un DataFrame de stats en StravaStats"""
        if len(df) == 0:
            return StravaStats(
                total_activities=0,
                total_distance=0.0,
                total_time="0h00",
                total_elevation=0.0,
            )

        row = df.iloc[0]
        return StravaStats(
            total_activities=int(row["total_activities"]),
            total_distance=float(row["total_distance"]),
            total_time=row["total_time"],
            total_elevation=float(row["total_elevation"]),
        )

    def _get_fallback_data(self) -> StravaData:
        """Retourne des données de test en cas d'échec BigQuery"""
        print("📊 Utilisation des données de test Strava...")

        activities = [
            StravaActivity(
                name="Course du dimanche",
                type="Run",
                date="22/09/2024",
                distance=8.5,
                moving_time="0h42",
                elevation_gain=89,
                average_speed=12.1,
            ),
            StravaActivity(
                name="Sortie vélo matinale",
                type="Ride",
                date="20/09/2024",
                distance=32.4,
                moving_time="1h28",
                elevation_gain=456,
                average_speed=22.1,
            ),
        ]

        stats = StravaStats(
            total_activities=2,
            total_distance=40.9,
            total_time="2h10",
            total_elevation=545.0,
        )

        return StravaData(activities=activities, stats=stats)

    def get_strava_data(self) -> StravaData:
        """Récupère les données Strava depuis BigQuery"""
        try:
            print("🔍 Exécution des requêtes BigQuery Strava...")

            # Adapter les requêtes selon l'environnement
            env_suffix = "dev" if self.environment == "dev" else "prd"
            activities_query = self.ACTIVITIES_QUERY.replace(
                "dp_lake_dev", f"dp_lake_{env_suffix}"
            )
            stats_query = self.STATS_QUERY.replace(
                "dp_lake_dev", f"dp_lake_{env_suffix}"
            )

            # Exécuter les requêtes
            activities_df = self.bigquery_provider.execute_query(activities_query)
            stats_df = self.bigquery_provider.execute_query(stats_query)

            print(f"📊 Trouvé: {len(activities_df)} activités Strava")

            # Convertir en modèles Pydantic
            activities = self._convert_activities_dataframe(activities_df)
            stats = self._convert_stats_dataframe(stats_df)

            return StravaData(activities=activities, stats=stats)

        except Exception as e:
            print(f"⚠️  Erreur BigQuery Strava: {e}")
            return self._get_fallback_data()

    def send_email_to(self, recipient: str) -> bool:
        """Envoie l'email Strava à un destinataire spécifique"""
        try:
            # 1. Récupérer les données
            print("📊 Récupération des données Strava...")
            strava_data = self.get_strava_data()

            # 2. Générer le contenu
            print("🎨 Génération de l'email Strava...")
            week_num = datetime.now().isocalendar()[1]
            subject = f"🔥 Ton Résumé Strava - Semaine {week_num}"

            html_content = StravaEmailTemplate.generate_html_email(strava_data)
            text_content = StravaEmailTemplate.generate_text_email(strava_data)

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
                print(f"🔥 Total activités: {strava_data.stats.total_activities}")
                print(f"🚴‍♂️ Distance totale: {strava_data.stats.total_distance}km")

            return success

        except Exception as e:
            print(f"❌ Erreur lors de l'envoi Strava: {e}")
            return False

    def test_services(self) -> Dict[str, bool]:
        """Test tous les services"""
        results = {}

        print("🧪 Test de la connexion BigQuery...")
        results["bigquery"] = self.bigquery_provider.test_connection()

        print("🧪 Test de la connexion SMTP...")
        results["smtp"] = self.email_service.test_connection()

        return results
