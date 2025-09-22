from datetime import datetime, timedelta
from typing import List, Dict
import pandas as pd

from ..core import (
    EmailService,
    EmailConfig,
    BigQueryConfig,
    GarminData,
    GarminActivity,
    GarminStats,
)
from ..providers import BigQueryProvider
from ..templates.garmin_template import GarminEmailTemplate


class GarminEmailService:
    """Service spécialisé pour les emails Garmin"""

    # Requêtes BigQuery pour les activités Garmin
    ACTIVITIES_QUERY = """
    SELECT
        activity_name,
        activity_type_display_name as activity_type,
        FORMAT_DATE('%d/%m/%Y', date) as date,
        distance_km as distance,
        CONCAT(
            CAST(EXTRACT(HOUR FROM duration) AS STRING), 'h',
            LPAD(CAST(EXTRACT(MINUTE FROM duration) AS STRING), 2, '0')
        ) as duration,
        calories,
        avg_heart_rate
    FROM `polar-scene-465223-f7.dp_lake_dev.lake_garmin__activities`
    WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
        AND date < CURRENT_DATE()
        AND distance_km > 0  -- Exclure les activités sans distance
    ORDER BY date DESC, start_time_local DESC
    LIMIT 10
    """

    STATS_QUERY = """
    SELECT
        COUNT(*) as total_activities,
        ROUND(SUM(distance_km), 1) as total_distance,
        CONCAT(
            CAST(SUM(EXTRACT(HOUR FROM duration)) AS STRING), 'h',
            LPAD(CAST(SUM(EXTRACT(MINUTE FROM duration)) AS STRING), 2, '0')
        ) as total_duration,
        SUM(calories) as total_calories,
        ROUND(AVG(avg_heart_rate)) as avg_heart_rate
    FROM `polar-scene-465223-f7.dp_lake_dev.lake_garmin__activities`
    WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
        AND date < CURRENT_DATE()
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

    def _convert_activities_dataframe(self, df: pd.DataFrame) -> List[GarminActivity]:
        """Convertit un DataFrame d'activités en liste de GarminActivity"""
        activities = []
        for _, row in df.iterrows():
            activities.append(
                GarminActivity(
                    activity_name=row["activity_name"],
                    activity_type=row["activity_type"],
                    date=row["date"],
                    distance=row.get("distance"),
                    duration=row["duration"],
                    calories=row.get("calories"),
                    avg_heart_rate=row.get("avg_heart_rate"),
                )
            )
        return activities

    def _convert_stats_dataframe(self, df: pd.DataFrame) -> GarminStats:
        """Convertit un DataFrame de stats en GarminStats"""
        if len(df) == 0:
            return GarminStats(
                total_activities=0,
                total_distance=0.0,
                total_duration="0h00",
                total_calories=0,
                avg_heart_rate=None,
            )

        row = df.iloc[0]
        return GarminStats(
            total_activities=int(row["total_activities"]),
            total_distance=float(row["total_distance"]),
            total_duration=row["total_duration"],
            total_calories=int(row["total_calories"]),
            avg_heart_rate=(
                int(row["avg_heart_rate"]) if pd.notna(row["avg_heart_rate"]) else None
            ),
        )

    def _get_fallback_data(self) -> GarminData:
        """Retourne des données de test en cas d'échec BigQuery"""
        print("📊 Utilisation des données de test Garmin...")

        activities = [
            GarminActivity(
                activity_name="Course matinale",
                activity_type="running",
                date="21/09/2024",
                distance=5.2,
                duration="0h28",
                calories=287,
                avg_heart_rate=165,
            ),
            GarminActivity(
                activity_name="Sortie vélo",
                activity_type="cycling",
                date="19/09/2024",
                distance=25.8,
                duration="1h15",
                calories=456,
                avg_heart_rate=145,
            ),
            GarminActivity(
                activity_name="Marche active",
                activity_type="walking",
                date="18/09/2024",
                distance=3.1,
                duration="0h45",
                calories=156,
                avg_heart_rate=120,
            ),
        ]

        stats = GarminStats(
            total_activities=3,
            total_distance=34.1,
            total_duration="2h28",
            total_calories=899,
            avg_heart_rate=143,
        )

        return GarminData(activities=activities, stats=stats)

    def get_garmin_data(self) -> GarminData:
        """Récupère les données Garmin depuis BigQuery"""
        try:
            print("🔍 Exécution des requêtes BigQuery Garmin...")

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

            print(f"📊 Trouvé: {len(activities_df)} activités Garmin")

            # Convertir en modèles Pydantic
            activities = self._convert_activities_dataframe(activities_df)
            stats = self._convert_stats_dataframe(stats_df)

            return GarminData(activities=activities, stats=stats)

        except Exception as e:
            print(f"⚠️  Erreur BigQuery Garmin: {e}")
            return self._get_fallback_data()

    def send_email_to(self, recipient: str) -> bool:
        """Envoie l'email Garmin à un destinataire spécifique"""
        try:
            # 1. Récupérer les données
            print("📊 Récupération des données Garmin...")
            garmin_data = self.get_garmin_data()

            # 2. Générer le contenu
            print("🎨 Génération de l'email Garmin...")
            week_num = datetime.now().isocalendar()[1]
            subject = f"⚡ Ton Résumé Garmin - Semaine {week_num}"

            html_content = GarminEmailTemplate.generate_html_email(garmin_data)
            text_content = GarminEmailTemplate.generate_text_email(garmin_data)

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
                print(f"⚡ Total activités: {garmin_data.stats.total_activities}")
                print(f"🏃‍♂️ Distance totale: {garmin_data.stats.total_distance}km")

            return success

        except Exception as e:
            print(f"❌ Erreur lors de l'envoi Garmin: {e}")
            return False

    def test_services(self) -> Dict[str, bool]:
        """Test tous les services"""
        results = {}

        print("🧪 Test de la connexion BigQuery...")
        results["bigquery"] = self.bigquery_provider.test_connection()

        print("🧪 Test de la connexion SMTP...")
        results["smtp"] = self.email_service.test_connection()

        return results
