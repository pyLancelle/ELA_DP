from datetime import datetime
from typing import Dict
from ..core.models import StravaData, StravaActivity


class StravaEmailTemplate:
    """Template generator pour les emails Strava"""

    @staticmethod
    def get_activity_icon(activity_type: str) -> str:
        """Retourne l'icône pour le type d'activité Strava"""
        icons = {
            "run": "🏃‍♂️",
            "ride": "🚴‍♂️",
            "walk": "🚶‍♂️",
            "swim": "🏊‍♂️",
            "hike": "🥾",
            "workout": "💪",
        }
        return icons.get(activity_type.lower(), "⚡")

    @classmethod
    def generate_activity_row(cls, activity: StravaActivity, position: int) -> str:
        """Génère une ligne d'activité Strava"""
        icon = cls.get_activity_icon(activity.type)

        # Formatage des données
        distance_str = f"{activity.distance:.1f}km" if activity.distance else "N/A"
        elevation_str = (
            f"{activity.elevation_gain:.0f}m" if activity.elevation_gain else "N/A"
        )
        speed_str = (
            f"{activity.average_speed:.1f}km/h" if activity.average_speed else "N/A"
        )

        return f"""
        <tr>
            <td style="padding-bottom: 12px;">
                <table width="100%" cellpadding="0" cellspacing="0" border="0" style="background: #f9f9f9; border-left: 4px solid #fc4c02; border-radius: 8px;">
                    <tr>
                        <td style="padding: 16px 20px;">
                            <table width="100%" cellpadding="0" cellspacing="0" border="0">
                                <tr>
                                    <td width="40" style="font-size: 24px; vertical-align: top; padding-right: 12px;">{icon}</td>
                                    <td style="vertical-align: top;">
                                        <div style="font-weight: 600; font-size: 16px; color: #1a1a1a; margin-bottom: 4px;">{activity.name}</div>
                                        <div style="font-size: 14px; color: #64748b; margin-bottom: 8px;">{activity.date}</div>
                                        <div style="font-size: 13px; color: #64748b;">
                                            {distance_str} • {activity.moving_time} • {elevation_str} D+ • {speed_str}
                                        </div>
                                    </td>
                                </tr>
                            </table>
                        </td>
                    </tr>
                </table>
            </td>
        </tr>"""

    @classmethod
    def generate_html_email(cls, strava_data: StravaData) -> str:
        """Génère le HTML complet de l'email Strava"""
        week_num = datetime.now().isocalendar()[1]
        date_str = datetime.now().strftime("%d/%m/%Y")

        # Générer les lignes d'activités
        activity_rows = ""
        for i, activity in enumerate(strava_data.activities, 1):
            activity_rows += cls.generate_activity_row(activity, i)

        html_content = f"""<!DOCTYPE html>
<html lang="fr">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Strava Weekly Report - Week {week_num}</title>
</head>
<body style="margin: 0; padding: 0; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Helvetica, Arial, sans-serif; background-color: #f8fafc;">

    <table width="100%" cellpadding="0" cellspacing="0" border="0" style="background-color: #f8fafc;">
        <tr>
            <td align="center" style="padding: 20px 10px;">

                <table width="600" cellpadding="0" cellspacing="0" border="0" style="max-width: 600px; width: 100%; background: white; border-radius: 16px; box-shadow: 0 10px 30px rgba(0,0,0,0.1);">
                    <tr>
                        <td style="padding: 40px 30px;">

                            <!-- Header -->
                            <table width="100%" cellpadding="0" cellspacing="0" border="0">
                                <tr>
                                    <td align="center" style="padding-bottom: 40px;">
                                        <h1 style="margin: 0; font-size: 28px; font-weight: 700; color: #1a1a1a; letter-spacing: 0.5px; line-height: 1.2;">
                                            🔥 STRAVA - WEEK {week_num}
                                        </h1>
                                        <p style="margin: 15px 0 0 0; color: #64748b; font-size: 16px; line-height: 1.4;">
                                            Tes performances de la semaine
                                        </p>
                                    </td>
                                </tr>
                            </table>

                            <!-- Stats Section -->
                            <table width="100%" cellpadding="0" cellspacing="0" border="0" style="background: linear-gradient(135deg, #fc4c02, #ff6b35); border-radius: 16px; margin-bottom: 40px;">
                                <tr>
                                    <td style="padding: 30px 20px;">
                                        <table width="100%" cellpadding="0" cellspacing="0" border="0">
                                            <tr>
                                                <td align="center" style="color: white;">
                                                    <h2 style="margin: 0 0 20px 0; font-size: 22px; font-weight: 600;">
                                                        📊 Résumé de la Semaine
                                                    </h2>
                                                </td>
                                            </tr>
                                            <tr>
                                                <td>
                                                    <table width="100%" cellpadding="0" cellspacing="0" border="0">
                                                        <tr>
                                                            <td width="25%" align="center" style="color: white; padding: 10px;">
                                                                <div style="font-size: 24px; font-weight: 700; margin-bottom: 5px;">{strava_data.stats.total_activities}</div>
                                                                <div style="font-size: 13px; opacity: 0.9;">Activités</div>
                                                            </td>
                                                            <td width="25%" align="center" style="color: white; padding: 10px;">
                                                                <div style="font-size: 24px; font-weight: 700; margin-bottom: 5px;">{strava_data.stats.total_distance:.1f}km</div>
                                                                <div style="font-size: 13px; opacity: 0.9;">Distance</div>
                                                            </td>
                                                            <td width="25%" align="center" style="color: white; padding: 10px;">
                                                                <div style="font-size: 24px; font-weight: 700; margin-bottom: 5px;">{strava_data.stats.total_time}</div>
                                                                <div style="font-size: 13px; opacity: 0.9;">Temps</div>
                                                            </td>
                                                            <td width="25%" align="center" style="color: white; padding: 10px;">
                                                                <div style="font-size: 24px; font-weight: 700; margin-bottom: 5px;">{strava_data.stats.total_elevation:.0f}m</div>
                                                                <div style="font-size: 13px; opacity: 0.9;">D+</div>
                                                            </td>
                                                        </tr>
                                                    </table>
                                                </td>
                                            </tr>
                                        </table>
                                    </td>
                                </tr>
                            </table>

                            <!-- Activities Section -->
                            <table width="100%" cellpadding="0" cellspacing="0" border="0" style="margin-bottom: 40px;">
                                <tr>
                                    <td align="center" style="padding-bottom: 30px;">
                                        <h2 style="margin: 0; font-size: 22px; font-weight: 600; color: #374151;">
                                            🚴‍♂️ Tes Activités
                                        </h2>
                                    </td>
                                </tr>

                                {activity_rows}

                            </table>

                            <!-- Call to action -->
                            <table width="100%" cellpadding="0" cellspacing="0" border="0" style="background: linear-gradient(135deg, #fc4c02, #ff6b35); border-radius: 16px; margin-bottom: 40px;">
                                <tr>
                                    <td align="center" style="padding: 25px 20px;">
                                        <p style="margin: 0 0 15px 0; color: white; font-size: 18px; font-weight: 700; line-height: 1.2;">
                                            📱 Voir plus de détails
                                        </p>
                                        <a href="https://www.strava.com"
                                           style="display: inline-block; background: white; color: #fc4c02; padding: 12px 25px; border-radius: 25px; text-decoration: none; font-weight: 700; font-size: 16px; line-height: 1;">
                                            Strava →
                                        </a>
                                    </td>
                                </tr>
                            </table>

                            <!-- Footer -->
                            <table width="100%" cellpadding="0" cellspacing="0" border="0" style="border-top: 1px solid #e5e7eb;">
                                <tr>
                                    <td align="center" style="padding-top: 30px;">
                                        <p style="margin: 0; color: #6b7280; font-size: 14px; line-height: 1.4;">
                                            <em>🤖 Généré automatiquement par ELA DataPlatform • {date_str}</em>
                                        </p>
                                    </td>
                                </tr>
                            </table>

                        </td>
                    </tr>
                </table>

            </td>
        </tr>
    </table>

</body>
</html>"""

        return html_content

    @classmethod
    def generate_text_email(cls, strava_data: StravaData) -> str:
        """Génère la version texte de l'email Strava"""
        week_num = datetime.now().isocalendar()[1]
        date_str = datetime.now().strftime("%d/%m/%Y")

        text_content = f"""
🔥 TON RÉSUMÉ STRAVA - SEMAINE {week_num}

📊 STATISTIQUES:
   Activités: {strava_data.stats.total_activities}
   Distance: {strava_data.stats.total_distance:.1f}km
   Temps: {strava_data.stats.total_time}
   Dénivelé: {strava_data.stats.total_elevation:.0f}m

🚴‍♂️ TES ACTIVITÉS:
"""
        for i, activity in enumerate(strava_data.activities, 1):
            icon = cls.get_activity_icon(activity.type)
            distance_str = f"{activity.distance:.1f}km" if activity.distance else "N/A"
            text_content += f"{icon} {activity.name}\n"
            text_content += (
                f"   {activity.date} • {distance_str} • {activity.moving_time}\n\n"
            )

        text_content += f"""
📱 Voir plus de détails sur Strava
🤖 ELA DataPlatform - {date_str}
"""
        return text_content
