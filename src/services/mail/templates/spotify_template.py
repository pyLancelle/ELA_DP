from datetime import datetime
from typing import Dict
from ..core.models import SpotifyData, SpotifyArtist, SpotifyTrack


class SpotifyEmailTemplate:
    """Template generator pour les emails Spotify"""

    @staticmethod
    def get_rank_styles(position: int) -> Dict[str, str]:
        """Retourne les styles CSS pour chaque position"""
        if position == 1:
            return {
                "bg": "background: linear-gradient(135deg, #ffd700, #ffed4e); border: 2px solid #ffd700;",
                "color": "#8b6f00",
            }
        elif position == 2:
            return {
                "bg": "background: #f5f5f5; border: 2px solid #e0e0e0;",
                "color": "#666",
            }
        elif position == 3:
            return {
                "bg": "background: linear-gradient(135deg, #cd7f32, #d2b48c); border: 2px solid #cd7f32;",
                "color": "#5c4000",
            }
        else:
            return {
                "bg": "background: #f9f9f9; border: 2px solid #f0f0f0;",
                "color": "#666",
            }

    @classmethod
    def generate_artist_row(cls, artist: SpotifyArtist, position: int) -> str:
        """Génère une ligne du classement artistes"""
        styles = cls.get_rank_styles(position)
        artist_url = artist.artist_url or "#"
        image_url = artist.image_url or "https://via.placeholder.com/36"

        return f"""
        <tr>
            <td style="padding-bottom: 12px;">
                <table width="100%" cellpadding="0" cellspacing="0" border="0" style="{styles['bg']} border-radius: 20px; overflow: hidden;">
                    <tr>
                        <td style="padding: 16px 20px;">
                            <a href="{artist_url}" style="text-decoration: none; color: inherit; display: block;">
                                <table width="100%" cellpadding="0" cellspacing="0" border="0">
                                    <tr>
                                        <td width="40" style="color: {styles['color']}; font-weight: 700; font-size: 16px; vertical-align: middle; padding-right: 16px;">#{position}</td>
                                        <td width="40" style="padding-right: 12px; vertical-align: middle;">
                                            <img src="{image_url}"
                                                 width="36" height="36"
                                                 style="width: 36px; height: 36px; border-radius: 50%; display: block; border: 0;"
                                                 alt="{artist.name}">
                                        </td>
                                        <td style="color: {styles['color']}; font-weight: 600; font-size: 17px; vertical-align: middle;">{artist.name}</td>
                                        <td align="right" style="color: {styles['color']}; font-weight: 700; font-size: 17px; vertical-align: middle;">{artist.time_display}</td>
                                    </tr>
                                </table>
                            </a>
                        </td>
                    </tr>
                </table>
            </td>
        </tr>"""

    @classmethod
    def generate_track_row(cls, track: SpotifyTrack, position: int) -> str:
        """Génère une ligne du classement titres"""
        styles = cls.get_rank_styles(position)
        track_url = track.track_url or "#"
        image_url = track.image_url or "https://via.placeholder.com/36"

        return f"""
        <tr>
            <td style="padding-bottom: 12px;">
                <table width="100%" cellpadding="0" cellspacing="0" border="0" style="{styles['bg']} border-radius: 20px; overflow: hidden;">
                    <tr>
                        <td style="padding: 16px 20px;">
                            <a href="{track_url}" style="text-decoration: none; color: inherit; display: block;">
                                <table width="100%" cellpadding="0" cellspacing="0" border="0">
                                    <tr>
                                        <td width="40" style="color: {styles['color']}; font-weight: 700; font-size: 16px; vertical-align: middle; padding-right: 16px;">#{position}</td>
                                        <td width="40" style="padding-right: 12px; vertical-align: middle;">
                                            <img src="{image_url}"
                                                 width="36" height="36"
                                                 style="width: 36px; height: 36px; border-radius: 8px; display: block; border: 0;"
                                                 alt="{track.artist_name} - {track.track_name}">
                                        </td>
                                        <td style="color: {styles['color']}; vertical-align: middle;">
                                            <div style="font-weight: 600; font-size: 17px; line-height: 1.2; margin-bottom: 2px;">{track.artist_name}</div>
                                            <div style="font-weight: 500; font-size: 14px; opacity: 0.8; line-height: 1.2;">{track.track_name}</div>
                                        </td>
                                        <td align="right" style="color: {styles['color']}; font-weight: 700; font-size: 17px; vertical-align: middle;">{track.play_display}</td>
                                    </tr>
                                </table>
                            </a>
                        </td>
                    </tr>
                </table>
            </td>
        </tr>"""

    @classmethod
    def generate_html_email(cls, spotify_data: SpotifyData) -> str:
        """Génère le HTML complet de l'email"""
        current_week = datetime.now().isocalendar()[1]
        previous_week = current_week - 1 if current_week > 1 else 52
        date_str = datetime.now().strftime("%d/%m/%Y")

        # Générer les lignes des classements
        artist_rows = ""
        for i, artist in enumerate(spotify_data.artists[:10], 1):
            artist_rows += cls.generate_artist_row(artist, i)

        track_rows = ""
        for i, track in enumerate(spotify_data.tracks[:10], 1):
            track_rows += cls.generate_track_row(track, i)

        # Template HTML complet
        html_content = f"""<!DOCTYPE html>
<html lang="fr">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Spotify Overview - W{previous_week}</title>
    <!--[if mso]>
    <noscript>
        <xml>
            <o:OfficeDocumentSettings>
                <o:PixelsPerInch>96</o:PixelsPerInch>
            </o:OfficeDocumentSettings>
        </xml>
    </noscript>
    <![endif]-->
</head>
<body style="margin: 0; padding: 0; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Helvetica, Arial, sans-serif; background-color: #f8fafc; -webkit-text-size-adjust: 100%; -ms-text-size-adjust: 100%;">

    <table width="100%" cellpadding="0" cellspacing="0" border="0" style="background-color: #f8fafc;">
        <tr>
            <td align="center" style="padding: 20px 10px;">

                <table width="600" cellpadding="0" cellspacing="0" border="0" style="max-width: 600px; width: 100%; background: white; border-radius: 16px; box-shadow: 0 10px 30px rgba(0,0,0,0.1);">
                    <tr>
                        <td style="padding: 40px 30px;">

                            <!-- Header -->
                            <table width="100%" cellpadding="0" cellspacing="0" border="0">
                                <tr>
                                    <td align="center" style="padding-bottom: 50px;">
                                        <h1 style="margin: 0; font-size: 28px; font-weight: 700; color: #1a1a1a; letter-spacing: 0.5px; line-height: 1.2;">
                                            🎵 SPOTIFY - OVERVIEW - W{previous_week}
                                        </h1>
                                        <p style="margin: 15px 0 0 0; color: #64748b; font-size: 16px; line-height: 1.4;">
                                            Tes découvertes musicales de la semaine
                                        </p>
                                    </td>
                                </tr>
                            </table>

                            <!-- TOP ARTISTES SECTION -->
                            <table width="100%" cellpadding="0" cellspacing="0" border="0" style="margin-bottom: 60px;">
                                <tr>
                                    <td align="center" style="padding-bottom: 30px;">
                                        <h2 style="margin: 0; font-size: 22px; font-weight: 600; color: #374151;">
                                            🎤 TOP ARTISTES
                                        </h2>
                                    </td>
                                </tr>

                                {artist_rows}

                            </table>

                            <!-- TOP TITRES SECTION -->
                            <table width="100%" cellpadding="0" cellspacing="0" border="0" style="margin-bottom: 40px;">
                                <tr>
                                    <td align="center" style="padding-bottom: 30px;">
                                        <h2 style="margin: 0; font-size: 22px; font-weight: 600; color: #374151;">
                                            🎵 TOP TITRES
                                        </h2>
                                    </td>
                                </tr>

                                {track_rows}

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
    def generate_text_email(cls, spotify_data: SpotifyData) -> str:
        """Génère la version texte de l'email"""
        current_week = datetime.now().isocalendar()[1]
        previous_week = current_week - 1 if current_week > 1 else 52
        date_str = datetime.now().strftime("%d/%m/%Y")

        text_content = f"""
🎵 SPOTIFY - OVERVIEW - W{previous_week}

🎤 TOP ARTISTES:
"""
        text_content += "\n".join(
            [
                f"#{i+1:2} {artist.name:25} {artist.time_display:>8}"
                for i, artist in enumerate(spotify_data.artists[:5])
            ]
        )

        text_content += f"""

🎵 TOP TITRES:
"""
        text_content += "\n".join(
            [
                f"#{i+1:2} {track.artist_name} - {track.track_name[:30]:30} {track.play_display:>8}"
                for i, track in enumerate(spotify_data.tracks[:5])
            ]
        )

        text_content += f"""

🤖 ELA DataPlatform - {date_str}
"""
        return text_content
