EMAIL_FROM = "elancelle.code@gmail.com"
EMAIL_TO = "etiennelancelle@outlook.fr"
EMAIL_PASS = "kmaa rwwe vwzj uowu"

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime, timedelta
from google.cloud import bigquery
from google.auth.transport.requests import Request
from google.oauth2 import service_account
import os
import json

def get_bigquery_client():
    """Configure le client BigQuery avec les credentials du service account"""
    try:
        credentials_path = "gcs_key.json"
        
        if not os.path.exists(credentials_path):
            raise FileNotFoundError(f"Fichier de credentials non trouvé: {credentials_path}")
        
        credentials = service_account.Credentials.from_service_account_file(
            credentials_path,
            scopes=["https://www.googleapis.com/auth/bigquery"]
        )
        
        client = bigquery.Client(
            credentials=credentials,
            project=credentials.project_id
        )
        
        print(f"✅ BigQuery client configuré avec le projet: {credentials.project_id}")
        return client
        
    except Exception as e:
        print(f"❌ Erreur configuration BigQuery: {e}")
        return None

def get_spotify_data():
    """Récupère le Top 10 Artistes ET Top 10 Titres depuis BigQuery"""
    try:
        client = get_bigquery_client()
        
        if client is None:
            raise Exception("Impossible de configurer le client BigQuery")
        
        print("🔍 Exécution des requêtes BigQuery...")
        
        # Query pour TOP ARTISTES - CORRIGÉE avec artist_image_url
        artists_query = """
        SELECT rank, artist_name, time_listened, total_plays, artist_url, artist_image_url
        FROM `polar-scene-465223-f7.dp_product_dev.pct_emailing__spotify_top10_artists`
        ORDER BY rank
        LIMIT 10
        """
        
        # Query pour TOP TITRES - CORRIGÉE avec time_listened
        tracks_query = """
        SELECT rank, artist_name, track_name, time_listened, total_plays, track_url, album_cover_url
        FROM `polar-scene-465223-f7.dp_product_dev.pct_emailing__spotify_top10_tracks`
        ORDER BY rank
        LIMIT 10
        """
        
        artists_results = client.query(artists_query).to_dataframe()
        tracks_results = client.query(tracks_query).to_dataframe()
        
        print(f"📊 Trouvé: {len(artists_results)} artistes, {len(tracks_results)} titres")
        
        if len(artists_results) == 0 or len(tracks_results) == 0:
            raise Exception("Aucune donnée trouvée")
            
        # Adapter les données BigQuery au format attendu par l'email
        artists_data = []
        for _, row in artists_results.iterrows():
            # Extraire l'ID Spotify depuis l'URL
            artist_id = row.get('artist_url', '').split('/')[-1] if row.get('artist_url') else 'unknown'
            
            artists_data.append({
                'name': row['artist_name'],
                'time_display': row['time_listened'],
                'play_count': row.get('total_plays', 0),
                'artist_id': artist_id,
                'artist_url': row.get('artist_url', '#'),
                'image_url': row.get('artist_image_url', 'https://via.placeholder.com/36')  # Placeholder pour les images d'artiste
            })
        
        tracks_data = []
        for _, row in tracks_results.iterrows():
            # Extraire l'ID Spotify depuis l'URL
            track_id = row.get('track_url', '').split('/')[-1] if row.get('track_url') else 'unknown'
            
            tracks_data.append({
                'artist_name': row['artist_name'],
                'track_name': row['track_name'],
                'play_display': row['time_listened'],  # MAINTENANT C'EST LE TEMPS D'ÉCOUTE
                'play_count': row.get('total_plays', 0),
                'track_id': track_id,
                'track_url': row.get('track_url', '#'),
                'image_url': row.get('album_cover_url', 'https://via.placeholder.com/36')
            })
            
        return {
            'artists': artists_data,
            'tracks': tracks_data
        }
        
    except Exception as e:
        print(f"⚠️  Erreur BigQuery: {e}")
        print("📊 Utilisation des données de test...")
        
        # Fallback avec données mockées
        return {
            'artists': [
                {"name": "Justice", "time_display": "3h27", "play_count": 45, "artist_id": "1gBUSTmm0PfPasR7RyYTNI", "artist_url": "https://open.spotify.com/artist/1gBUSTmm0PfPasR7RyYTNI", "image_url": "https://i.scdn.co/image/ab6761610000e5eb0c68f6c95232e716e2a2c394"},
                {"name": "twenty one pilots", "time_display": "2h11", "play_count": 38, "artist_id": "3YQKmKGau1PzlVlkL5iodx", "artist_url": "https://open.spotify.com/artist/3YQKmKGau1PzlVlkL5iodx", "image_url": "https://i.scdn.co/image/ab6761610000e5eb196972172c37d934d9ca32d6"},
                {"name": "Daft Punk", "time_display": "2h06", "play_count": 35, "artist_id": "4tZwfgrHOc3mvqYlEYSvVi", "artist_url": "https://open.spotify.com/artist/4tZwfgrHOc3mvqYlEYSvVi", "image_url": "https://i.scdn.co/image/ab6761610000e5ebae07171f989e2cb3b0662487"},
                {"name": "Dire Straits", "time_display": "2h03", "play_count": 32, "artist_id": "0WwSkZ7LtFUFjGjMZBMt6T", "artist_url": "https://open.spotify.com/artist/0WwSkZ7LtFUFjGjMZBMt6T", "image_url": "https://i.scdn.co/image/ab6761610000e5eb941445b7d9c4ca7c2ce77832"},
                {"name": "Dua Lipa", "time_display": "1h43", "play_count": 28, "artist_id": "6M2wZ9GZgrQXHCFfjv46we", "artist_url": "https://open.spotify.com/artist/6M2wZ9GZgrQXHCFfjv46we", "image_url": "https://i.scdn.co/image/ab6761610000e5eb5a00969a4698c3132a15fbb0"},
            ],
            'tracks': [
                {"artist_name": "Justice", "track_name": "D.A.N.C.E.", "play_display": "15x", "play_count": 15, "track_id": "5W3cjX2J3tjhG8zb6u0qHn", "track_url": "https://open.spotify.com/track/5W3cjX2J3tjhG8zb6u0qHn", "image_url": "https://i.scdn.co/image/ab67616d0000b273e0b64c0840ddeef14d44a1c9"},
                {"artist_name": "Daft Punk", "track_name": "One More Time", "play_display": "13x", "play_count": 13, "track_id": "0DiWol3AO6WpXZgp0goxAV", "track_url": "https://open.spotify.com/track/0DiWol3AO6WpXZgp0goxAV", "image_url": "https://i.scdn.co/image/ab67616d0000b273195710000825d1fb8a5c8bca"},
                {"artist_name": "twenty one pilots", "track_name": "Heathens", "play_display": "12x", "play_count": 12, "track_id": "6I1kxjDaEiMBbdUUhBjlTe", "track_url": "https://open.spotify.com/track/6I1kxjDaEiMBbdUUhBjlTe", "image_url": "https://i.scdn.co/image/ab67616d0000b273cdb645498cd3d8a2db4d05e1"},
                {"artist_name": "The Weeknd", "track_name": "Blinding Lights", "play_display": "11x", "play_count": 11, "track_id": "0VjIjW4GlUZAMYd2vXMi3b", "track_url": "https://open.spotify.com/track/0VjIjW4GlUZAMYd2vXMi3b", "image_url": "https://i.scdn.co/image/ab67616d0000b2738863bc11d2aa12b54f5aeb36"},
                {"artist_name": "Dire Straits", "track_name": "Sultans of Swing", "play_display": "10x", "play_count": 10, "track_id": "09YKLmVHUjFB5ZUjOsZiLh", "track_url": "https://open.spotify.com/track/09YKLmVHUjFB5ZUjOsZiLh", "image_url": "https://i.scdn.co/image/ab67616d0000b273a91c10fe9472d9bd89802e5a"},
            ]
        }

def get_rank_styles(position):
    """Retourne les styles CSS pour chaque position"""
    if position == 1:
        return {
            "bg": "background: linear-gradient(135deg, #ffd700, #ffed4e); border: 2px solid #ffd700;",
            "color": "#8b6f00"
        }
    elif position == 2:
        return {
            "bg": "background: #f5f5f5; border: 2px solid #e0e0e0;",
            "color": "#666"
        }
    elif position == 3:
        return {
            "bg": "background: linear-gradient(135deg, #cd7f32, #d2b48c); border: 2px solid #cd7f32;",
            "color": "#5c4000"
        }
    else:
        return {
            "bg": "background: #f9f9f9; border: 2px solid #f0f0f0;",
            "color": "#666"
        }

def generate_artist_row(artist, position):
    """Génère une ligne du classement artistes"""
    styles = get_rank_styles(position)
    # Utilisation des vraies URLs depuis BigQuery
    artist_url = artist.get('artist_url', '#')
    image_url = artist.get('image_url', 'https://via.placeholder.com/36')
    
    return f"""
    <tr>
        <td style="padding-bottom: 12px;">
            <table width="100%" cellpadding="0" cellspacing="0" border="0" style="{styles['bg']} border-radius: 20px; overflow: hidden;">
                <tr>
                    <td style="padding: 16px 20px;">
                        <a href="{artist_url}" style="text-decoration: none; color: inherit; display: block;">
                            <table width="100%" cellpadding="0" cellspacing="0" border="0">
                                <tr>
                                    <td width="30" style="color: {styles['color']}; font-weight: 700; font-size: 16px; vertical-align: middle;">#{position}</td>
                                    <td width="40" style="padding-right: 12px; vertical-align: middle;">
                                        <img src="{image_url}" 
                                             width="36" height="36"
                                             style="width: 36px; height: 36px; border-radius: 50%; display: block; border: 0;" 
                                             alt="{artist['name']}">
                                    </td>
                                    <td style="color: {styles['color']}; font-weight: 600; font-size: 17px; vertical-align: middle;">{artist['name']}</td>
                                    <td align="right" style="color: {styles['color']}; font-weight: 700; font-size: 17px; vertical-align: middle;">{artist['time_display']}</td>
                                </tr>
                            </table>
                        </a>
                    </td>
                </tr>
            </table>
        </td>
    </tr>"""

def generate_track_row(track, position):
    """Génère une ligne du classement titres"""
    styles = get_rank_styles(position)
    # Utilisation des vraies URLs depuis BigQuery
    track_url = track.get('track_url', '#')
    image_url = track.get('image_url', 'https://via.placeholder.com/36')
    
    return f"""
    <tr>
        <td style="padding-bottom: 12px;">
            <table width="100%" cellpadding="0" cellspacing="0" border="0" style="{styles['bg']} border-radius: 20px; overflow: hidden;">
                <tr>
                    <td style="padding: 16px 20px;">
                        <a href="{track_url}" style="text-decoration: none; color: inherit; display: block;">
                            <table width="100%" cellpadding="0" cellspacing="0" border="0">
                                <tr>
                                    <td width="30" style="color: {styles['color']}; font-weight: 700; font-size: 16px; vertical-align: top; padding-top: 2px;">#{position}</td>
                                    <td width="40" style="padding-right: 12px; vertical-align: top;">
                                        <img src="{image_url}" 
                                             width="36" height="36"
                                             style="width: 36px; height: 36px; border-radius: 8px; display: block; border: 0;" 
                                             alt="{track['artist_name']} - {track['track_name']}">
                                    </td>
                                    <td style="color: {styles['color']}; vertical-align: top;">
                                        <div style="font-weight: 600; font-size: 17px; line-height: 1.2; margin-bottom: 2px;">{track['artist_name']}</div>
                                        <div style="font-weight: 500; font-size: 14px; opacity: 0.8; line-height: 1.2;">{track['track_name']}</div>
                                    </td>
                                    <td align="right" style="color: {styles['color']}; font-weight: 700; font-size: 17px; vertical-align: top; padding-top: 8px;">{track['play_display']}</td>
                                </tr>
                            </table>
                        </a>
                    </td>
                </tr>
            </table>
        </td>
    </tr>"""

def generate_email_html(spotify_data):
    """Génère le HTML complet de l'email"""
    
    week_num = datetime.now().isocalendar()[1]
    date_str = datetime.now().strftime('%d/%m/%Y')
    
    # Générer les lignes des classements
    artist_rows = ""
    for i, artist in enumerate(spotify_data['artists'][:10], 1):
        artist_rows += generate_artist_row(artist, i)
    
    track_rows = ""
    for i, track in enumerate(spotify_data['tracks'][:10], 1):
        track_rows += generate_track_row(track, i)
    
    # Template HTML complet
    html_content = f"""<!DOCTYPE html>
<html lang="fr">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Spotify Top 10 - Week {week_num}</title>
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
                                            🎵 SPOTIFY - WEEK {week_num}
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
                            
                            <!-- Call to action -->
                            <table width="100%" cellpadding="0" cellspacing="0" border="0" style="background: linear-gradient(135deg, #1DB954, #1ed760); border-radius: 16px; margin-bottom: 40px;">
                                <tr>
                                    <td align="center" style="padding: 25px 20px;">
                                        <p style="margin: 0 0 15px 0; color: white; font-size: 18px; font-weight: 700; line-height: 1.2;">
                                            🎧 Découvre tes stats complètes
                                        </p>
                                        <a href="https://open.spotify.com/genre/wrapped" 
                                           style="display: inline-block; background: white; color: #1DB954; padding: 12px 25px; border-radius: 25px; text-decoration: none; font-weight: 700; font-size: 16px; line-height: 1;">
                                            Voir sur Spotify →
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

def send_spotify_email():
    """Fonction principale pour envoyer l'email"""
    
    print(f"📧 Configuration email:")
    print(f"   FROM: {EMAIL_FROM}")
    print(f"   TO: {EMAIL_TO}")
    print(f"   PASS: {'*' * len(EMAIL_PASS)}")
    
    try:
        # 1. Récupérer les données
        print("\n📊 Récupération des données Spotify...")
        spotify_data = get_spotify_data()
        
        # 2. Générer l'HTML
        print("🎨 Génération de l'email mobile-optimisé...")
        html_content = generate_email_html(spotify_data)
        
        # 3. Créer le message email
        msg = MIMEMultipart('alternative')
        msg['From'] = EMAIL_FROM
        msg['To'] = EMAIL_TO
        msg['Subject'] = f'🎵 Tes Top 5 Spotify - Artistes & Titres - S{datetime.now().isocalendar()[1]}'
        
        # Version texte (fallback)
        text_version = f"""
🎵 TES TOP 5 SPOTIFY - SEMAINE {datetime.now().isocalendar()[1]}

🎤 TOP ARTISTES:
""" + "\n".join([f"#{i+1:2} {item['name']:25} {item['time_display']:>8}" 
                  for i, item in enumerate(spotify_data['artists'][:5])])

        text_version += f"""

🎵 TOP TITRES:
""" + "\n".join([f"#{i+1:2} {item['artist_name']} - {item['track_name'][:30]:30} {item['play_display']:>8}" 
                  for i, item in enumerate(spotify_data['tracks'][:5])])

        text_version += f"""

🎧 Découvre tes stats complètes sur Spotify
🤖 ELA DataPlatform - {datetime.now().strftime('%d/%m/%Y')}
"""
        
        # Attacher les deux versions
        msg.attach(MIMEText(text_version, 'plain', 'utf-8'))
        msg.attach(MIMEText(html_content, 'html', 'utf-8'))
        
        # 4. Envoyer
        print("📧 Envoi en cours...")
        with smtplib.SMTP('smtp.gmail.com', 587) as server:
            server.starttls()
            server.login(EMAIL_FROM, EMAIL_PASS)
            server.send_message(msg)
        
        # 5. Confirmation
        print("✅ Email envoyé avec succès!")
        print(f"🥇 Top artiste: {spotify_data['artists'][0]['name']}")
        print(f"🎵 Top titre: {spotify_data['tracks'][0]['artist_name']} - {spotify_data['tracks'][0]['track_name']}")
        
        return True
        
    except Exception as e:
        print(f"❌ Erreur: {e}")
        return False

if __name__ == "__main__":
    print("🎵 === SPOTIFY TOP 5 EMAIL SENDER ===")
    print("📱 Version mobile-optimisée avec BigQuery + URLs")
    print("🔑 Utilisation de gcs_key.json pour l'authentification")
    
    # Vérifier que le fichier de credentials existe
    if not os.path.exists("gcs_key.json"):
        print("\n❌ ERREUR: Fichier gcs_key.json non trouvé!")
        print("💡 Assure-toi que le fichier gcs_key.json est dans le répertoire courant")
        exit(1)
    
    send_spotify_email()