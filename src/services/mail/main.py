from .functions import send_spotify_email
import os

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