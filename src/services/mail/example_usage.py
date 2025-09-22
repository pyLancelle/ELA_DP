#!/usr/bin/env python3
"""
Exemple d'utilisation du service mail modulaire
Montre comment utiliser les différents composants du module
"""

from src.services.mail import (
    EmailConfig,
    BigQueryConfig,
    SpotifyEmailService,
    EmailService,
)


def example_spotify_service():
    """Exemple d'utilisation du service Spotify"""
    print("📧 === EXEMPLE: SERVICE SPOTIFY ===")

    # Configuration
    email_config = EmailConfig(
        smtp_server="smtp.gmail.com",
        smtp_port=587,
        email_from="your-email@gmail.com",
        email_password="your-app-password",
        use_tls=True,
    )

    bigquery_config = BigQueryConfig(credentials_path="gcs_key.json")

    # Création du service
    spotify_service = SpotifyEmailService(
        email_config=email_config,
        bigquery_config=bigquery_config,
        to_email="recipient@example.com",
    )

    # Test des services
    results = spotify_service.test_services()
    print(f"BigQuery: {'✅' if results['bigquery'] else '❌'}")
    print(f"SMTP: {'✅' if results['smtp'] else '❌'}")

    # Envoi de l'email (décommenté quand prêt)
    # success = spotify_service.send_weekly_email()
    # print(f"Email envoyé: {'✅' if success else '❌'}")


def example_generic_email():
    """Exemple d'utilisation du service email générique"""
    print("\n📧 === EXEMPLE: SERVICE EMAIL GÉNÉRIQUE ===")

    email_config = EmailConfig(
        smtp_server="smtp.gmail.com",
        smtp_port=587,
        email_from="your-email@gmail.com",
        email_password="your-app-password",
        use_tls=True,
    )

    email_service = EmailService(email_config)

    # Créer un email simple
    email_content = email_service.create_email_content(
        to_email="recipient@example.com",
        subject="Test Email",
        text_content="Ceci est un test!",
        html_content="<h1>Ceci est un test!</h1>",
    )

    # Test de connexion
    if email_service.test_connection():
        print("✅ Connexion SMTP OK")
        # Envoi (décommenté quand prêt)
        # success = email_service.send_email(email_content)
        # print(f"Email envoyé: {'✅' if success else '❌'}")
    else:
        print("❌ Problème de connexion SMTP")


def example_extensibility():
    """Exemple montrant comment étendre le système"""
    print("\n🔧 === EXEMPLE: EXTENSIBILITÉ ===")
    print("Pour ajouter un nouveau service (ex: Chess.com):")
    print("1. Créer ChessEmailService dans services/")
    print("2. Créer ChessEmailTemplate dans templates/")
    print("3. Ajouter les modèles spécifiques dans core/models.py")
    print("4. Utiliser BigQueryProvider ou créer un nouveau provider")
    print("5. Réutiliser EmailService pour l'envoi")


if __name__ == "__main__":
    example_spotify_service()
    example_generic_email()
    example_extensibility()
