#!/usr/bin/env python3
"""
Point d'entrée principal pour le service d'emails multi-périmètres
Usage:
  python -m src.services.mail.main --perimeter spotify_weekly --destinataires you@gmail.com
  python -m src.services.mail.main --perimeter garmin_weekly --destinataires team@company.com,john@gmail.com
  python -m src.services.mail.main                                          # Mode legacy (Spotify seulement)
"""

import os
import sys
import argparse
from pathlib import Path
from typing import List

from . import EmailConfig, BigQueryConfig, SpotifyEmailService


def parse_args():
    """Parse les arguments CLI"""
    parser = argparse.ArgumentParser(
        description="Service d'emails automatisés multi-périmètres",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemples d'usage:
  %(prog)s --perimeter spotify_weekly --destinataires you@gmail.com
  %(prog)s --perimeter garmin_weekly --destinataires team@company.com,john@gmail.com
  %(prog)s --perimeter strava_weekly --destinataires athletes@club.fr
  %(prog)s                                          # Mode legacy (Spotify uniquement)

Périmètres supportés:
  spotify_weekly    Classement hebdomadaire Spotify (artistes & titres)
  garmin_weekly     Résumé hebdomadaire activités Garmin
  strava_weekly     Performance hebdomadaire Strava
        """,
    )

    parser.add_argument(
        "--perimeter",
        choices=["spotify_weekly", "garmin_weekly", "strava_weekly"],
        default="spotify_weekly",
        help="Périmètre d'email à envoyer (défaut: spotify_weekly)",
    )

    parser.add_argument(
        "--destinataires",
        type=str,
        help="Destinataires séparés par des virgules (ex: john@gmail.com,jane@company.com). Si non spécifié, utilise EMAIL_SMTP_TO",
    )

    parser.add_argument(
        "--env",
        choices=["dev", "prd"],
        default="prd",
        help="Environnement (dev/prd) pour les données BigQuery (défaut: prd)",
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Mode test : génère l'email sans l'envoyer",
    )

    return parser.parse_args()


def parse_recipients(recipients_str: str) -> List[str]:
    """Parse la chaîne de destinataires en liste d'emails"""
    if not recipients_str:
        return []

    # Nettoyer et séparer les emails
    emails = [email.strip() for email in recipients_str.split(",")]
    emails = [email for email in emails if email]  # Supprimer les chaînes vides

    # Validation basique des emails
    valid_emails = []
    for email in emails:
        if "@" in email and "." in email.split("@")[1]:
            valid_emails.append(email)
        else:
            print(f"⚠️  Email invalide ignoré: {email}")

    return valid_emails


def main():
    """Fonction principale"""
    args = parse_args()

    print(f"📧 === SERVICE EMAIL MULTI-PÉRIMÈTRES ===")
    print(f"🎯 Périmètre: {args.perimeter}")
    print(f"🌍 Environnement: {args.env}")

    if args.dry_run:
        print("🧪 Mode DRY-RUN activé - aucun email ne sera envoyé")

    # Détection de l'environnement
    is_github_actions = os.getenv("GITHUB_ACTIONS") == "true"

    if is_github_actions:
        print("🔧 Mode GitHub Actions détecté")
        print("🔑 Utilisation des variables d'environnement pour l'authentification")
    else:
        print("🔑 Mode local - utilisation de gcs_key.json")

    # Configuration des credentials BigQuery
    if is_github_actions:
        # En GitHub Actions, le fichier est créé par l'action google-github-actions/auth
        credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "gcs_key.json")
    else:
        credentials_path = "gcs_key.json"

    if not os.path.exists(credentials_path):
        print(f"\n❌ ERREUR: Fichier {credentials_path} non trouvé!")
        if is_github_actions:
            print(
                "💡 Vérifiez que l'action google-github-actions/auth s'est bien exécutée"
            )
        else:
            print(
                "💡 Assure-toi que le fichier gcs_key.json est dans le répertoire courant"
            )
        sys.exit(1)

    # Configuration email depuis les variables d'environnement ou valeurs par défaut
    email_from = os.getenv("EMAIL_SMTP_FROM", "elancelle.code@gmail.com")
    email_password = os.getenv("EMAIL_SMTP_PASSWORD", "kmaa rwwe vwzj uowu")

    # Gestion des destinataires
    recipients = []
    if args.destinataires:
        recipients = parse_recipients(args.destinataires)
        if not recipients:
            print("❌ Aucun destinataire valide fourni")
            sys.exit(1)
    else:
        # Mode legacy : utiliser EMAIL_SMTP_TO pour rétrocompatibilité locale
        default_to_email = os.getenv("EMAIL_SMTP_TO", "etiennelancelle@outlook.fr")
        recipients = [default_to_email]
        print(
            f"💡 Mode legacy : utilisation du destinataire par défaut {default_to_email}"
        )

    print(f"📧 Configuration email:")
    print(f"   FROM: {email_from}")
    print(f"   TO: {', '.join(recipients)}")
    print(
        f"   PASSWORD: {'*' * len(email_password) if email_password else 'NON DÉFINI'}"
    )

    # Configuration email
    email_config = EmailConfig(
        smtp_server="smtp.gmail.com",
        smtp_port=587,
        email_from=email_from,
        email_password=email_password,
        use_tls=True,
    )

    # Configuration BigQuery
    bigquery_config = BigQueryConfig(credentials_path=credentials_path)

    # Import dynamique de la factory (pour éviter les imports circulaires)
    from .factory import EmailServiceFactory

    # Créer le service selon le périmètre
    try:
        service = EmailServiceFactory.create_service(
            perimeter=args.perimeter,
            email_config=email_config,
            bigquery_config=bigquery_config,
            environment=args.env,
        )
    except ValueError as e:
        print(f"❌ Erreur de configuration: {e}")
        sys.exit(1)

    # Test des services (optionnel)
    print("\n🧪 Test des connexions...")
    test_results = service.test_services()

    if test_results.get("bigquery"):
        print("✅ BigQuery: Connexion OK")
    else:
        print(
            "⚠️  BigQuery: Problème de connexion (utilisation des données de fallback)"
        )

    if test_results.get("smtp"):
        print("✅ SMTP: Connexion OK")
    else:
        print("❌ SMTP: Problème de connexion")
        sys.exit(1)

    # Envoyer l'email à tous les destinataires
    print(f"\n📧 Envoi des emails {args.perimeter}...")
    success_count = 0
    total_count = len(recipients)

    for i, recipient in enumerate(recipients, 1):
        print(f"📨 Envoi {i}/{total_count} vers {recipient}...")

        if args.dry_run:
            print(
                f"🧪 DRY-RUN: Email {args.perimeter} pour {recipient} (pas d'envoi réel)"
            )
            success_count += 1
        else:
            success = service.send_email_to(recipient)
            if success:
                print(f"✅ Envoyé avec succès à {recipient}")
                success_count += 1
            else:
                print(f"❌ Échec envoi vers {recipient}")

    # Rapport final
    print(f"\n📊 Rapport d'envoi:")
    print(f"   Succès: {success_count}/{total_count}")
    print(f"   Périmètre: {args.perimeter}")

    if success_count == total_count:
        print("🎉 Tous les emails envoyés avec succès!")
    elif success_count > 0:
        print("⚠️  Envoi partiel - vérifiez les logs ci-dessus")
        sys.exit(1)
    else:
        print("❌ Aucun email envoyé")
        sys.exit(1)


if __name__ == "__main__":
    main()
