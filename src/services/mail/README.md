# Service Mail Modulaire - ELA DataPlatform

Architecture modulaire et extensible pour l'envoi d'emails automatisés, commençant par les données Spotify.

## 🏗️ Architecture

```
src/services/mail/
├── core/                    # Composants de base
│   ├── models.py           # Modèles Pydantic
│   ├── email_service.py    # Service d'envoi générique
│   └── __init__.py
├── providers/               # Fournisseurs de données
│   ├── bigquery_provider.py
│   └── __init__.py
├── templates/               # Générateurs de templates
│   ├── spotify_template.py
│   └── __init__.py
├── services/               # Services spécialisés
│   ├── spotify_service.py
│   └── __init__.py
├── main_new.py            # Point d'entrée refactorisé
├── example_usage.py       # Exemples d'utilisation
└── README.md
```

## 🚀 Utilisation Rapide

### Interface CLI (Recommandée)

```bash
# Spotify avec destinataire unique
python -m src.services.mail.main --perimeter spotify_weekly --destinataires you@gmail.com

# Multi-destinataires
python -m src.services.mail.main --perimeter spotify_weekly --destinataires john@gmail.com,jane@company.com

# Garmin avec mock data
python -m src.services.mail.main --perimeter garmin_weekly --destinataires team@company.com --dry-run

# Mode legacy (rétrocompatible)
python -m src.services.mail.main
```

### Service Programmatique

```python
from src.services.mail import EmailConfig, BigQueryConfig, SpotifyEmailService

# Configuration
email_config = EmailConfig(
    smtp_server="smtp.gmail.com",
    smtp_port=587,
    email_from="your-email@gmail.com",
    email_password="your-app-password"
)

bigquery_config = BigQueryConfig(
    credentials_path="gcs_key.json"
)

# Service
spotify_service = SpotifyEmailService(
    email_config=email_config,
    bigquery_config=bigquery_config,
    to_email="recipient@example.com"
)

# Envoi
success = spotify_service.send_weekly_email()
```

### Service Email Générique

```python
from src.services.mail import EmailConfig, EmailService

email_service = EmailService(email_config)

email_content = email_service.create_email_content(
    to_email="recipient@example.com",
    subject="Test",
    text_content="Hello!",
    html_content="<h1>Hello!</h1>"
)

success = email_service.send_email(email_content)
```

## 🧩 Composants

### Core

- **EmailService**: Service générique d'envoi d'emails via SMTP
- **Models**: Modèles Pydantic pour validation et sérialisation
  - `EmailConfig`: Configuration SMTP
  - `SpotifyData`, `SpotifyArtist`, `SpotifyTrack`: Modèles Spotify
  - `BigQueryConfig`: Configuration BigQuery
  - `EmailContent`: Contenu d'email

### Providers

- **BigQueryProvider**: Interface vers BigQuery avec gestion des credentials et requêtes

### Templates

- **SpotifyEmailTemplate**: Générateur de templates HTML/Text pour Spotify

### Services

- **SpotifyEmailService**: Service complet orchestrant BigQuery → Template → Email

## 🔧 Extensibilité

Pour ajouter un nouveau service (ex: Chess.com):

1. **Modèles** (core/models.py):
```python
class ChessGame(BaseModel):
    white_player: str
    black_player: str
    result: str
    # ...
```

2. **Template** (templates/chess_template.py):
```python
class ChessEmailTemplate:
    @classmethod
    def generate_html_email(cls, chess_data: ChessData) -> str:
        # Logique de génération
        pass
```

3. **Service** (services/chess_service.py):
```python
class ChessEmailService:
    def __init__(self, email_config, bigquery_config, to_email):
        self.email_service = EmailService(email_config)
        # ...
```

## 📋 Bonnes Pratiques

- **Validation**: Utilisation de Pydantic pour la validation des données
- **Séparation des responsabilités**: Chaque module a une responsabilité claire
- **Réutilisabilité**: Components core réutilisables pour nouveaux services
- **Configuration**: Injection de dépendances via des objets de configuration
- **Fallback**: Données de test en cas d'échec BigQuery
- **Testing**: Méthodes de test de connexion intégrées

## 🛠️ Lancement

### Via le script principal
```bash
python -m src.services.mail.main
```

### Via les exemples
```bash
python src/services/mail/example_usage.py
```

## 📦 Dépendances

- `pydantic`: Validation et sérialisation
- `google-cloud-bigquery`: Client BigQuery
- `google-auth`: Authentification Google Cloud
- `pandas`: Manipulation de données
- Standard library: `smtplib`, `email`, `datetime`

## 🔐 Configuration

Nécessite:
- `gcs_key.json`: Fichier de credentials Google Cloud
- Configuration SMTP Gmail avec mot de passe d'application

## 🎯 Avantages de cette Architecture

1. **Modulaire**: Chaque composant peut être développé/testé indépendamment
2. **Extensible**: Facile d'ajouter de nouveaux services
3. **Réutilisable**: Core components partagés
4. **Type-safe**: Validation Pydantic
5. **Maintenable**: Séparation claire des responsabilités
6. **Testable**: Méthodes de test intégrées