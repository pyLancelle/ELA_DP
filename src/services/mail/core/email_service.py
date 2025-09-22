import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Optional

from .models import EmailConfig, EmailContent


class EmailService:
    """Service générique d'envoi d'emails"""

    def __init__(self, config: EmailConfig):
        self.config = config

    def create_email_content(
        self,
        to_email: str,
        subject: str,
        text_content: str,
        html_content: Optional[str] = None,
    ) -> EmailContent:
        """Crée un objet EmailContent"""
        return EmailContent(
            subject=subject,
            text_content=text_content,
            html_content=html_content or text_content,
            to_email=to_email,
        )

    def send_email(self, email_content: EmailContent) -> bool:
        """Envoie un email"""
        try:
            # Créer le message
            msg = MIMEMultipart("alternative")
            msg["From"] = self.config.email_from
            msg["To"] = email_content.to_email
            msg["Subject"] = email_content.subject

            # Attacher la version texte
            text_part = MIMEText(email_content.text_content, "plain", "utf-8")
            msg.attach(text_part)

            # Attacher la version HTML si disponible
            if email_content.html_content != email_content.text_content:
                html_part = MIMEText(email_content.html_content, "html", "utf-8")
                msg.attach(html_part)

            # Envoyer
            with smtplib.SMTP(self.config.smtp_server, self.config.smtp_port) as server:
                if self.config.use_tls:
                    server.starttls()
                server.login(self.config.email_from, self.config.email_password)
                server.send_message(msg)

            print("✅ Email envoyé avec succès!")
            return True

        except Exception as e:
            print(f"❌ Erreur lors de l'envoi de l'email: {e}")
            return False

    def test_connection(self) -> bool:
        """Test la connexion SMTP"""
        try:
            with smtplib.SMTP(self.config.smtp_server, self.config.smtp_port) as server:
                if self.config.use_tls:
                    server.starttls()
                server.login(self.config.email_from, self.config.email_password)
            return True
        except Exception:
            return False
