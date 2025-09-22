import os
from typing import Optional
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd

from ..core.models import BigQueryConfig


class BigQueryProvider:
    """Provider pour les interactions avec BigQuery"""

    def __init__(self, config: BigQueryConfig):
        self.config = config
        self._client: Optional[bigquery.Client] = None

    @property
    def client(self) -> bigquery.Client:
        """Lazy loading du client BigQuery"""
        if self._client is None:
            self._client = self._create_client()
        return self._client

    def _create_client(self) -> bigquery.Client:
        """Crée et configure le client BigQuery"""
        if not os.path.exists(self.config.credentials_path):
            raise FileNotFoundError(
                f"Fichier de credentials non trouvé: {self.config.credentials_path}"
            )

        credentials = service_account.Credentials.from_service_account_file(
            self.config.credentials_path,
            scopes=["https://www.googleapis.com/auth/bigquery"],
        )

        project_id = self.config.project_id or credentials.project_id
        if not project_id:
            raise ValueError(
                "Aucun project_id trouvé dans la configuration ou les credentials"
            )

        client = bigquery.Client(credentials=credentials, project=project_id)

        print(f"✅ BigQuery client configuré avec le projet: {project_id}")
        return client

    def execute_query(self, query: str) -> pd.DataFrame:
        """Exécute une requête et retourne un DataFrame"""
        try:
            result = self.client.query(query)
            return result.to_dataframe()
        except Exception as e:
            print(f"❌ Erreur lors de l'exécution de la requête: {e}")
            raise

    def test_connection(self) -> bool:
        """Test la connexion à BigQuery"""
        try:
            query = "SELECT 1 as test_value"
            result = self.execute_query(query)
            return len(result) == 1
        except Exception:
            return False
