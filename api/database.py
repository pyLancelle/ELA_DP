from google.cloud import bigquery
from api.config import PROJECT_ID

_bq_client = None


def get_bq_client():
    global _bq_client
    if _bq_client is None:
        _bq_client = bigquery.Client(project=PROJECT_ID)
    return _bq_client
