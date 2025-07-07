import os
from google.cloud import bigquery
from google.oauth2 import service_account

def get_bigquery_client():
    """Get a BigQuery client using environment variables."""
    credentials_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
    credentials = service_account.Credentials.from_service_account_file(credentials_path)
    return bigquery.Client(project=os.getenv('BIGQUERY_PROJECT'), credentials=credentials)
