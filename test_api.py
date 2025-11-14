from google.cloud import bigquery
from google.oauth2 import service_account
import os
from dotenv import load_dotenv

load_dotenv()

credentials_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
credentials = service_account.Credentials.from_service_account_file(credentials_path)

print(f"Testing with: {credentials.service_account_email}")
print(f"Project: {credentials.project_id}\n")

# Try the simplest possible call
client = bigquery.Client(project='gasag-465208', credentials=credentials)

try:
    # This just checks if we can connect
    job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
    query = "SELECT 1"
    query_job = client.query(query, job_config=job_config)
    print("✅ BigQuery API is accessible!")
    print(f"   Total bytes processed: {query_job.total_bytes_processed}")
except Exception as e:
    print(f"❌ Error: {e}")
    if "API has not been used" in str(e) or "disabled" in str(e):
        print("\n🔧 FIX: Enable BigQuery API at:")
        print(f"   https://console.cloud.google.com/apis/library/bigquery.googleapis.com?project=gasag-465208")

