from google.cloud import bigquery
from google.oauth2 import service_account
import os
from dotenv import load_dotenv

load_dotenv()

credentials_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
credentials = service_account.Credentials.from_service_account_file(credentials_path)
client = bigquery.Client(project='gasag-465208', credentials=credentials)

print("Checking BigQuery project: gasag-465208\n")

try:
    datasets = list(client.list_datasets())
    if datasets:
        print(f"✅ Found {len(datasets)} datasets:")
        for dataset in datasets:
            print(f"  - {dataset.dataset_id}")

            # List tables in each dataset
            try:
                tables = list(client.list_tables(dataset.dataset_id))
                if tables:
                    for table in tables:
                        print(f"      └─ {table.table_id}")
                else:
                    print(f"      └─ (no tables)")
            except Exception as e:
                print(f"      └─ Error listing tables: {e}")
    else:
        print("❌ No datasets found in project")
except Exception as e:
    print(f"❌ Error: {e}")
