#!/usr/bin/env python3
"""Rebuild the entire GASAG data pipeline from scratch."""

from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

# Setup BigQuery client
credentials_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
credentials = service_account.Credentials.from_service_account_file(credentials_path)
client = bigquery.Client(project='gasag-465208', credentials=credentials)

print("🏗️  Rebuilding GASAG Pipeline\n")

# Step 1: Create datasets
print("1. Creating datasets...")
for dataset_id in ['raw', 'analysis']:
    dataset_ref = f"{client.project}.{dataset_id}"
    try:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"
        client.create_dataset(dataset, exists_ok=True)
        print(f"   ✅ Created dataset: {dataset_id}")
    except Exception as e:
        print(f"   ❌ Error creating {dataset_id}: {e}")

# Step 2: Load CSV data
print("\n2. Loading CSV data...")
try:
    df = pd.read_csv("data/kraftwerksdaten_csv.csv", sep=';')
    print(f"   📊 Read {len(df)} rows from CSV")

    table_ref = f"{client.project}.raw.kraftwerksdaten_raw"
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=[
            bigquery.SchemaField("Kraftwerk", "STRING"),
            bigquery.SchemaField("Timestamp", "STRING"),
            bigquery.SchemaField("Zaehlerstand", "FLOAT")
        ]
    )

    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()
    print(f"   ✅ Loaded {len(df)} rows into raw.kraftwerksdaten_raw")
except Exception as e:
    print(f"   ❌ Error loading CSV: {e}")

print("\n3. Next step: Run dbt models")
print("   cd dbt_gasag && dbt run --profiles-dir profiles/")
print("\n✅ Pipeline foundation ready!")
