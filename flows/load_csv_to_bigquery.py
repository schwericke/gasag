import pandas as pd
from prefect import flow, task
from utils.bigquery_connection import get_bigquery_client
from dotenv import load_dotenv
import os
from google.cloud import bigquery

# Load environment variables
load_dotenv()

@task
def read_csv(file_path: str) -> pd.DataFrame:
    """Read CSV."""
    return pd.read_csv(file_path, sep=';')

@task
def load_to_bigquery(df: pd.DataFrame, table_name: str = "kraftwerksdaten_raw"):
    """Load DataFrame to BigQuery table (create/overwrite)."""
    client = get_bigquery_client()

    # Define dataset and table
    dataset_id = os.getenv('BIGQUERY_DATASET', 'raw')
    dataset_ref = f"{client.project}.{dataset_id}"
    table_ref = f"{dataset_ref}.{table_name}"

    # Create dataset if not exists
    try:
        client.get_dataset(dataset_ref)
    except:
        dataset = bigquery.Dataset(dataset_ref)
        client.create_dataset(dataset, exists_ok=True)
        print(f"Created dataset {dataset_ref}")

    # Load data
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=[
            bigquery.SchemaField("Kraftwerk", "STRING"),
            bigquery.SchemaField("Timestamp", "STRING"),
            bigquery.SchemaField("Zaehlerstand", "FLOAT")
        ]
    )

    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()  # Wait for job to complete

    print(f"Loaded {len(df)} rows into {table_ref}")

@flow(name="load-csv-to-bigquery")
def load_csv_to_bigquery_flow():
    """Load CSV to BigQuery raw table."""
    df = read_csv("data/kraftwerksdaten_csv.csv")
    load_to_bigquery(df)

if __name__ == "__main__":
    load_csv_to_bigquery_flow()
