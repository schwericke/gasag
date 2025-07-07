import pandas as pd
from prefect import flow, task
from utils.bigquery_connection import get_bigquery_client
from dotenv import load_dotenv
import os
from google.cloud import bigquery

# Load environment variables
load_dotenv()

@task
def get_raw_data():
    """Get raw data from BigQuery."""
    client = get_bigquery_client()
    query = """
    SELECT * FROM `gasag-465208.raw.kraftwerksdaten_raw`
    """
    return client.query(query).to_dataframe()

@task
def calculate_metrics(df: pd.DataFrame):
    """Calculate average base load and daily deviation for each Kraftwerk."""
    # Convert timestamp to datetime
    df['Timestamp'] = pd.to_datetime(df['Timestamp'], format="%d.%m.%Y %H:%M")

    # Calculate Leistung (difference in Zaehlerstand)
    df = df.sort_values(['Kraftwerk', 'Timestamp'])
    df['Leistung'] = df.groupby('Kraftwerk')['Zaehlerstand'].diff()

    # Filter out the half day 11.02.2024 and first measurements
    df = df[(df['Timestamp'] > '2024-01-01 00:00:00') &
            (df['Timestamp'] < '2024-02-11 01:00:00')]

    # 1. Durchschnittliche Grundlast (average base load)
    avg_base_load = df.groupby('Kraftwerk')['Leistung'].mean().reset_index()
    avg_base_load.columns = ['Kraftwerk', 'Durchschnittliche_Grundlast']

    # 2. Tagesabweichung vom Mittelwert (daily deviation from mean)
    # Get data from last half day (11.02.2024)
    last_day_data = df[df['Timestamp'] >= '2024-02-10 12:00:00'].copy()

    # Calculate daily average for each Kraftwerk
    daily_avg = df.groupby(['Kraftwerk', df['Timestamp'].dt.date])['Leistung'].mean().reset_index()
    daily_avg.columns = ['Kraftwerk', 'Date', 'Tagesmittel']

    # Calculate overall mean for each Kraftwerk
    overall_mean = df.groupby('Kraftwerk')['Leistung'].mean().reset_index()
    overall_mean.columns = ['Kraftwerk', 'Gesamtmittel']

    # Calculate deviation
    deviation = daily_avg.merge(overall_mean, on='Kraftwerk')
    deviation['Tagesabweichung'] = deviation['Tagesmittel'] - deviation['Gesamtmittel']

    return avg_base_load, deviation

@task
def load_to_bigquery(avg_base_load: pd.DataFrame, deviation: pd.DataFrame):
    """Load transformed data to BigQuery."""
    client = get_bigquery_client()
    dataset_id = os.getenv('BIGQUERY_DATASET', 'raw')
    dataset_ref = f"{client.project}.{dataset_id}"

    # Create dataset if not exists
    try:
        client.get_dataset(dataset_ref)
    except:
        dataset = bigquery.Dataset(dataset_ref)
        client.create_dataset(dataset, exists_ok=True)

    # Load average base load table
    avg_table_ref = f"{dataset_ref}.kraftwerke_grundlast"
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=[
            bigquery.SchemaField("Kraftwerk", "STRING"),
            bigquery.SchemaField("Durchschnittliche_Grundlast", "FLOAT")
        ]
    )
    job = client.load_table_from_dataframe(avg_base_load, avg_table_ref, job_config=job_config)
    job.result()
    print(f"Loaded {len(avg_base_load)} rows into {avg_table_ref}")

    # Load deviation table
    dev_table_ref = f"{dataset_ref}.kraftwerke_tagesabweichung"
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=[
            bigquery.SchemaField("Kraftwerk", "STRING"),
            bigquery.SchemaField("Date", "DATE"),
            bigquery.SchemaField("Tagesmittel", "FLOAT"),
            bigquery.SchemaField("Gesamtmittel", "FLOAT"),
            bigquery.SchemaField("Tagesabweichung", "FLOAT")
        ]
    )
    job = client.load_table_from_dataframe(deviation, dev_table_ref, job_config=job_config)
    job.result()
    print(f"Loaded {len(deviation)} rows into {dev_table_ref}")

@flow(name="transform-kraftwerke-data")
def transform_kraftwerke_flow():
    """Transform raw data for dashboard."""
    df = get_raw_data()
    avg_base_load, deviation = calculate_metrics(df)
    load_to_bigquery(avg_base_load, deviation)

if __name__ == "__main__":
    transform_kraftwerke_flow()
