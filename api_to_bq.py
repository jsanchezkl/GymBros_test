import os
import requests
import pandas as pd
from google.cloud import bigquery

BQ_DATASET = os.getenv('BQ_DATASET', 'raw')
BQ_TABLE = os.getenv('BQ_API_TABLE', 'crypto_prices')
PROJECT_ID = os.getenv('GCP_PROJECT_ID')

def load_data_from_api():
    """
    Add data API
    """
    api_url = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum&vs_currencies=usd"
    try:
        response = requests.get(api_url)
        response.raise_for_status()
        data = response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error connect API: {e}")
        return

    df = pd.DataFrame.from_dict(data, orient='index')
    df.reset_index(inplace=True)
    df.rename(columns={'index': 'coin_id'}, inplace=True)
    df['ingestion_time'] = pd.to_datetime('now', utc=True)
    
    bq_client = bigquery.Client()
    table_id = f"{PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"
    
    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        write_disposition="WRITE_APPEND"
    )
    
    job = bq_client.load_table_from_dataframe(
        df, table_id, job_config=job_config
    )
    job.result()
    print(f"load {job.output_rows} in {table_id}")

if __name__ == "__main__":
    load_data_from_api()