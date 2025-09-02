import os
import pandas as pd
from google.cloud import bigquery
from google.cloud.sql.connector import Connector, Connection
import pg8000

INSTANCE_CONNECTION_NAME = os.getenv('INSTANCE_CONNECTION_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_NAME = os.getenv('DB_NAME')

BQ_DATASET = os.getenv('BQ_DATASET', 'raw')
BQ_TABLE = os.getenv('BQ_POSTGRES_TABLE', 'users')
PROJECT_ID = os.getenv('GCP_PROJECT_ID')

connector = Connector()

def get_conn() -> Connection:
    conn: Connection = connector.connect(
        INSTANCE_CONNECTION_NAME,
        "pg8000",
        user=DB_USER,
        password=DB_PASSWORD,
        db=DB_NAME
    )
    return conn

def get_last_run_timestamp(client: bigquery.Client) -> pd.Timestamp:
    query = f"""
    SELECT MAX(created_at) FROM `{PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}`
    """
    try:
        query_job = client.query(query)
        result = query_job.result()
        for row in result:
            return row[0]
    except Exception as e:
        print(f"{e}")
        return None

def load_data_from_postgres():
    bq_client = bigquery.Client()
    
    last_run_timestamp = get_last_run_timestamp(bq_client)
    
    conn = get_conn()
    
    if last_run_timestamp:
        query = f"SELECT * FROM users WHERE created_at > '{last_run_timestamp}' ORDER BY created_at ASC"
    else:
        query = "SELECT * FROM users"

    df = pd.read_sql(query, conn)
    
    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        write_disposition="WRITE_APPEND"
    )

    table_id = f"{PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"
    job = bq_client.load_table_from_dataframe(
        df, table_id, job_config=job_config
    )
    job.result()
    print(f"load {job.output_rows} in {table_id}")
    conn.close()

if __name__ == "__main__":
    load_data_from_postgres()