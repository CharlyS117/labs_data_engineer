import os
from datetime import datetime
import json
import requests
import pandas as pd
import boto3
from sqlalchemy import create_engine

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# Parámetros desde variables de entorno
S3_ENDPOINT = os.getenv("AWS_ENDPOINT_URL", "http://localstack:4566")
S3_BUCKET = os.getenv("S3_BUCKET_NAME", "raw-data")
AWS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "test")
AWS_SECRET = os.getenv("AWS_SECRET_ACCESS_KEY", "test")
AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")

PG_USER = os.getenv("POSTGRES_USER", "airflow")
PG_PASS = os.getenv("POSTGRES_PASSWORD", "airflow")
PG_HOST = os.getenv("POSTGRES_HOST", "postgres")
PG_PORT = os.getenv("POSTGRES_PORT", "5432")
PG_DB = os.getenv("POSTGRES_DB", "airflow")

GBFS_STATUS_URL = "https://gbfs.mex.lyftbikes.com/gbfs/es/station_status.json"

def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=AWS_KEY,
        aws_secret_access_key=AWS_SECRET,
        region_name=AWS_REGION
    )

def extract_gbfs_to_s3(**context):
    response = requests.get(GBFS_STATUS_URL, timeout=30)
    response.raise_for_status()
    payload = response.json()
    
    stations_data = payload.get("data", {}).get("stations", [])
    s3 = get_s3_client()

    # Crear bucket si no existe
    buckets = [b["Name"] for b in s3.list_buckets().get("Buckets", [])]
    if S3_BUCKET not in buckets:
        s3.create_bucket(Bucket=S3_BUCKET)

    key = f"mobility/ecobici/status/fecha={context['ds']}/run_{context['ts_nodash']}.json"
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=json.dumps(stations_data)
    )
    # Compartir ruta vía XCom
    context['task_instance'].xcom_push(key="s3_file_key", value=key)

def load_s3_to_postgres(**context):
    ti = context['task_instance']
    key = ti.xcom_pull(task_ids="extract_gbfs_to_s3", key="s3_file_key")
    
    s3 = get_s3_client()
    obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
    records = json.loads(obj["Body"].read().decode("utf-8"))

    # Normalizar para PostgreSQL
    df = pd.DataFrame(records)
    df["extracted_at"] = datetime.utcnow()
    
    # Mantener columnas analíticas necesarias
    cols_to_keep = [
        "station_id", "num_bikes_available", "num_ebikes_available",
        "num_docks_available", "is_installed", "is_renting",
        "is_returning", "last_reported", "extracted_at"
    ]
    df_filtered = df[[c for c in cols_to_keep if c in df.columns]]

    engine = create_engine(f"postgresql+psycopg2://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}")
    with engine.begin() as conn:
        conn.exec_driver_sql("CREATE TABLE IF NOT EXISTS public.raw_station_status (station_id TEXT, num_bikes_available BIGINT, num_docks_available BIGINT, is_installed BIGINT, is_renting BIGINT, is_returning BIGINT, last_reported BIGINT, extracted_at TIMESTAMP);")
        conn.exec_driver_sql("TRUNCATE TABLE public.raw_station_status;")
        df_filtered.to_sql("raw_station_status", con=conn, schema="public", if_exists="append", index=False)
    print(f"Ingestados {len(df_filtered)} registros en public.raw_station_status")

with DAG(
    dag_id="pipeline_movilidad_urbana_elt",
    start_date=datetime(2026, 1, 1),
    schedule="@hourly",
    catchup=False,
    tags=["movilidad", "gbfs", "s3", "dbt"]
) as dag:

    task_extract = PythonOperator(
        task_id="extract_gbfs_to_s3",
        python_callable=extract_gbfs_to_s3
    )

    task_load = PythonOperator(
        task_id="load_s3_to_postgres",
        python_callable=load_s3_to_postgres
    )

    task_dbt_run = BashOperator(
        task_id="dbt_run_models",
        bash_command="cd /opt/airflow/dbt_project && dbt run --profiles-dir ."
    )

    task_dbt_test = BashOperator(
        task_id="dbt_test_models",
        bash_command="cd /opt/airflow/dbt_project && dbt test --profiles-dir ."
    )

    task_extract >> task_load >> task_dbt_run >> task_dbt_test
