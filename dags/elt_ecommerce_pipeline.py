import os
from datetime import datetime
from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import boto3
import pandas as pd
from sqlalchemy import create_engine
import io

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

def load_s3_to_postgres(bucket: str, key: str, table_name: str):
    s3 = boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=AWS_KEY,
        aws_secret_access_key=AWS_SECRET,
        region_name=AWS_REGION,
    )
    obj = s3.get_object(Bucket=bucket, Key=key)
    df = pd.read_csv(io.BytesIO(obj["Body"].read()))

    engine = create_engine(f"postgresql+psycopg2://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}")
    with engine.begin() as conn:
        df.to_sql(table_name, con=conn, schema="public", if_exists="replace", index=False)
    print(f"Tabla {table_name} cargada con {len(df)} registros.")

with DAG(
    dag_id="elt_ecommerce_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["s3", "dbt", "elt"],
) as dag:

    wait_orders = S3KeySensor(
        task_id="wait_for_orders_s3",
        bucket_name=S3_BUCKET,
        bucket_key="orders/orders.csv",
        aws_conn_id="aws_default",
        poke_interval=10,
        timeout=120,
    )

    load_orders = PythonOperator(
        task_id="load_orders_to_raw",
        python_callable=load_s3_to_postgres,
        op_kwargs={"bucket": S3_BUCKET, "key": "orders/orders.csv", "table_name": "raw_orders"},
    )

    load_customers = PythonOperator(
        task_id="load_customers_to_raw",
        python_callable=load_s3_to_postgres,
        op_kwargs={"bucket": S3_BUCKET, "key": "customers/customers.csv", "table_name": "raw_customers"},
    )

    run_dbt = BashOperator(
        task_id="run_dbt_models",
        bash_command="cd /opt/airflow/dbt_project && dbt run --profiles-dir .",
    )

    test_dbt = BashOperator(
        task_id="test_dbt_models",
        bash_command="cd /opt/airflow/dbt_project && dbt test --profiles-dir .",
    )

    wait_orders >> [load_orders, load_customers] >> run_dbt >> test_dbt
