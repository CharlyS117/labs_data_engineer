from datetime import datetime
from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import boto3
import pandas as pd
from sqlalchemy import create_engine
import io

def load_s3_to_postgres(bucket: str, key: str, table_name: str):
    # Conexión al S3 local
    s3 = boto3.client(
        "s3",
        endpoint_url="http://localstack:4566",
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1",
    )
    obj = s3.get_object(Bucket=bucket, Key=key)
    df = pd.read_csv(io.BytesIO(obj["Body"].read()))

    # Carga a Postgres de Airflow (esquema crudo)
    engine = create_engine("postgresql+psycopg2://airflow:airflow@postgres:5432/airflow")
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

    # 1. Sensor: Esperar a que el archivo de órdenes exista en S3
    wait_orders = S3KeySensor(
        task_id="wait_for_orders_s3",
        bucket_name="raw-data",
        bucket_key="orders/orders.csv",
        aws_conn_id="aws_default",
        poke_interval=10,
        timeout=120,
    )

    # 2. Cargar orders a Postgres
    load_orders = PythonOperator(
        task_id="load_orders_to_raw",
        python_callable=load_s3_to_postgres,
        op_kwargs={"bucket": "raw-data", "key": "orders/orders.csv", "table_name": "raw_orders"},
    )

    # 3. Cargar customers a Postgres
    load_customers = PythonOperator(
        task_id="load_customers_to_raw",
        python_callable=load_s3_to_postgres,
        op_kwargs={"bucket": "raw-data", "key": "customers/customers.csv", "table_name": "raw_customers"},
    )

    # 4. Transformar con dbt
    run_dbt = BashOperator(
        task_id="run_dbt_models",
        bash_command="cd /opt/airflow/dbt_project && dbt run --profiles-dir .",
    )

    # 5. Probar calidad de datos con dbt test
    test_dbt = BashOperator(
        task_id="test_dbt_models",
        bash_command="cd /opt/airflow/dbt_project && dbt test --profiles-dir .",
    )

    wait_orders >> [load_orders, load_customers] >> run_dbt >> test_dbt