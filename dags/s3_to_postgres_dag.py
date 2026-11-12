import os
from datetime import datetime
import csv
import io
import boto3
import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator

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

def extract_from_s3_and_load_to_postgres():
    s3 = boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=AWS_KEY,
        aws_secret_access_key=AWS_SECRET,
        region_name=AWS_REGION,
    )
    
    object_key = "ventas/fecha=2026-11-08/datos.csv"
    print(f"Descargando archivo {object_key} desde el bucket {S3_BUCKET}...")
    response = s3.get_object(Bucket=S3_BUCKET, Key=object_key)
    csv_content = response["Body"].read().decode("utf-8")
    
    csv_file = io.StringIO(csv_content)
    reader = csv.DictReader(csv_file)
    rows_to_insert = [(int(row["id"]), row["producto"], float(row["precio"])) for row in reader]
    print(f"Registros leídos: {len(rows_to_insert)}")
    
    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        user=PG_USER,
        password=PG_PASS,
        database=PG_DB
    )
    cur = conn.cursor()
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS public.raw_ventas (
            id INT PRIMARY KEY,
            producto VARCHAR(50),
            precio NUMERIC,
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    
    insert_query = """
        INSERT INTO public.raw_ventas (id, producto, precio)
        VALUES (%s, %s, %s)
        ON CONFLICT (id) DO UPDATE SET
            producto = EXCLUDED.producto,
            precio = EXCLUDED.precio,
            loaded_at = CURRENT_TIMESTAMP;
    """
    cur.executemany(insert_query, rows_to_insert)
    conn.commit()
    cur.close()
    conn.close()
    print("Datos insertados con éxito en la tabla public.raw_ventas.")

with DAG(
    dag_id="02_s3_to_postgres_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["elt", "s3", "postgres", "lab"],
) as dag:

    s3_a_postgres = PythonOperator(
        task_id="cargar_s3_hacia_postgres",
        python_callable=extract_from_s3_and_load_to_postgres,
    )
