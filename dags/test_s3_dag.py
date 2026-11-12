import os
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import boto3

S3_ENDPOINT = os.getenv("AWS_ENDPOINT_URL", "http://localstack:4566")
S3_BUCKET = os.getenv("S3_BUCKET_NAME", "raw-data")
AWS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "test")
AWS_SECRET = os.getenv("AWS_SECRET_ACCESS_KEY", "test")
AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")

def setup_s3_bucket():
    s3 = boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=AWS_KEY,
        aws_secret_access_key=AWS_SECRET,
        region_name=AWS_REGION,
    )
    
    # Crear bucket si no existe
    buckets = [b["Name"] for b in s3.list_buckets().get("Buckets", [])]
    if S3_BUCKET not in buckets:
        s3.create_bucket(Bucket=S3_BUCKET)
        print(f"Bucket {S3_BUCKET} creado con éxito.")
    
    # Subir un archivo de prueba
    s3.put_object(
        Bucket=S3_BUCKET,
        Key="ventas/fecha=2026-11-08/datos.csv",
        Body="id,producto,precio\n1,laptop,1200\n2,mouse,25\n",
    )
    print("Archivo de prueba subido exitosamente a S3.")

with DAG(
    dag_id="01_setup_local_s3",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["localstack", "s3", "lab"],
) as dag:

    crear_datos = PythonOperator(
        task_id="crear_bucket_y_datos",
        python_callable=setup_s3_bucket,
    )
