from datetime import datetime
import csv
import io
import boto3
import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator

def extract_from_s3_and_load_to_postgres():
    # 1. Conexión a LocalStack S3
    s3 = boto3.client(
        "s3",
        endpoint_url="http://localstack:4566",
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1",
    )
    
    bucket_name = "raw-data"
    object_key = "ventas/fecha=2026-11-08/datos.csv"
    
    print(f"Descargando archivo {object_key} desde el bucket {bucket_name}...")
    response = s3.get_object(Bucket=bucket_name, Key=object_key)
    csv_content = response["Body"].read().decode("utf-8")
    
    # 2. Parsear CSV en memoria
    csv_file = io.StringIO(csv_content)
    reader = csv.DictReader(csv_file)
    rows_to_insert = [(int(row["id"]), row["producto"], float(row["precio"])) for row in reader]
    print(f"Registros leídos: {len(rows_to_insert)}")
    
    # 3. Cargar datos en PostgreSQL
    conn = psycopg2.connect("postgresql://airflow:airflow@postgres:5432/airflow")
    cur = conn.cursor()
    
    # Crear tabla de destino si no existe
    cur.execute("""
        CREATE TABLE IF NOT EXISTS public.raw_ventas (
            id INT PRIMARY KEY,
            producto VARCHAR(50),
            precio NUMERIC,
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    
    # Insertar o actualizar (UPSERT)
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

