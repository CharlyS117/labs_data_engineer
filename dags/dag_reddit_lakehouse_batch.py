import os
from datetime import datetime
import boto3
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.exceptions import AirflowSkipException

S3_ENDPOINT = os.getenv("AWS_ENDPOINT_URL", "http://localstack:4566")
AWS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "test")
AWS_SECRET = os.getenv("AWS_SECRET_ACCESS_KEY", "test")
AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "raw-data")
DELTA_PREFIX = "delta/reddit_posts/"

def check_delta_data_exists():
    s3 = boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=AWS_KEY,
        aws_secret_access_key=AWS_SECRET,
        region_name=AWS_REGION
    )
    
    response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=DELTA_PREFIX, MaxKeys=5)
    contents = response.get("Contents", [])
    if not contents:
        raise AirflowSkipException(f"No hay datos aún en s3://{BUCKET_NAME}/{DELTA_PREFIX}. Se omite ejecución batch.")
    print(f"Validación exitosa: se encontraron {len(contents)} objetos en s3://{BUCKET_NAME}/{DELTA_PREFIX}")

with DAG(
    dag_id="dag_reddit_lakehouse_batch",
    start_date=datetime(2026, 1, 1),
    schedule="@hourly",
    catchup=False,
    tags=["reddit", "spark", "delta", "postgres", "lakehouse"],
) as dag:

    validate_delta_s3 = PythonOperator(
        task_id="check_delta_posts_exist",
        python_callable=check_delta_data_exists,
    )

    submit_spark_aggregation = SparkSubmitOperator(
        task_id="spark_aggregate_reddit_posts",
        conn_id="spark_default",
        application="/opt/airflow/spark_jobs/batch_aggregate_reddit.py",
        packages="org.apache.hadoop:hadoop-aws:3.3.4,io.delta:delta-spark_2.12:3.1.0,org.postgresql:postgresql:42.7.1",
        conf={
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            "spark.hadoop.fs.s3a.endpoint": S3_ENDPOINT,
            "spark.hadoop.fs.s3a.access.key": AWS_KEY,
            "spark.hadoop.fs.s3a.secret.key": AWS_SECRET,
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
        },
        verbose=True,
    )

    validate_delta_s3 >> submit_spark_aggregation

