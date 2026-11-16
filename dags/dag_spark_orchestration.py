from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import os

default_args = {
    "owner": "airflow",
    "start_date": datetime(2026, 1, 1),
}

with DAG(
    dag_id="dag_spark_orchestration",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["spark", "delta", "s3", "bigdata"],
) as dag:

    submit_delta_s3_job = SparkSubmitOperator(
        task_id="submit_spark_delta_s3",
        conn_id="spark_default",
        application="/opt/airflow/spark_jobs/test_spark_s3_delta.py",
        packages="org.apache.hadoop:hadoop-aws:3.3.4,io.delta:delta-spark_2.12:3.1.0",
        conf={
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            "spark.hadoop.fs.s3a.endpoint": os.getenv("AWS_ENDPOINT_URL", "http://localstack:4566"),
            "spark.hadoop.fs.s3a.access.key": os.getenv("AWS_ACCESS_KEY_ID", "test"),
            "spark.hadoop.fs.s3a.secret.key": os.getenv("AWS_SECRET_ACCESS_KEY", "test"),
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
        },
        verbose=True,
    )

    submit_delta_s3_job

