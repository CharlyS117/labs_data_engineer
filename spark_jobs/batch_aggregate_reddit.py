import os
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number, count, avg, sum, round as spark_round
from delta.tables import DeltaTable

def main():
    s3_endpoint = os.getenv("AWS_ENDPOINT_URL", "http://localstack:4566")
    aws_access_key = os.getenv("AWS_ACCESS_KEY_ID", "test")
    aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "test")
    
    pg_host = os.getenv("POSTGRES_HOST", "postgres")
    pg_port = os.getenv("POSTGRES_PORT", "5432")
    pg_db = os.getenv("POSTGRES_DB", "airflow")
    pg_user = os.getenv("POSTGRES_USER", "airflow")
    pg_pass = os.getenv("POSTGRES_PASSWORD", "airflow")

    delta_path = "s3a://raw-data/delta/reddit_posts/"

    print("Iniciando PySpark Batch Aggregation (Delta Lake -> Postgres)...")
    spark = (
        SparkSession.builder
        .appName("RedditDeltaBatchAggregation")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # S3A Config
        .config("spark.hadoop.fs.s3a.endpoint", s3_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", aws_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", aws_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print(f"Leyendo tabla Delta en: {delta_path}")
    raw_delta_df = spark.read.format("delta").load(delta_path)

    # 1. Deduplicar por 'id' conservando la versión más reciente según 'ingested_at'
    window_spec = Window.partitionBy("id").orderBy(col("ingested_at").desc())
    dedup_df = (
        raw_delta_df
        .withColumn("row_num", row_number().over(window_spec))
        .filter(col("row_num") == 1)
        .drop("row_num")
    )

    # 2. Calcular agregaciones por subreddit e ingestion_date
    print("Calculando KPIs por subreddit e ingestion_date...")
    kpi_df = (
        dedup_df
        .groupBy("subreddit", "ingestion_date")
        .agg(
            count("id").alias("total_unique_posts"),
            spark_round(avg("score"), 2).alias("avg_score"),
            sum("num_comments").alias("total_comments"),
            spark_round(avg("upvote_ratio"), 3).alias("avg_upvote_ratio"),
            spark_round(avg("title_length"), 1).alias("avg_title_length")
        )
    )

    print("Resumen de KPIs generados:")
    kpi_df.show(truncate=False)

    # 3. Escribir resultado en PostgreSQL mediante JDBC
    jdbc_url = f"jdbc:postgresql://{pg_host}:{pg_port}/{pg_db}"
    db_properties = {
        "user": pg_user,
        "password": pg_pass,
        "driver": "org.postgresql.Driver"
    }

    target_table = "public.reddit_subreddit_kpis"
    print(f"Guardando resultados consolidados en PostgreSQL: {jdbc_url} ({target_table})")
    (
        kpi_df.write
        .mode("overwrite")
        .jdbc(url=jdbc_url, table=target_table, properties=db_properties)
    )
    print("Guardado en PostgreSQL completado.")

    # 4. Limpieza (Vacuum) de versiones antiguas en Delta Lake (168 horas = 7 días de retención)
    try:
        print("Ejecutando vacuum en Delta Lake...")
        spark.conf.set("spark.databricks.delta.vacuum.parallelDelete.enabled", "true")
        delta_table = DeltaTable.forPath(spark, delta_path)
        delta_table.vacuum(168)
        print("Vacuum finalizado con éxito.")
    except Exception as e:
        print(f"Aviso durante el vacuum (normal en tablas recién creadas): {e}")

    spark.stop()
    print("Job batch finalizado exitosamente.")

if __name__ == "__main__":
    main()

