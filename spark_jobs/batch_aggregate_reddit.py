import os
import hashlib
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col, row_number, count, avg, sum, round as spark_round,
    lower, coalesce, length, trim, lit, md5, substring
)
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

    print("Iniciando PySpark Batch Curation & Aggregation (Delta Lake -> Postgres)...")
    spark = (
        SparkSession.builder
        .appName("RedditDeltaBatchCurationAndAggregation")
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

    if "link_flair_text" not in raw_delta_df.columns:
        raw_delta_df = raw_delta_df.withColumn("link_flair_text", lit(""))
    if "is_self" not in raw_delta_df.columns:
        raw_delta_df = raw_delta_df.withColumn("is_self", lit(True))

    # 1. Deduplicar por 'id' conservando la versión más reciente según 'ingested_at'
    window_id = Window.partitionBy("id").orderBy(col("ingested_at").desc())
    dedup_id_df = (
        raw_delta_df
        .withColumn("row_num", row_number().over(window_id))
        .filter(col("row_num") == 1)
        .drop("row_num")
    )

    # 2. Calcular agregaciones de KPIs por subreddit e ingestion_date
    print("Calculando KPIs por subreddit e ingestion_date...")
    kpi_df = (
        dedup_id_df
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

    # 3. Filtrar y Curar Publicaciones / Artículos Extensos (public.reddit_articles)
    print("Filtrando y deduplicando publicaciones por contenido global...")
    flair_clean = lower(coalesce(col("link_flair_text"), lit("")))
    selftext_clean = trim(coalesce(col("selftext"), lit("")))
    selftext_len = length(selftext_clean)

    # Regla 1: Contenido con cuerpo sustancial (>= 200 caracteres)
    rule_has_body = (col("selftext").isNotNull()) & (selftext_len >= 200)

    # Regla 2: Excluir publicaciones de spam o enlaces breves
    rule_not_spam = (
        ~flair_clean.contains("spam") &
        ~flair_clean.contains("promo") &
        ~flair_clean.contains("telegram") &
        ~flair_clean.contains("discord")
    )

    # Regla 3: Artículos técnicos, relatos o publicaciones explicativas sustanciales
    rule_is_article = (
        flair_clean.contains("article") |
        flair_clean.contains("tutorial") |
        flair_clean.contains("architecture") |
        flair_clean.contains("discussion") |
        flair_clean.contains("optimization") |
        flair_clean.contains("best practices") |
        flair_clean.contains("relato") |
        flair_clean.contains("historia") |
        flair_clean.contains("experiencia") |
        flair_clean.contains("confesion") |
        flair_clean.contains("confesión") |
        flair_clean.contains("anecdota") |
        flair_clean.contains("anécdota") |
        (
            (col("is_self") == True) &
            (selftext_len >= 250)
        )
    )

    filtered_df = dedup_id_df.filter(rule_has_body & rule_not_spam & rule_is_article)

    # DEDUPLICACIÓN GLOBAL CROSS-SUBREDDIT:
    # Si un mismo post o historia fue publicado en varios subreddits o múltiples veces,
    # agrupamos por el hash de los primeros 120 caracteres de texto normalizado o título.
    # Conservamos exactamente 1 único registro (el de mayor score o más reciente).
    window_content = (
        Window
        .partitionBy(lower(trim(col("title"))), substring(selftext_clean, 1, 100))
        .orderBy(col("score").desc(), col("event_timestamp").desc())
    )

    articles_df = (
        filtered_df
        .withColumn("content_row", row_number().over(window_content))
        .filter(col("content_row") == 1)
        .drop("content_row")
        .select(
            "id",
            "subreddit",
            "link_flair_text",
            "title",
            "selftext",
            "author",
            "score",
            "num_comments",
            "url",
            "event_timestamp",
            "ingestion_date"
        )
    )

    total_articles = articles_df.count()
    print(f"Total de artículos/publicaciones únicos curados y sin duplicados: {total_articles}")

    # 4. Escribir resultados en PostgreSQL mediante JDBC
    jdbc_url = f"jdbc:postgresql://{pg_host}:{pg_port}/{pg_db}"
    db_properties = {
        "user": pg_user,
        "password": pg_pass,
        "driver": "org.postgresql.Driver"
    }

    # Guardar KPIs
    print(f"Guardando KPIs en PostgreSQL ({jdbc_url} -> public.reddit_subreddit_kpis)...")
    kpi_df.write.mode("overwrite").jdbc(url=jdbc_url, table="public.reddit_subreddit_kpis", properties=db_properties)

    # Guardar Artículos (public.reddit_articles)
    print(f"Guardando publicaciones únicas en PostgreSQL ({jdbc_url} -> public.reddit_articles)...")
    articles_df.write.mode("overwrite").jdbc(url=jdbc_url, table="public.reddit_articles", properties=db_properties)

    # También actualizar public.reddit_relatos para retrocompatibilidad
    print(f"Sincronizando tabla public.reddit_relatos con contenido deduplicado...")
    articles_df.write.mode("overwrite").jdbc(url=jdbc_url, table="public.reddit_relatos", properties=db_properties)
    print("Guardado en PostgreSQL completado.")

    # 5. Limpieza (Vacuum)
    try:
        print("Ejecutando vacuum en Delta Lake...")
        spark.conf.set("spark.databricks.delta.vacuum.parallelDelete.enabled", "true")
        delta_table = DeltaTable.forPath(spark, delta_path)
        delta_table.vacuum(168)
        print("Vacuum finalizado con éxito.")
    except Exception as e:
        print(f"Aviso durante el vacuum: {e}")

    spark.stop()
    print("Job batch finalizado exitosamente.")

if __name__ == "__main__":
    main()
