import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, to_date, length
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, BooleanType

def main():
    s3_endpoint = os.getenv("AWS_ENDPOINT_URL", "http://localstack:4566")
    aws_access_key = os.getenv("AWS_ACCESS_KEY_ID", "test")
    aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "test")
    kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    topic_name = os.getenv("KAFKA_TOPIC_REDDIT", "reddit_posts_stream")

    delta_target_path = "s3a://raw-data/delta/reddit_posts/"
    checkpoint_path = "s3a://raw-data/checkpoints/reddit_posts/"

    print("Iniciando PySpark Structured Streaming (Kafka -> Delta Lake S3)...")
    spark = (
        SparkSession.builder
        .appName("RedditStreamingToDelta")
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

    # Esquema JSON explícito del evento Reddit con link_flair_text, is_self, selftext completo
    reddit_schema = StructType([
        StructField("id", StringType(), False),
        StructField("subreddit", StringType(), False),
        StructField("link_flair_text", StringType(), True),
        StructField("is_self", BooleanType(), True),
        StructField("title", StringType(), True),
        StructField("selftext", StringType(), True),
        StructField("author", StringType(), True),
        StructField("score", LongType(), True),
        StructField("num_comments", LongType(), True),
        StructField("upvote_ratio", DoubleType(), True),
        StructField("over_18", BooleanType(), True),
        StructField("created_utc", DoubleType(), True),
        StructField("url", StringType(), True),
        StructField("ingested_at", StringType(), True),
    ])

    print(f"Leyendo streaming desde Kafka topic: '{topic_name}' en {kafka_servers}...")
    kafka_raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_servers)
        .option("subscribe", topic_name)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # Parsear y derivar columnas analíticas
    parsed_df = (
        kafka_raw_df
        .selectExpr("CAST(key AS STRING) as message_key", "CAST(value AS STRING) as json_value")
        .select(from_json(col("json_value"), reddit_schema).alias("data"))
        .select("data.*")
        .withColumn("event_timestamp", to_timestamp(col("created_utc")))
        .withColumn("ingestion_date", to_date(to_timestamp(col("created_utc"))))
        .withColumn("title_length", length(col("title")))
    )

    print(f"Escribiendo micro-lotes en Delta Lake: {delta_target_path}")
    streaming_query = (
        parsed_df.writeStream
        .format("delta")
        .outputMode("append")
        .partitionBy("ingestion_date")
        .option("checkpointLocation", checkpoint_path)
        .option("mergeSchema", "true")
        .trigger(processingTime="15 seconds")
        .start(delta_target_path)
    )

    streaming_query.awaitTermination()

if __name__ == "__main__":
    main()
