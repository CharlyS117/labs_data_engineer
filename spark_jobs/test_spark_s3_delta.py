from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
import os

def create_spark_session():
    # Endpoints y credenciales (usando variables o defaults de LocalStack)
    s3_endpoint = os.getenv("AWS_ENDPOINT_URL", "http://localstack:4566")
    aws_access_key = os.getenv("AWS_ACCESS_KEY_ID", "test")
    aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "test")

    return (
        SparkSession.builder
        .appName("DeltaS3Test")
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,io.delta:delta-spark_2.12:3.1.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # Configuración S3A para LocalStack / S3 Mock
        .config("spark.hadoop.fs.s3a.endpoint", s3_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", aws_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", aws_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )

def main():
    print("Iniciando sesión de Apache Spark con soporte Delta Lake y S3A...")
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # Datos de prueba para validar Delta Lake
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("producto", StringType(), False),
        StructField("categoria", StringType(), True),
        StructField("precio", DoubleType(), False)
    ])

    data = [
        (1, "Laptop Pro 16", "Computo", 1850.00),
        (2, "Mouse Ergonomico", "Accesorios", 45.50),
        (3, "Teclado Mecanico RGB", "Accesorios", 120.00),
        (4, "Monitor 4K 27in", "Pantallas", 380.00),
        (5, "Hub USB-C Dual", "Accesorios", 35.00)
    ]

    df = spark.createDataFrame(data, schema)
    print("DataFrame original creado:")
    df.show()

    delta_path = "s3a://raw-data/delta_test/ventas_delta"
    print(f"Escribiendo DataFrame en formato Delta en: {delta_path}")
    df.write.format("delta").mode("overwrite").save(delta_path)
    print("Escritura en formato Delta completada con éxito.")

    print(f"Leyendo tabla Delta desde S3: {delta_path}")
    df_read = spark.read.format("delta").load(delta_path)
    print("DataFrame leído desde Delta Lake en S3:")
    df_read.show()

    total_records = df_read.count()
    print(f"Validación exitosa: Se leyeron {total_records} registros correctamente desde Delta Lake.")
    spark.stop()

if __name__ == "__main__":
    main()

