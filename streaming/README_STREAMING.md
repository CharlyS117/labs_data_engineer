# Guía de Ejecución: Pipeline Streaming & Lakehouse (Reddit + Kafka + Spark + Delta Lake) 🚀

Este laboratorio implementa una arquitectura completa de **Lakehouse Streaming & Batch**:
- **Ingesta continua:** Productor Python consumiendo la API de Reddit y emitiendo a Apache Kafka (KRaft).
- **Procesamiento Streaming:** PySpark Structured Streaming escribiendo micro-lotes en formato **Delta Lake** sobre S3.
- **Procesamiento Batch:** PySpark deduplicando con Window Functions, agregando KPIs y escribiendo en **PostgreSQL** vía JDBC.
- **Orquestación:** DAG en Apache Airflow programado cada hora con `SparkSubmitOperator`.

---

## 🏗️ Servicios y Puertos

| Servicio | Contenedor | URL / Puerto | Función |
| :--- | :--- | :--- | :--- |
| **Apache Kafka Broker** | `kafka_broker` | `kafka:9092` (interno), `localhost:29092` (externo) | Streaming de eventos |
| **Kafka UI** | `kafka_ui` | [http://localhost:8082](http://localhost:8082) | Inspección visual de tópicos y mensajes |
| **Spark Master UI** | `spark_master` | [http://localhost:8081](http://localhost:8081) | Monitoreo de Workers y Jobs activos |
| **Apache Airflow** | `airflow_webserver` | [http://localhost:8888](http://localhost:8888) | Orquestador |
| **LocalStack S3** | `localstack_s3` | `http://localhost:4566` | Data Lake (Delta Lake & Checkpoints) |
| **PostgreSQL** | `airflow_postgres` | `localhost:5433` (externo) | Consolidado analítico |

---

## ⚡ Paso a Paso de Ejecución y Validación

### Paso 1: Iniciar el Productor de Reddit hacia Kafka
Ejecuta el productor en segundo plano o en una terminal separada:

```bash
docker exec -d airflow_webserver python3 /opt/airflow/streaming/reddit_producer.py
```
Para ver los logs en tiempo real:
```bash
docker exec airflow_webserver tail -f /tmp/reddit_producer.log  # o ejecutarlo en primer plano
```
*Abre [http://localhost:8082](http://localhost:8082) en tu navegador para ver el tópico `reddit_posts_stream` recibiendo eventos.*

---

### Paso 2: Ejecutar el Job de Streaming (Kafka -> Delta Lake en S3)
Envía el job estructurado al clúster de Spark:

```bash
docker exec spark_master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.hadoop:hadoop-aws:3.3.4,io.delta:delta-spark_2.12:3.1.0 \
  /opt/spark/jobs/stream_reddit_to_delta.py
```
*Abre [http://localhost:8081](http://localhost:8081) para verificar que el job `RedditStreamingToDelta` esté en estado `RUNNING`.*

---

### Paso 3: Ejecutar el Job Batch de Agregación
Una vez que hayan entrado micro-lotes en Delta Lake, puedes disparar la consolidación de KPIs:

**Opción A: Vía Airflow (Recomendado)**
- Ingresa a [http://localhost:8888](http://localhost:8888).
- Activa el DAG **`dag_reddit_lakehouse_batch`** y presiona **Trigger DAG**.

**Opción B: Vía terminal con `spark-submit`**
```bash
docker exec spark_master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.hadoop:hadoop-aws:3.3.4,io.delta:delta-spark_2.12:3.1.0,org.postgresql:postgresql:42.7.1 \
  /opt/spark/jobs/batch_aggregate_reddit.py
```

---

### Paso 4: Consultar los Resultados en PostgreSQL
Desde tu editor SQL en **DBeaver** (puerto `5433`, usuario/db `airflow`) ejecuta:

```sql
SELECT * FROM public.reddit_subreddit_kpis ORDER BY total_unique_posts DESC;
```
Verás las métricas consolidadas: `subreddit`, `ingestion_date`, `total_unique_posts`, `avg_score`, `total_comments`, `avg_upvote_ratio` y `avg_title_length`.

