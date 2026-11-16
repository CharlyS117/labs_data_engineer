# Data Engineering Labs 🚀

Entorno profesional para prácticas y laboratorios de ingeniería de datos usando **Apache Airflow**, **PostgreSQL**, **LocalStack (AWS S3)**, **dbt (Data Build Tool)**, **Apache Spark (PySpark + Delta Lake)** y **Apache Kafka (Streaming)** sobre Docker.

---

## 🏗️ Arquitectura de Servicios

| Servicio | Contenedor | Puerto Host | Descripción | Variables Configurables |
| :--- | :--- | :--- | :--- | :--- |
| **Apache Airflow** | `airflow_webserver` | `8888` | Orquestador Web & Scheduler | `${AIRFLOW_WEBSERVER_HOST_PORT}`, `${AIRFLOW_ADMIN_USER}` |
| **PostgreSQL** | `airflow_postgres` | `5433` | Metastore de Airflow y Data Warehouse | `${POSTGRES_HOST_PORT}`, `${POSTGRES_USER}`, `${POSTGRES_DB}` |
| **LocalStack (S3)** | `localstack_s3` | `4566` | Emulador oficial de AWS S3 | `${LOCALSTACK_HOST_PORT}`, `${AWS_ACCESS_KEY_ID}` |
| **Spark Master** | `spark_master` | `7077`, `8081` | Clúster Master y Spark Web UI | `${SPARK_MASTER_PORT}`, `${SPARK_UI_PORT}` |
| **Spark Worker** | `spark_worker` | Interno | Worker del clúster Spark | `${SPARK_WORKER_MEMORY}`, `${SPARK_WORKER_CORES}` |
| **Apache Kafka** | `kafka_broker` | `9092`, `29092` | Broker en modo KRaft (sin Zookeeper) | `${KAFKA_BROKER_PORT}`, `${KAFKA_HOST_PORT}` |
| **Kafka UI** | `kafka_ui` | `8082` | Consola Web para inspeccionar tópicos | `${KAFKA_UI_HOST_PORT}` |

---

## 🌐 URLs y Consolas Web de Acceso

- **Airflow Web UI:** [http://localhost:8888](http://localhost:8888) (`admin` / `admin`).
- **Spark Master UI:** [http://localhost:8081](http://localhost:8081) (supervisión de jobs y workers).
- **Kafka Web UI:** [http://localhost:8082](http://localhost:8082) (explorador visual de tópicos y mensajes).
- **PostgreSQL (DBeaver):** `localhost:5433` (`airflow` / `airflow` en BD `airflow`).
- **LocalStack S3:** `http://localhost:4566` (navegable con Cyberduck / MobaXterm).

---

## ⚙️ Conexiones Automatizadas en Airflow

Al levantar el contenedor, Airflow auto-configura las siguientes conexiones:
1. **`aws_default`**: Apuntando a LocalStack (`http://localstack:4566`, key: `test`, secret: `test`).
2. **`spark_default`**: Tipo Spark (`spark://spark-master:7077`) para operadores como `SparkSubmitOperator`.

---

## 📁 Estructura del Proyecto

```plaintext
labs_data_engineer/
├── dags/
│   ├── dag_mobility_elt.py        # Pipeline ELT producción: GBFS Ecobici -> LocalStack S3 -> Postgres -> dbt
│   ├── elt_ecommerce_pipeline.py  # Pipeline ELT E-commerce con sensor S3
│   ├── dag_spark_orchestration.py # Orquestación Spark con SparkSubmitOperator y Delta Lake
│   ├── s3_to_postgres_dag.py      # Pipeline S3 a PostgreSQL con upsert
│   └── test_s3_dag.py             # DAG base de verificación S3
├── spark_jobs/
│   └── test_spark_s3_delta.py     # Script PySpark con soporte Delta Lake y S3A
├── streaming/
│   ├── producer_test.py           # Productor de eventos transaccionales con Confluent Kafka
│   └── consumer_test.py           # Consumidor de eventos transaccionales
├── dbt_project/                   # Proyecto dbt con profiles.yml parametrizado
│   ├── models/
│   │   ├── staging/               # Limpieza, tipado y validaciones (stg_station_status, stg_orders)
│   │   └── marts/                 # Modelos analíticos finales (fct_station_kpis, fct_customer_orders)
│   └── dbt_project.yml
├── Dockerfile.airflow             # Imagen customizada de Airflow con OpenJDK 17, dbt, Spark y Kafka
├── docker-compose.yaml            # Stack Docker ampliado y parametrizado vía .env
├── .env.example                   # Plantilla de variables para producción/desarrollo
└── README.md                      # Documentación del proyecto
```

---

## ⚡ Guía de Inicio Rápido

### 1. Construir y Levantar el Entorno Completo
```bash
docker compose up -d --build
```

### 2. Probar Kafka Streaming (Productor y Consumidor)
Puedes probar el flujo de eventos en tiempo real desde el contenedor de Airflow o desde tu máquina:

```bash
# Enviar 5 eventos simulados
docker exec airflow_webserver python3 /opt/airflow/streaming/producer_test.py

# Consumir los 5 eventos
docker exec airflow_webserver python3 /opt/airflow/streaming/consumer_test.py
```
*(También puedes entrar a [http://localhost:8082](http://localhost:8082) y ver el tópico `eventos_transacciones` con sus mensajes).*

### 3. Probar Procesamiento Distribuido (Spark + Delta Lake en S3)
Ejecuta el job directamente en el clúster Spark:

```bash
docker exec spark_master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.hadoop:hadoop-aws:3.3.4,io.delta:delta-spark_2.12:3.1.0 \
  /opt/spark/jobs/test_spark_s3_delta.py
```
O activa el DAG **`dag_spark_orchestration`** desde la interfaz web de Airflow en [http://localhost:8888](http://localhost:8888).
