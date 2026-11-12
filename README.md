# Data Engineering Labs 🚀

Entorno profesional para prácticas y laboratorios de ingeniería de datos usando **Apache Airflow**, **PostgreSQL**, **LocalStack (AWS S3)** y **dbt (Data Build Tool)** sobre Docker.

---

## 🏗️ Arquitectura de Servicios

| Servicio | Contenedor | Puerto Host | Descripción | Variables Configurables |
| :--- | :--- | :--- | :--- | :--- |
| **Apache Airflow** | `airflow_webserver` | `8888` | Orquestador Web & Scheduler | `${AIRFLOW_WEBSERVER_HOST_PORT}`, `${AIRFLOW_ADMIN_USER}` |
| **LocalStack (S3)** | `localstack_s3` | `4566` | Emulador oficial de AWS S3 | `${LOCALSTACK_HOST_PORT}`, `${AWS_ACCESS_KEY_ID}` |
| **PostgreSQL** | `airflow_postgres` | `5433` | Metastore de Airflow y Data Warehouse | `${POSTGRES_HOST_PORT}`, `${POSTGRES_USER}`, `${POSTGRES_DB}` |

---

## ⚙️ Parametrización y Variables de Entorno

El proyecto está 100% desacoplado de credenciales hardcodeadas siguiendo el estándar **12-Factor App**:

1. Copia la plantilla de configuración:
   ```bash
   cp .env.example .env
   ```
2. Modifica los parámetros en `.env` según tus necesidades (puertos, usuarios, contraseñas, bucket names).

---

## 📁 Estructura del Proyecto

```plaintext
labs_data_engineer/
├── dags/
│   ├── dag_mobility_elt.py        # Pipeline ELT producción: GBFS Ecobici -> LocalStack S3 -> Postgres -> dbt
│   ├── elt_ecommerce_pipeline.py  # Pipeline ELT E-commerce con sensor S3
│   ├── s3_to_postgres_dag.py      # Pipeline S3 a PostgreSQL con upsert
│   └── test_s3_dag.py             # DAG base de verificación S3
├── dbt_project/                   # Proyecto dbt con profiles.yml parametrizado
│   ├── models/
│   │   ├── staging/               # Limpieza, tipado y validaciones (stg_station_status, stg_orders)
│   │   └── marts/                 # Modelos analíticos finales (fct_station_kpis, fct_customer_orders)
│   └── dbt_project.yml
├── docker-compose.yaml            # Stack Docker totalmente parametrizado vía .env
├── .env.example                   # Plantilla de variables para producción/desarrollo
└── README.md                      # Documentación del proyecto
```

---

## ⚡ Guía de Inicio Rápido

### 1. Levantar el entorno
```bash
docker compose up -d
```

### 2. Acceso a las Interfaces
- **Apache Airflow:** [http://localhost:8888](http://localhost:8888) (`admin` / `admin` o según tu `.env`).
- **PostgreSQL (DBeaver / DataGrip):**
  - **Host:** `localhost` | **Puerto:** `5433`
  - **Usuario:** `airflow` | **Contraseña:** `airflow` | **BD:** `airflow`
- **S3 (LocalStack):** `http://localhost:4566` (navegable con Cyberduck o MobaXterm).

---

## 📊 Pipelines Implementados

### 1. Movilidad Urbana ELT (`pipeline_movilidad_urbana_elt`)
- **Extracción:** Consulta en tiempo real a la API de Ecobici GBFS de CDMX.
- **Data Lake (S3):** Guarda snapshots crudos en `s3://${S3_BUCKET_NAME}/mobility/ecobici/status/fecha=YYYY-MM-DD/run_*.json`.
- **Ingesta:** Carga eficiente en `public.raw_station_status`.
- **Transformación dbt:** 
  - `stg_station_status`: Limpieza de tipos, cálculo de `occupancy_rate` protegido ante divisiones por cero.
  - `fct_station_kpis`: Clasificación operativa en tiempo real (`HEALTHY_STOCK`, `CRITICAL_LOW_STOCK`, `OUT_OF_SERVICE`, `CRITICAL_HIGH_CONGESTION`).
- **Tests de Calidad:** Pruebas automatizadas de no nulos y unicidad.

### 2. E-Commerce Pipeline (`elt_ecommerce_pipeline`)
- Espera eventos con `S3KeySensor`.
- Carga `orders` y `customers`.
- Compila `fct_customer_orders` con total gastado y métricas de clientes.
