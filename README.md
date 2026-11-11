# Data Engineering Labs 🚀

Entorno local para prácticas y laboratorios de ingeniería de datos usando **Apache Airflow**, **PostgreSQL** y **LocalStack (AWS S3)** sobre Docker.

---

## 🏗️ Arquitectura de Servicios

| Servicio | Contenedor | Puerto Host | Descripción | Credenciales / Endpoint |
| :--- | :--- | :--- | :--- | :--- |
| **Apache Airflow** | `airflow_webserver` | `8888` | Orquestador Web & Scheduler | User: `admin` / Pass: `admin` |
| **LocalStack (S3)** | `localstack_s3` | `4566` | Emulador oficial de AWS S3 | Key: `test` / Secret: `test` (Región: `us-east-1`) |
| **PostgreSQL** | `airflow_postgres` | `5432` | Metastore de Airflow y BD analítica | User: `airflow` / Pass: `airflow` (DB: `airflow`) |

---

## 🖥️ Cómo navegar y explorar los servicios

### 1. Mock de S3 (LocalStack)
El servicio está escuchando en el puerto `4566`.

- **Opción A (Interfaz Web oficial de LocalStack):**
  1. Ingresa en tu navegador a: **[https://app.localstack.cloud](https://app.localstack.cloud)** (o la extensión de LocalStack en Docker Desktop).
  2. En el menú lateral ve a **Resource Browser ➔ S3**.
  3. Verás el bucket `raw-data` y todos sus archivos sin costo ni configuración compleja.

- **Opción B (Clientes GUI de escritorio como Cyberduck o DBeaver):**
  - **Tipo de conexión:** Amazon S3
  - **Servidor / Endpoint:** `localhost:4566` (desmarca SSL/HTTPS)
  - **Access Key ID:** `test`
  - **Secret Access Key:** `test`
  - **Default Region:** `us-east-1`

### 2. Apache Airflow (Interfaz Web)
- **URL:** [http://localhost:8888](http://localhost:8888)
- **Usuario:** `admin`
- **Contraseña:** `admin`

### 3. PostgreSQL (DBeaver / DataGrip / pgAdmin)
- **Host:** `localhost` | **Puerto:** `5432`
- **Usuario:** `airflow` | **Password:** `airflow` | **Database:** `airflow`

---

## 📁 Estructura del Proyecto

```plaintext
labs_data_engineer/
├── dags/                  # DAGs de Apache Airflow
│   └── test_s3_dag.py     # DAG de prueba (crea bucket y sube CSV a LocalStack S3)
├── dbt_project/           # Proyecto dbt montado en /opt/airflow/dbt_project
├── docker-compose.yaml    # Configuración de los servicios
└── README.md              # Documentación
```

---

## ⚡ Comandos frecuentes

- **Levantar los servicios:**
  ```bash
  docker compose up -d
  ```
- **Disparar el DAG manualmente:**
  ```bash
  docker exec airflow_webserver airflow dags trigger 01_setup_local_s3
  ```
- **Listar buckets directamente con AWS CLI local (opcional):**
  ```bash
  aws --endpoint-url=http://localhost:4566 s3 ls
  ```
