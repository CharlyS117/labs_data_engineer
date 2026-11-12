# Arquitectura de Datos y Flujo de Interacción: Airflow + S3 + dbt + PostgreSQL 📐

Este documento detalla la arquitectura técnica, la estructura modular del repositorio y el flujo de extremo a extremo (ELT) que conecta las fuentes de datos, el almacenamiento en objetos, la orquestación y el modelado analítico.

---

## 1. Arquitectura General del Sistema

```mermaid
flowchart TD
    subgraph Fuentes ["Fuentes Externas"]
        API["API GBFS Ecobici (REST / JSON)"]
        CSV["Archivos CSV (E-commerce / Ventas)"]
    end

    subgraph Orquestacion ["Orquestación (Apache Airflow 2.9)"]
        DAG1["dag_mobility_elt"]
        DAG2["elt_ecommerce_pipeline"]
        DAG3["s3_to_postgres_dag"]
    end

    subgraph Storage ["Data Lake (LocalStack AWS S3)"]
        B1[("Bucket: raw-data<br/>/mobility/ecobici/status/...")]
        B2[("Bucket: raw-data<br/>/orders/... y /customers/...")]
    end

    subgraph Warehouse ["Data Warehouse (PostgreSQL 15)"]
        subgraph EsquemaPublic ["Esquema: public (Capa Raw / Ingesta)"]
            T1[("raw_station_status")]
            T2[("raw_orders")]
            T3[("raw_customers")]
            T4[("raw_ventas")]
        end

        subgraph EsquemaAnalytics ["Esquema: analytics (Transformado por dbt)"]
            V1{{"stg_station_status (View)"}}
            V2{{"stg_orders (View)"}}
            M1[("fct_station_kpis (Table)")]
            M2[("fct_customer_orders (Table)")]
        end
    end

    subgraph Transformacion ["Modelado y Calidad de Datos (dbt Core)"]
        DBT_RUN["dbt run --profiles-dir ."]
        DBT_TEST["dbt test --profiles-dir ."]
    end

    %% Relaciones
    API -->|1. Extracción Python| DAG1
    DAG1 -->|2. Put Object JSON| B1
    B1 -->|3. Get Object JSON| DAG1
    DAG1 -->|4. Ingesta Bulk / Truncate| T1

    CSV -->|1. Carga previa| B2
    B2 -->|2. Sensor S3KeySensor| DAG2
    DAG2 -->|3. Carga Pandas to_sql| T2
    DAG2 -->|3. Carga Pandas to_sql| T3

    DAG1 -->|5. BashOperator| DBT_RUN
    DAG2 -->|4. BashOperator| DBT_RUN

    DBT_RUN -->|Lee capa Raw| EsquemaPublic
    DBT_RUN -->|Materializa Staging| V1
    DBT_RUN -->|Materializa Staging| V2
    V1 -->|Agregaciones & Case| M1
    V2 -->|Join con Clientes| M2

    DBT_RUN --> DBT_TEST
    DBT_TEST -->|Valida Not Null & Unicidad| EsquemaAnalytics
```

---

## 2. Diagrama de Secuencia: Cómo interactúa Airflow con dbt

A continuación se muestra el ciclo de vida de una ejecución del pipeline `pipeline_movilidad_urbana_elt`:

```mermaid
sequenceDiagram
    autonumber
    actor Scheduler as Airflow Scheduler
    participant PythonTask as Task: Ingesta (Python)
    participant LocalStack as LocalStack S3 (raw-data)
    participant Postgres as PostgreSQL (DW)
    participant DbtTask as Task: dbt (BashOperator)

    Scheduler->>PythonTask: Ejecutar task 'extract_gbfs_to_s3'
    PythonTask->>LocalStack: GET datos API Ecobici -> PUT JSON en S3
    LocalStack-->>PythonTask: 200 OK (S3 Key registrado)

    Scheduler->>PythonTask: Ejecutar task 'load_s3_to_postgres'
    PythonTask->>LocalStack: Descargar snapshot JSON
    PythonTask->>Postgres: TRUNCATE & COPY a public.raw_station_status
    Postgres-->>PythonTask: Filas insertadas

    Scheduler->>DbtTask: Ejecutar task 'dbt_run_models'
    Note over DbtTask,Postgres: dbt compila modelos SQL usando profiles.yml (.env)
    DbtTask->>Postgres: CREATE VIEW analytics.stg_station_status AS ...
    DbtTask->>Postgres: CREATE TABLE analytics.fct_station_kpis AS ...
    Postgres-->>DbtTask: Tablas y vistas materializadas con éxito

    Scheduler->>DbtTask: Ejecutar task 'dbt_test_models'
    DbtTask->>Postgres: SELECT count(*) en tests (not_null, unique)
    Postgres-->>DbtTask: 0 errores encontrados
    Note over Scheduler: DAG finaliza con estado SUCCESS ✅
```

---

## 3. Modelo de Capas dbt (Medallion Architecture)

El proyecto organiza las transformaciones en capas analíticas bien definidas:

```mermaid
graph LR
    subgraph CapaBronce ["Capa Bronce (Raw - public)"]
        R1["raw_station_status"]
        R2["raw_orders"]
        R3["raw_customers"]
    end

    subgraph CapaPlata ["Capa Plata (Staging / Views - analytics)"]
        S1["stg_station_status<br/>• Tipado boolean<br/>• to_timestamp()<br/>• occupancy_rate seguro"]
        S2["stg_orders<br/>• Cast numeric<br/>• Validación fechas"]
    end

    subgraph CapaOro ["Capa Oro (Marts / Tables - analytics)"]
        M1["fct_station_kpis<br/>• Clasificación de stock<br/>• Alertas operativas"]
        M2["fct_customer_orders<br/>• Total gastado por cliente<br/>• Conteo de órdenes"]
    end

    R1 --> S1 --> M1
    R2 --> S2 --> M2
    R3 --> M2
```

---

## 4. Estructura Modular del Repositorio

```plaintext
labs_data_engineer/
│
├── .env.example                 # Plantilla pública de variables de entorno
├── .env                         # Secretos locales (ignorado en Git)
├── .gitignore                   # Exclusiones de Git (logs, target dbt, .env)
├── docker-compose.yaml          # Orquestación de contenedores parametrizada
├── README.md                    # Documentación operativa y credenciales
├── DIAGRAMS.md                  # Este documento con diagramas de flujo y arquitectura
│
├── dags/                        # Orquestación (Apache Airflow)
│   ├── dag_mobility_elt.py      # Pipeline producción: API -> S3 -> Postgres -> dbt
│   ├── elt_ecommerce_pipeline.py# Pipeline E-Commerce: Sensor S3 -> Postgres -> dbt
│   ├── s3_to_postgres_dag.py    # Pipeline incremental con UPSERT
│   └── test_s3_dag.py           # DAG de prueba y validación de S3
│
└── dbt_project/                 # Modelado y Calidad (dbt Core)
    ├── dbt_project.yml          # Configuración del proyecto y materializaciones
    ├── profiles.yml             # Perfil dinámico que lee variables con env_var()
    └── models/
        ├── staging/             # Capa de preparación y normalización
        │   ├── schema.yml       # Tests automatizados (not_null, unique)
        │   ├── stg_orders.sql
        │   └── stg_station_status.sql
        └── marts/               # Capa de negocio y reporting
            ├── fct_customer_orders.sql
            └── fct_station_kpis.sql
```
