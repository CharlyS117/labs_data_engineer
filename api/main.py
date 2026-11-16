import os
from typing import Dict, Any, List, Optional
from datetime import datetime
from fastapi import FastAPI, Request, Query
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
from sqlalchemy import create_engine, text
import boto3

app = FastAPI(
    title="Data Lakehouse & Streaming Observability API",
    description="Observability API and Dashboard for Reddit Lakehouse (Kafka, Spark, Delta Lake, PostgreSQL)",
    version="1.1.0"
)

# Setup Templates
templates = Jinja2Templates(directory=os.path.join(os.path.dirname(__file__), "templates"))

# Database Configuration
POSTGRES_USER = os.getenv("POSTGRES_USER", "airflow")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "airflow")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "airflow")

DATABASE_URL = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
engine = create_engine(DATABASE_URL, pool_pre_ping=True)

# AWS / S3 Configuration
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "test")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "test")
AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
AWS_ENDPOINT_URL = os.getenv("AWS_ENDPOINT_URL", "http://localstack:4566")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "raw-data")

def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=AWS_ENDPOINT_URL,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_DEFAULT_REGION
    )

@app.get("/health", tags=["Health"])
def health_check() -> Dict[str, Any]:
    """Check health of PostgreSQL and LocalStack S3."""
    status = {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "services": {}
    }

    # Test Postgres
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        status["services"]["postgres"] = "online"
    except Exception as e:
        status["services"]["postgres"] = f"offline: {str(e)}"
        status["status"] = "degraded"

    # Test S3 / LocalStack
    try:
        s3 = get_s3_client()
        s3.list_buckets()
        status["services"]["s3"] = "online"
    except Exception as e:
        status["services"]["s3"] = f"offline: {str(e)}"
        status["status"] = "degraded"

    return status

@app.get("/metrics/summary", tags=["Metrics"])
def get_metrics_summary() -> Dict[str, Any]:
    """Fetch KPIs aggregated by Spark batch jobs stored in PostgreSQL."""
    try:
        with engine.connect() as conn:
            check_table = conn.execute(text(
                "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'reddit_subreddit_kpis');"
            )).scalar()

            if not check_table:
                return {
                    "status": "warning",
                    "message": "Table public.reddit_subreddit_kpis does not exist yet. Run Spark batch aggregation DAG/job first.",
                    "data": []
                }

            result = conn.execute(text(
                "SELECT subreddit, ingestion_date, total_unique_posts, avg_score, total_comments, avg_upvote_ratio, avg_title_length "
                "FROM public.reddit_subreddit_kpis ORDER BY total_unique_posts DESC;"
            ))
            rows = [dict(row._mapping) for row in result]
            return {
                "status": "success",
                "count": len(rows),
                "data": rows
            }
    except Exception as e:
        return {
            "status": "error",
            "message": str(e),
            "data": []
        }

@app.get("/api/articles", tags=["Articles"])
def get_articles(
    subreddit: Optional[str] = Query(None, description="Filtrar por subreddit (ej: dataengineering)"),
    flair: Optional[str] = Query(None, description="Filtrar por etiqueta/flair"),
    search: Optional[str] = Query(None, description="Búsqueda por texto en título o contenido"),
    limit: int = Query(50, ge=1, le=100)
) -> Dict[str, Any]:
    """Obtiene los artículos técnicos curados con su texto completo desde PostgreSQL."""
    try:
        with engine.connect() as conn:
            check_table = conn.execute(text(
                "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'reddit_articles');"
            )).scalar()

            if not check_table:
                return {
                    "status": "warning",
                    "message": "La tabla public.reddit_articles aún no existe. Ejecuta el job batch de curación de Spark.",
                    "count": 0,
                    "data": []
                }

            query = "SELECT id, subreddit, link_flair_text, title, selftext, author, score, num_comments, url, event_timestamp, ingestion_date FROM public.reddit_articles WHERE 1=1"
            params: Dict[str, Any] = {"limit": limit}

            if subreddit:
                query += " AND subreddit ILIKE :subreddit"
                params["subreddit"] = f"%{subreddit}%"
            if flair:
                query += " AND link_flair_text ILIKE :flair"
                params["flair"] = f"%{flair}%"
            if search:
                query += " AND (title ILIKE :search OR selftext ILIKE :search)"
                params["search"] = f"%{search}%"

            query += " ORDER BY event_timestamp DESC LIMIT :limit"

            result = conn.execute(text(query), params)
            rows = []
            for row in result:
                item = dict(row._mapping)
                if isinstance(item.get("event_timestamp"), datetime):
                    item["event_timestamp"] = item["event_timestamp"].isoformat()
                if item.get("ingestion_date"):
                    item["ingestion_date"] = str(item["ingestion_date"])
                rows.append(item)

            return {
                "status": "success",
                "count": len(rows),
                "data": rows
            }
    except Exception as e:
        return {
            "status": "error",
            "message": str(e),
            "count": 0,
            "data": []
        }

@app.get("/metrics/delta", tags=["Metrics"])
def get_delta_lake_stats(limit: int = 20) -> Dict[str, Any]:
    """Inspect recent posts directly from Delta Lake stored in S3/Localstack."""
    storage_options = {
        "AWS_ENDPOINT_URL": AWS_ENDPOINT_URL,
        "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY_ID,
        "AWS_SECRET_ACCESS_KEY": AWS_SECRET_ACCESS_KEY,
        "AWS_REGION": AWS_DEFAULT_REGION,
        "AWS_ALLOW_HTTP": "true",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true"
    }
    
    delta_uri = f"s3://{S3_BUCKET_NAME}/delta/reddit_posts"
    
    try:
        from deltalake import DeltaTable
        import pyarrow as pa
        dt = DeltaTable(delta_uri, storage_options=storage_options)
        version = dt.version()
        
        table = dt.to_pyarrow_table()
        total_rows = len(table)
        pydict_list = table.to_pylist()
        
        if pydict_list and "ingested_at" in pydict_list[0]:
            pydict_list.sort(key=lambda x: str(x.get("ingested_at") or ""), reverse=True)
            
        recent_records = pydict_list[:limit]
        
        for rec in recent_records:
            for k, v in rec.items():
                if isinstance(v, datetime):
                    rec[k] = v.isoformat()
                    
        return {
            "status": "success",
            "delta_version": version,
            "total_records": total_rows,
            "sample_size": len(recent_records),
            "data": recent_records
        }
    except Exception as e:
        try:
            s3 = get_s3_client()
            response = s3.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix="delta/reddit_posts/")
            keys = [obj["Key"] for obj in response.get("Contents", [])]
            return {
                "status": "pending_first_write",
                "message": f"Delta Lake table not ready or empty: {str(e)}",
                "s3_objects_found": len(keys),
                "keys_preview": keys[:10],
                "data": []
            }
        except Exception as s3_err:
            return {
                "status": "error",
                "message": f"Delta error: {str(e)} | S3 error: {str(s3_err)}",
                "data": []
            }

@app.get("/", response_class=HTMLResponse, tags=["Dashboard"])
@app.get("/dashboard", response_class=HTMLResponse, tags=["Dashboard"])
def get_dashboard(request: Request):
    """Serve the real-time Observability and Articles Dashboard."""
    return templates.TemplateResponse("index.html", {"request": request})
