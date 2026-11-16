import time
import json
import logging
import random
from datetime import datetime, timezone
import os
import requests
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("RedditProducer")

SUBREDDITS = ["dataengineering", "apachespark", "python"]
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_NAME = os.getenv("KAFKA_TOPIC_REDDIT", "reddit_posts_stream")
HEADERS = {"User-Agent": "python:de_streaming_lab:v1.0 (by /u/labs_data_engineer)"}
POLL_INTERVAL_SECONDS = 20

def ensure_topic(bootstrap_servers, topic_name):
    try:
        admin_client = AdminClient({"bootstrap.servers": bootstrap_servers})
        metadata = admin_client.list_topics(timeout=10)
        if topic_name not in metadata.topics:
            logger.info(f"Creando tópico '{topic_name}'...")
            new_topic = NewTopic(topic=topic_name, num_partitions=3, replication_factor=1)
            futures = admin_client.create_topics([new_topic])
            for topic, f in futures.items():
                f.result()
                logger.info(f"Tópico '{topic}' creado con éxito.")
        else:
            logger.info(f"Tópico '{topic_name}' verificado en Kafka.")
    except Exception as e:
        logger.warning(f"Aviso al verificar/crear tópico: {e}")

def delivery_callback(err, msg):
    if err:
        logger.error(f"Error al enviar mensaje a Kafka: {err}")
    else:
        logger.debug(f"Mensaje enviado a {msg.topic()} [part {msg.partition()}] offset {msg.offset()}")

def generate_mock_reddit_posts(subreddit):
    """Genera posts simulados en caso de bloqueo 403 de Reddit para asegurar continuidad del laboratorio."""
    now_ts = time.time()
    mock_posts = []
    sample_titles = {
        "dataengineering": [
            "How do you orchestrate streaming with Delta Lake and Spark?",
            "Kafka vs Redpanda in 2026 for CDC pipelines",
            "Best practices for partitioning in S3 data lakes",
            "dbt incremental models vs Delta Merge operations",
            "Migrating Airflow to Kubernetes executor"
        ],
        "apachespark": [
            "Structured Streaming micro-batch latency optimization",
            "Delta Lake vacuum retention threshold and checkpoints",
            "PySpark memory tuning: executor cores vs memory overhead",
            "Reading S3A files directly with Hadoop AWS 3.3.4",
            "Handling schema evolution with Delta format"
        ],
        "python": [
            "Python 3.12 performance improvements in data processing",
            "FastAPI with background Kafka event producer",
            "Pydantic v2 validation for streaming JSON payloads",
            "Asyncio vs ThreadPoolExecutor for I/O bound APIs",
            "Polars vs Pandas 2.0 benchmark on 10M rows"
        ]
    }
    
    titles = sample_titles.get(subreddit, ["General discussion on data architecture"])
    for idx, title in enumerate(titles):
        post_id = f"sim_{subreddit}_{int(now_ts)}_{idx}"
        mock_posts.append({
            "data": {
                "id": post_id,
                "subreddit": subreddit,
                "title": title,
                "author": f"user_{idx}_{subreddit}",
                "score": int(random.randint(5, 350)),
                "num_comments": int(random.randint(1, 80)),
                "upvote_ratio": round(random.uniform(0.75, 0.99), 2),
                "created_utc": now_ts - random.randint(10, 3600),
                "url": f"https://reddit.com/r/{subreddit}/comments/{post_id}"
            }
        })
    return mock_posts

def fetch_subreddit_posts(subreddit, session):
    url = f"https://www.reddit.com/r/{subreddit}/new.json?limit=15"
    backoff = 5
    for attempt in range(2):
        try:
            response = session.get(url, headers=HEADERS, timeout=10)
            if response.status_code == 200:
                data = response.json()
                return data.get("data", {}).get("children", [])
            elif response.status_code == 429:
                logger.warning(f"HTTP 429 Too Many Requests para r/{subreddit}. Reintentando en {backoff}s...")
                time.sleep(backoff)
                backoff *= 2
            elif response.status_code == 403:
                logger.warning(f"HTTP 403 Forbidden de Reddit en r/{subreddit} (bloqueo datacenter/IP pública). Activando generador sintético para el laboratorio.")
                return generate_mock_reddit_posts(subreddit)
            else:
                logger.error(f"Error HTTP {response.status_code} al consultar r/{subreddit}")
                return generate_mock_reddit_posts(subreddit)
        except requests.exceptions.RequestException as e:
            logger.error(f"Error de red al consultar r/{subreddit}: {e}")
            time.sleep(backoff)
    return generate_mock_reddit_posts(subreddit)

def main():
    logger.info(f"Iniciando Reddit Kafka Producer con brokers: {KAFKA_BOOTSTRAP_SERVERS}")
    
    # Intentar conexión con fallback a localhost si corre fuera de Docker
    servers = KAFKA_BOOTSTRAP_SERVERS
    try:
        ensure_topic(servers, TOPIC_NAME)
    except Exception:
        servers = "localhost:29092"
        logger.info(f"Cambiando fallback a: {servers}")
        ensure_topic(servers, TOPIC_NAME)

    producer = Producer({
        "bootstrap.servers": servers,
        "client.id": "reddit-producer-service",
        "acks": "all"
    })

    session = requests.Session()
    seen_post_ids = set()

    logger.info(f"Escaneando subreddits {SUBREDDITS} cada {POLL_INTERVAL_SECONDS} segundos...")

    try:
        while True:
            total_nuevos = 0
            for sub in SUBREDDITS:
                children = fetch_subreddit_posts(sub, session)
                for item in children:
                    post = item.get("data", {})
                    post_id = post.get("id")

                    if not post_id or post_id in seen_post_ids:
                        continue

                    seen_post_ids.add(post_id)
                    total_nuevos += 1

                    # Estructurar evento limpio
                    event = {
                        "id": str(post_id),
                        "subreddit": str(post.get("subreddit", sub)),
                        "title": str(post.get("title", "")),
                        "author": str(post.get("author", "unknown")),
                        "score": int(post.get("score", 0)),
                        "num_comments": int(post.get("num_comments", 0)),
                        "upvote_ratio": float(post.get("upvote_ratio", 1.0)),
                        "created_utc": float(post.get("created_utc", time.time())),
                        "url": str(post.get("url", "")),
                        "ingested_at": datetime.now(timezone.utc).isoformat()
                    }

                    payload = json.dumps(event).encode("utf-8")
                    key = str(post_id).encode("utf-8")

                    producer.produce(
                        topic=TOPIC_NAME,
                        key=key,
                        value=payload,
                        callback=delivery_callback
                    )

                time.sleep(1)  # Cortesía entre llamadas a subreddits

            producer.flush()
            if total_nuevos > 0:
                logger.info(f"Ciclo completado: {total_nuevos} nuevos posts publicados a '{TOPIC_NAME}'. Total en memoria: {len(seen_post_ids)}")
            else:
                logger.info("Sin posts nuevos en este ciclo.")

            # Mantener el set en un tamaño razonable en ejecuciones largas
            if len(seen_post_ids) > 10000:
                seen_post_ids = set(list(seen_post_ids)[-5000:])

            time.sleep(POLL_INTERVAL_SECONDS)

    except KeyboardInterrupt:
        logger.info("Interrupción por usuario. Cerrando productor...")
    finally:
        producer.flush()
        logger.info("Productor detenido.")

if __name__ == "__main__":
    main()

