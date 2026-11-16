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

# Configuración parametrizada de subreddits y queries para ingeniería de datos / tecnología
DEFAULT_SUBREDDITS = "dataengineering,apachespark,python,datascience,database"
DEFAULT_QUERIES = "data engineering architecture,spark streaming delta lake,kafka kraft cluster,dbt data modeling"

subreddits_env = os.getenv("REDDIT_SUBREDDITS", DEFAULT_SUBREDDITS)
queries_env = os.getenv("REDDIT_SEARCH_QUERIES", DEFAULT_QUERIES)

SUBREDDITS = [s.strip() for s in subreddits_env.split(",") if s.strip()]
SEARCH_QUERIES = [q.strip() for q in queries_env.split(",") if q.strip()]

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_NAME = os.getenv("KAFKA_TOPIC_REDDIT", "reddit_posts_stream")
HEADERS = {"User-Agent": os.getenv("REDDIT_USER_AGENT", "python:de_streaming_lab:v2.0 (by /u/labs_data_engineer)")}
POLL_INTERVAL_SECONDS = int(os.getenv("REDDIT_POLL_INTERVAL_SECONDS", "20"))

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

def generate_mock_reddit_posts(identifier, is_search=False):
    """Genera publicaciones técnicas sintéticas ante restricciones HTTP 403 de la API pública."""
    now_ts = time.time()
    mock_posts = []
    
    sample_data = [
        (
            "Arquitectura Lakehouse con Apache Spark y Delta Lake en Producción",
            "dataengineering",
            "Architecture",
            True,
            "Compartimos nuestra experiencia migrando un Data Warehouse tradicional hacia una arquitectura Lakehouse basada en Delta Lake y Apache Spark. Logramos reducir los tiempos de procesamiento batch en un 65% gracias al particionamiento adecuado por fecha y el uso intensivo de compactación (OPTIMIZE/VACUUM). La implementación de ACID transactions eliminó por completo los problemas de escrituras concurrentes que teníamos anteriormente. Si están evaluando este cambio, les recomendamos dimensionar adecuadamente el memory overhead de los executors."
        ),
        (
            "Optimización de consultas PySpark y tuning de executors en Kubernetes",
            "apachespark",
            "Optimization",
            True,
            "En este artículo detallamos las lecciones aprendidas afinando clusters de Spark sobre Kubernetes. Descubrimos que asignar 4 cores por executor con 16GB de RAM y 2GB de memory overhead ofrece el mejor equilibrio para evitar el Garbage Collection excesivo. Además, habilitar Adaptive Query Execution (AQE) simplificó enormemente el manejo de data skews en joins complejos sin requerir salting manual."
        ),
        (
            "Mejores prácticas para pipelines de streaming continuo con Kafka KRaft",
            "dataengineering",
            "Tutorial",
            True,
            "La eliminación de ZooKeeper en Apache Kafka con el modo KRaft ha simplificado la administración de brokers en entornos de contenedores. Para garantizar alta disponibilidad en microservicios, recomendamos configurar replication factor 3, min.insync.replicas en 2 y compresión snappy o zstd en el productor para reducir el consumo de ancho de banda."
        ),
        (
            "Modelado dimensional con dbt y Data Lakehouse: Guía y casos reales",
            "datascience",
            "Best Practices",
            True,
            "Implementar modelado dimensional (tablas de hechos y dimensiones) directamente sobre tablas Delta utilizando dbt nos permitió tener versionamiento de código y pruebas automáticas de integridad. La separación en capas staging, intermediate y marts permite que los analistas de negocio consulten métricas confiables con tiempos de respuesta sub-segundo."
        ),
        (
            "Asyncio y FastAPI para microservicios de ingesta de eventos de alta concurrencia",
            "python",
            "Discussion",
            True,
            "Construir endpoints de ingesta con FastAPI respaldados por colas asíncronas hacia Kafka nos permitió procesar más de 25,000 eventos por segundo con latencias de 12ms. Usar uvloop y pydantic v2 reduce notablemente la sobrecarga de serialización JSON en comparación con frameworks sincrónicos."
        )
    ]

    for idx, (title, sub, flair, is_self, text_body) in enumerate(sample_data):
        target_sub = identifier if not is_search else sub
        post_id = f"post_{target_sub}_{int(now_ts)}_{idx}"
        
        mock_posts.append({
            "data": {
                "id": post_id,
                "subreddit": target_sub,
                "title": title,
                "selftext": text_body,
                "link_flair_text": flair,
                "is_self": is_self,
                "author": f"eng_{target_sub}_{idx}",
                "score": int(random.randint(45, 680)),
                "num_comments": int(random.randint(5, 120)),
                "upvote_ratio": round(random.uniform(0.85, 0.99), 2),
                "over_18": False,
                "created_utc": now_ts - random.randint(30, 7200),
                "url": f"https://reddit.com/r/{target_sub}/comments/{post_id}"
            }
        })
    return mock_posts

def fetch_reddit_json(url, session, identifier, is_search=False):
    try:
        response = session.get(url, headers=HEADERS, timeout=12)
        if response.status_code == 200:
            data = response.json()
            return data.get("data", {}).get("children", [])
        elif response.status_code == 429:
            logger.warning(f"HTTP 429 Too Many Requests para {url}. Pausando 60 segundos...")
            time.sleep(60)
            return []
        elif response.status_code in (403, 404):
            logger.warning(f"HTTP {response.status_code} en {url}. Aplicando fallback de laboratorio técnico.")
            return generate_mock_reddit_posts(identifier, is_search)
        else:
            logger.error(f"Error HTTP {response.status_code} en {url}")
            return []
    except requests.exceptions.RequestException as e:
        logger.error(f"Error de conexion en {url}: {e}")
        return generate_mock_reddit_posts(identifier, is_search)

def fetch_subreddit_posts(subreddit, session):
    url = f"https://www.reddit.com/r/{subreddit}/new.json?limit=15"
    return fetch_reddit_json(url, session, subreddit, is_search=False)

def fetch_search_posts(query, session):
    url = f"https://www.reddit.com/search.json?q={requests.utils.quote(query)}&sort=new&limit=15"
    return fetch_reddit_json(url, session, query, is_search=True)

def build_post_event(post, fallback_subreddit):
    post_id = post.get("id")
    if not post_id:
        return None, None
        
    selftext = str(post.get("selftext") or "")
    link_flair_text = str(post.get("link_flair_text") or "")
    is_self = bool(post.get("is_self", False))

    event = {
        "id": str(post_id),
        "subreddit": str(post.get("subreddit") or fallback_subreddit),
        "link_flair_text": link_flair_text,
        "is_self": is_self,
        "title": str(post.get("title") or ""),
        "selftext": selftext,
        "author": str(post.get("author") or "unknown"),
        "score": int(post.get("score") or 0),
        "num_comments": int(post.get("num_comments") or 0),
        "upvote_ratio": float(post.get("upvote_ratio") or 1.0),
        "over_18": bool(post.get("over_18", False)),
        "created_utc": float(post.get("created_utc") or time.time()),
        "url": str(post.get("url") or ""),
        "ingested_at": datetime.now(timezone.utc).isoformat()
    }
    return post_id, event

def main():
    logger.info(f"Iniciando Reddit Kafka Producer con brokers: {KAFKA_BOOTSTRAP_SERVERS}")
    
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
                    if item.get("kind") not in (None, "t3"):
                        continue
                    post = item.get("data", {})
                    post_id, event = build_post_event(post, sub)
                    if not post_id or post_id in seen_post_ids:
                        continue

                    seen_post_ids.add(post_id)
                    total_nuevos += 1

                    payload = json.dumps(event).encode("utf-8")
                    key = str(post_id).encode("utf-8")

                    producer.produce(
                        topic=TOPIC_NAME,
                        key=key,
                        value=payload,
                        callback=delivery_callback
                    )

                time.sleep(1)

            for query in SEARCH_QUERIES:
                children = fetch_search_posts(query, session)
                for item in children:
                    if item.get("kind") not in (None, "t3"):
                        continue
                    post = item.get("data", {})
                    post_id, event = build_post_event(post, "search_feed")
                    if not post_id or post_id in seen_post_ids:
                        continue

                    seen_post_ids.add(post_id)
                    total_nuevos += 1

                    payload = json.dumps(event).encode("utf-8")
                    key = str(post_id).encode("utf-8")

                    producer.produce(
                        topic=TOPIC_NAME,
                        key=key,
                        value=payload,
                        callback=delivery_callback
                    )

                time.sleep(1)

            producer.flush()
            if total_nuevos > 0:
                logger.info(f"Ciclo completado: {total_nuevos} nuevos posts publicados a '{TOPIC_NAME}'. Total vistos: {len(seen_post_ids)}")
            else:
                logger.info("Sin posts nuevos en este ciclo.")

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
