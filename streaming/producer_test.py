import json
import time
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
import os

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
TOPIC_NAME = "eventos_transacciones"

def delivery_report(err, msg):
    if err is not None:
        print(f"Error al enviar mensaje: {err}")
    else:
        print(f"Mensaje entregado a {msg.topic()} [partición {msg.partition()}] offset {msg.offset()}")

def ensure_topic_exists(bootstrap_servers, topic_name):
    admin_client = AdminClient({"bootstrap.servers": bootstrap_servers})
    topics = admin_client.list_topics(timeout=10).topics
    if topic_name not in topics:
        print(f"Creando tópico '{topic_name}'...")
        new_topic = NewTopic(topic=topic_name, num_partitions=1, replication_factor=1)
        futures = admin_client.create_topics([new_topic])
        for topic, f in futures.items():
            try:
                f.result()
                print(f"Tópico '{topic}' creado con éxito.")
            except Exception as e:
                print(f"Aviso al crear tópico {topic}: {e}")
    else:
        print(f"El tópico '{topic_name}' ya existe.")

def main():
    print(f"Conectando Producer a Kafka en {BOOTSTRAP_SERVERS}...")
    ensure_topic_exists(BOOTSTRAP_SERVERS, TOPIC_NAME)

    producer_conf = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "client.id": "test-transaction-producer"
    }
    producer = Producer(producer_conf)

    eventos = [
        {"transaccion_id": "TX-1001", "usuario_id": "USR-401", "monto": 149.99, "tipo": "COMPRA", "timestamp": time.time()},
        {"transaccion_id": "TX-1002", "usuario_id": "USR-108", "monto": 25.50, "tipo": "PAGO", "timestamp": time.time()},
        {"transaccion_id": "TX-1003", "usuario_id": "USR-320", "monto": 890.00, "tipo": "TRANSFERENCIA", "timestamp": time.time()},
        {"transaccion_id": "TX-1004", "usuario_id": "USR-401", "monto": 12.00, "tipo": "COMPRA", "timestamp": time.time()},
        {"transaccion_id": "TX-1005", "usuario_id": "USR-550", "monto": 430.75, "tipo": "RETIRO", "timestamp": time.time()},
    ]

    print(f"Enviando {len(eventos)} eventos simulados al tópico '{TOPIC_NAME}'...")
    for evento in eventos:
        payload = json.dumps(evento).encode("utf-8")
        key = evento["transaccion_id"].encode("utf-8")
        producer.produce(topic=TOPIC_NAME, key=key, value=payload, callback=delivery_report)
        time.sleep(0.2)

    producer.flush()
    print("Todos los eventos fueron enviados exitosamente a Kafka.")

if __name__ == "__main__":
    main()

