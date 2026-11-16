import json
from confluent_kafka import Consumer, KafkaError
import os

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
TOPIC_NAME = "eventos_transacciones"

def main():
    print(f"Conectando Consumer a Kafka en {BOOTSTRAP_SERVERS}...")
    conf = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": "test-transaction-consumer-group",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True
    }

    consumer = Consumer(conf)
    consumer.subscribe([TOPIC_NAME])
    print(f"Suscrito al tópico '{TOPIC_NAME}'. Esperando eventos (timeout de 10s)...")

    count = 0
    try:
        while count < 5:
            msg = consumer.poll(timeout=5.0)
            if msg is None:
                print("No se recibieron más mensajes en el timeout.")
                break
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print(f"Fin de partición alcanzado {msg.topic()} [{msg.partition()}]")
                else:
                    print(f"Error en Consumer: {msg.error()}")
                break

            key = msg.key().decode("utf-8") if msg.key() else "None"
            value = json.loads(msg.value().decode("utf-8"))
            count += 1
            print(f"[{count}/5] Mensaje recibido -> Key: {key} | Offset: {msg.offset()} | Data: {value}")
    finally:
        consumer.close()
        print(f"Consumer cerrado. Total procesados: {count} eventos.")

if __name__ == "__main__":
    main()

