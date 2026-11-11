"""TP Kafka — Exercice 1 : envoi du message Hello Kafka."""
import json
from kafka import KafkaProducer

BOOTSTRAP_SERVER = "localhost:9092"
TOPIC = "weather_stream"

producer = KafkaProducer(
    bootstrap_servers=[BOOTSTRAP_SERVER],
    value_serializer=lambda data: json.dumps(data, ensure_ascii=False).encode("utf-8"),
)
try:
    futur = producer.send(TOPIC, {"msg": "Hello Kafka"})
    metadata = futur.get(timeout=15)
    print(f"Message envoyé dans {metadata.topic}, partition {metadata.partition}, offset {metadata.offset}")
finally:
    producer.flush()
    producer.close()
