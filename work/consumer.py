"""TP Kafka — Exercice 2 : affichage des messages en temps réel."""
from kafka import KafkaConsumer

TOPIC = "weather_stream"  # Changeable, comme demandé dans le TP.
BOOTSTRAP_SERVER = "localhost:9092"

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=[BOOTSTRAP_SERVER],
    auto_offset_reset="latest",  # Lit les NOUVEAUX messages ; utiliser 'earliest' pour l'historique.
    group_id=None,
    value_deserializer=lambda raw: raw.decode("utf-8"),
)

print(f"Écoute du topic {TOPIC} (Ctrl+C pour arrêter)...")
try:
    for message in consumer:
        print(f"[{message.topic} / offset={message.offset}] {message.value}", flush=True)
except KeyboardInterrupt:
    print("\nConsommateur arrêté.")
finally:
    consumer.close()
