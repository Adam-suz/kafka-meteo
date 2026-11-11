"""TP Kafka — Exercice 3 : météo Open-Meteo vers weather_stream toutes les 30 s."""
import json
import time
import requests
from kafka import KafkaProducer

BOOTSTRAP_SERVER = "localhost:9092"
TOPIC = "weather_stream"
API_URL = "https://api.open-meteo.com/v1/forecast"
PERIODE_SECONDES = 30

VILLES = [
    ("Paris", "France", 48.8566, 2.3522),
    ("Seville", "Spain", 37.3891, -5.9845),
    ("Dubai", "United Arab Emirates", 25.2048, 55.2708),
    ("Reykjavik", "Iceland", 64.1466, -21.9426),
    ("Wellington", "New Zealand", -41.2865, 174.7762),
]

producer = KafkaProducer(
    bootstrap_servers=[BOOTSTRAP_SERVER],
    value_serializer=lambda data: json.dumps(data, ensure_ascii=False).encode("utf-8"),
)

print("Producteur météo démarré. Arrêt : Ctrl+C")
try:
    while True:
        debut = time.monotonic()
        for city, country, latitude, longitude in VILLES:
            try:
                response = requests.get(
                    API_URL,
                    params={
                        "latitude": latitude,
                        "longitude": longitude,
                        "current": "temperature_2m,wind_speed_10m,weather_code",
                        "wind_speed_unit": "ms",
                    },
                    timeout=15,
                )
                response.raise_for_status()
                data = response.json()
                if not all(k in data.get("current", {}) for k in ("temperature_2m", "wind_speed_10m", "weather_code")):
                    raise ValueError("Champs météo manquants dans la réponse API")
                data["city"] = city
                data["country"] = country
                producer.send(TOPIC, value=data, key=city.encode("utf-8")).get(timeout=15)
                valeurs = data["current"]
                print(
                    f"Message envoyé : {city} | Température : {valeurs['temperature_2m']} °C "
                    f"| Vent : {valeurs['wind_speed_10m']} m/s",
                    flush=True,
                )
            except Exception as error:
                # L'erreur d'une ville ne doit pas interrompre les autres villes.
                print(f"Erreur pour {city} : {error}", flush=True)
        producer.flush()
        time.sleep(max(0, PERIODE_SECONDES - (time.monotonic() - debut)))
except KeyboardInterrupt:
    print("\nArrêt du producteur météo.")
finally:
    producer.flush()
    producer.close()
