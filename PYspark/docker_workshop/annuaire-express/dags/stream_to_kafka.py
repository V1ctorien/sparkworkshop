"""Récupère des profils sur randomuser.me et les publie dans Kafka."""
import json
import logging
import os
import time
from datetime import datetime, timezone

import requests
from kafka import KafkaProducer

API_URL = "https://randomuser.me/api/?results=1"
TOPIC = "random_names"
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9094")

log = logging.getLogger(__name__)


def fetch_user() -> dict:
    resp = requests.get(API_URL, timeout=10)
    resp.raise_for_status()
    return resp.json()["results"][0]


def to_record(user: dict) -> dict:
    loc = user["location"]
    name = user["name"]
    return {
        "id": user["login"]["uuid"],
        "full_name": f"{name['title']} {name['first']} {name['last']}",
        "gender": user["gender"],
        "address": f"{loc['street']['number']} {loc['street']['name']}",
        "city": loc["city"],
        "country": loc["country"],
        "postcode": str(loc["postcode"]),
        "latitude": float(loc["coordinates"]["latitude"]),
        "longitude": float(loc["coordinates"]["longitude"]),
        "email": user["email"],
        "ingested_at": datetime.now(timezone.utc).isoformat(),
    }


def start_streaming(duration_s: int = 120, every_s: int = 10) -> int:
    """Publie un nouveau profil toutes les `every_s` secondes pendant `duration_s`."""
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
    )
    sent = 0
    end = time.time() + duration_s
    try:
        while time.time() < end:
            record = to_record(fetch_user())        # nouveau profil à chaque tour
            producer.send(TOPIC, key=record["id"], value=record)
            sent += 1
            log.info("Envoyé %s (%s)", record["full_name"], record["country"])
            time.sleep(every_s)
    finally:
        producer.flush()                            # rien ne reste dans le tampon
        producer.close()
    log.info("%d messages envoyés", sent)
    return sent


if __name__ == "__main__":                           # test rapide depuis ton PC
    logging.basicConfig(level=logging.INFO)
    start_streaming(duration_s=30, every_s=5)