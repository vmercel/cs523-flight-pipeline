"""
Part 1 — Real-time Data Ingestion (Apache Kafka)
Polls the OpenSky Network REST API every 10 seconds and publishes
one JSON message per airborne flight to the Kafka topic 'flights-raw'.
"""

import json
import os
import time

import requests
from dotenv import load_dotenv
from kafka import KafkaProducer

load_dotenv()

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "172.18.0.4:9092")
TOPIC = "flights-raw"
POLL_INTERVAL = 10  # seconds — matches OpenSky anonymous rate limit

OPENSKY_URL = "https://opensky-network.org/api/states/all"
OPENSKY_USER = os.getenv("OPENSKY_USERNAME") or None
OPENSKY_PASS = os.getenv("OPENSKY_PASSWORD") or None

# Fall-back replay file if OpenSky is unreachable
REPLAY_FILE = os.path.join(os.path.dirname(__file__), "..", "data", "static", "replay.json")

# ---------------------------------------------------------------------------
# Kafka producer
# ---------------------------------------------------------------------------
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8"),
    acks="all",
    retries=3,
)


def fetch_states():
    """Fetch all current flight states from OpenSky.  Returns list of dicts."""
    auth = (OPENSKY_USER, OPENSKY_PASS) if OPENSKY_USER else None
    try:
        resp = requests.get(OPENSKY_URL, auth=auth, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        return data.get("states", []) or []
    except Exception as exc:
        print(f"[producer] OpenSky error: {exc} — trying replay file")
        return load_replay()


def load_replay():
    """Load pre-recorded states from replay.json as a fallback."""
    try:
        with open(REPLAY_FILE) as f:
            return json.load(f)
    except Exception:
        print("[producer] No replay file found. Skipping this poll cycle.")
        return []


def state_to_dict(state, ingestion_ts):
    """Map OpenSky state vector array to a named dictionary."""
    return {
        "icao24":         state[0],
        "callsign":       (state[1] or "").strip(),
        "origin_country": state[2],
        "time_position":  state[3],
        "last_contact":   state[4],
        "longitude":      state[5],
        "latitude":       state[6],
        "baro_altitude":  state[7],
        "on_ground":      state[8],
        "velocity":       state[9],
        "true_track":     state[10],
        "vertical_rate":  state[11],
        "sensors":        state[12],
        "geo_altitude":   state[13],
        "squawk":         state[14],
        "spi":            state[15],
        "position_source":state[16],
        "ingestion_ts":   ingestion_ts,
    }


def main():
    print(f"[producer] Starting — broker={KAFKA_BOOTSTRAP}  topic={TOPIC}")
    poll_count = 0
    while True:
        ingestion_ts = int(time.time())
        states = fetch_states()

        sent = 0
        for state in states:
            if not state or state[0] is None:
                continue
            msg = state_to_dict(state, ingestion_ts)
            producer.send(TOPIC, key=msg["icao24"], value=msg)
            sent += 1

        producer.flush()
        poll_count += 1
        print(f"[producer] poll={poll_count}  sent={sent} messages  ts={ingestion_ts}")
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
