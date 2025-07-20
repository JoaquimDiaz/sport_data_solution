from argparse import ArgumentParser
from datetime import datetime
from dotenv import load_dotenv
from uuid import uuid4
import json
import logging
import os
import random
import sys
import time

from kafka import KafkaProducer
import polars as pl
from prefect import flow, task, get_run_logger
import config

load_dotenv()
logging.basicConfig(level=logging.INFO)

# ─────────────────────── CLI PARSER ────────────────────────
def build_parser() -> ArgumentParser:
    p = ArgumentParser("Prefect producer with occasional bad rows")
    p.add_argument("--ip", default=os.getenv("REDPANDA_EXTERNAL_HOST", "localhost"))
    p.add_argument("--port", type=int, default=int(os.getenv("REDPANDA_EXTERNAL_PORT", 9092)))
    p.add_argument("--topic", default=os.getenv("TOPIC", "strava-data"))
    p.add_argument("--interval", type=float, default=float(os.getenv("INTERVAL", 1.0)))
    p.add_argument("--error-rate", type=float, default=config.ERROR_RATE)
    return p


# ─────────────────────── Functions ────────────────────────
def create_producer(ip: str, port: int) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=f"{ip}:{port}",
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )


# ─────────────────────── tasks ──────────────────────────
@task(log_prints=True)
def generate_good(mapping: dict) -> dict:
    " Generates a strava like event with correct data. "
    emp = random.choice(list(mapping.keys()))
    sport = mapping[emp]
    low, high = config.SPORT_DISTANCE_RANGES.get(sport, (0, 0))
    return {
        "id": str(uuid4()),
        "id_salarie": emp,
        "date_debut": datetime.now().isoformat(),
        "sport_type": sport,
        "distance": random.randint(low, high) if high else 0,
        "duration": random.randint(30, 180),
        "comment": None,
    }


def mutate(event: dict) -> dict:
    " Corrupts an event with erroneous data. "
    e = event.copy()
    err = random.choice(
        ["missing_field", "wrong_type", "invalid_uuid", "negative_distance", "unknown_sport"]
    )
    if err == "missing_field":
        e.pop(random.choice(["distance", "sport_type"]))
    elif err == "wrong_type":
        e["distance"] = "oops"
    elif err == "invalid_uuid":
        e["id"] = random.randint(1000, 9999)
    elif err == "negative_distance":
        e["distance"] = -500
    elif err == "unknown_sport":
        e["sport_type"] = "Quidditch"
    return e


@task(log_prints=True)
def generate_event(mapping: dict, error_rate: float) -> dict:
    " Generates a random event. The event is corrupted randomly following the 'error_rate'. "
    event = generate_good.fn(mapping)
    if random.random() < error_rate:
        event = mutate(event)
        get_run_logger().warning(f"⚠️  BAD event generated: {event}")
    return event


# ─────────────────────── flow ───────────────────────────
@flow(name="Producer-Strava-POC")
def kafka_producer_flow(
    ip: str,
    port: int,
    topic: str,
    interval: float,
    error_rate: float,
):
    log = get_run_logger()

    # 1. Loading salarie info
    df = pl.read_csv(config.SALARIE_SPORT_FILE)
    mapping = {
        r["id_salarie"]: r["pratique_d_un_sport"]
        for r in df.iter_rows(named=True)
        if r["pratique_d_un_sport"] in config.SPORT_DISTANCE_RANGES
    }
    log.info(f"Loaded {len(mapping)} employees | error rate = {error_rate*100:.0f}%")

    # 2. Creates the producer and connects to the broker
    producer = create_producer(ip, port)
    log.info(f"Producer connected to {ip}:{port} (topic '{topic}')")

    # 3. Starting the loop to generate events
    try:
        while True:
            ev = generate_event(mapping, error_rate)
            producer.send(topic=topic, value=ev)
            log.info(f"→ Sent to Kafka: {ev['id']}")
            time.sleep(interval)
    except KeyboardInterrupt:
        log.info("⏹  Producer stopped by user")
    finally:
        producer.flush(); producer.close()
        log.info("Producer closed")


# ────────────────────── cli entry ───────────────────────
if __name__ == "__main__":
    p = build_parser().parse_args(sys.argv[1:])
    kafka_producer_flow(
        p.ip, 
        p.port, 
        p.topic, 
        p.interval, 
        p.error_rate)
