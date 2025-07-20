from argparse import ArgumentParser
from datetime import datetime, timezone
import json
import logging
import os
import sys

from dotenv import load_dotenv
from kafka import KafkaConsumer
import psycopg
from pydantic import ValidationError
from prefect import flow, task, get_run_logger
from prefect.artifacts import create_markdown_artifact

import config
from model_validation import StravaEvent

# ────────────────────────── Basic configuration ────────────────────

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ──────────────────────── Utility functions ────────────────────────


def build_postgres_uri(user, pwd, host, port, db):
    """
    Return a SQLAlchemy‑style Postgres connection URI.
    """
    return f"postgresql://{user}:{pwd}@{host}:{port}/{db}"


def create_table_if_not_exists(pg_conn):
    """
    Ensure the main activity table and companion error table are present.
    """
    with pg_conn.cursor() as cur:
        # Main table
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {config.SPORT_TABLE} (
                id UUID PRIMARY KEY,
                id_salarie TEXT NOT NULL,
                date_debut TIMESTAMP NOT NULL,
                date_fin TIMESTAMP NOT NULL,
                sport_type TEXT NOT NULL,
                distance_metre INT,
                comment TEXT
            );
            """
        )
        # Error table
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {config.SPORT_TABLE}_errors (
                id SERIAL PRIMARY KEY,
                raw_data JSONB NOT NULL,
                errors TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT NOW()
            );
            """
        )
    pg_conn.commit()


def create_validation_artifact(raw, details):
    """
    Write a validation‑failure artifact to the Prefect UI (flow level).
    """
    create_markdown_artifact(
        key=f"validation-error-{raw.get('id', 'unknown')}",
        markdown=(
            "## ❌ Validation error\n\n"
            f"**Payload**\n```json\n{json.dumps(raw, indent=2, default=str)}\n```\n\n"
            "**Details**\n" + "\n".join(f"- {d}" for d in details)
        ),
    )


def create_insert_artifact(activity, err):
    """
    Write an insert‑failure artifact to the Prefect UI (flow level).
    """
    create_markdown_artifact(
        key=f"insert-fail-{activity.id}",
        markdown=(
            "## ❌ Database insert failed\n\n"
            f"**ID**: `{activity.id}`  \n"
            f"**Error**: `{err}`\n\n"
            f"```json\n{activity.json(indent=2)}\n```"
        ),
    )


# ─────────────────────────────── Tasks ─────────────────────────────


@task(log_prints=True)
def log_error(raw_data, error, db_uri):
    """
    Persist a raw payload plus error message to the *_errors* table.
    """
    logger = get_run_logger()
    with psycopg.connect(db_uri) as pg_conn:
        with pg_conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {config.SPORT_TABLE}_errors (raw_data, errors)
                VALUES (%s, %s)
                """,
                (json.dumps(raw_data), str(error)),
            )
        pg_conn.commit()
    logger.info("✅ Error logged")


@task(log_prints=True)
def validate_activity_data(raw_data):
    """
    Validate a Kafka message against the ``StravaEvent`` schema.

    On failure the task raises, so the flow can surface an artifact.
    """
    logger = get_run_logger()
    try:
        activity = StravaEvent(**raw_data)
        logger.info(f"✅ Validated activity {activity.id}")
        return activity
    except ValidationError as ve:
        log_error.submit(raw_data, ve, config.PG_URI)
        raise
    except Exception as exc:
        log_error.submit(raw_data, exc, config.PG_URI)
        raise


@task(retries=3, log_prints=True)
def insert_activity(activity, db_uri, table):
    """
    Insert a validated activity row. Retries on transient failures.
    """
    logger = get_run_logger()
    with psycopg.connect(db_uri) as pg_conn:
        with pg_conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {table}
                  (id, id_salarie, date_debut, date_fin,
                   sport_type, distance_metre, comment)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING;
                """,
                (
                    str(activity.id),
                    activity.id_salarie,
                    activity.date_debut,
                    activity.calculate_end_time(),
                    activity.sport_type,
                    activity.distance,
                    activity.comment,
                ),
            )
        pg_conn.commit()
    logger.info(f"✅ Inserted activity {activity.id}")


@task(log_prints=True)
def build_kafka_consumer(ip, port, topic):
    """
    Create and return a KafkaConsumer subscribed to *topic*.
    """
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=f"{ip}:{port}",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="strava-consumer-group",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )
    get_run_logger().info(f"Connected to {ip}:{port}, subscribed to '{topic}'")
    return consumer


# ─────────────────────────────── Flow ──────────────────────────────


@flow(name="Postgres-Consumer")
def kafka_consumer_flow(ip,
                        port,
                        topic,
                        db_user,
                        db_pwd,
                        db_host,
                        db_port,
                        db_name):
    """
    Main Prefect flow that continuously consumes, validates and persists
    Strava activities.
    """
    logger = get_run_logger()

    # Database setup
    db_uri = build_postgres_uri(db_user, db_pwd, db_host, db_port, db_name)
    with psycopg.connect(db_uri) as conn:
        create_table_if_not_exists(conn)

    logger.info(f"Table {config.SPORT_TABLE} ready — starting consumption")

    consumer = build_kafka_consumer(ip, port, topic)

    processed = 0
    invalid   = 0

    try:
        for msg in consumer:
            processed += 1
            raw = msg.value

            # 1️⃣ Validation
            try:
                activity = validate_activity_data.submit(raw).result()
            except ValidationError as ve:
                details = [
                    " → ".join(map(str, err["loc"])) + f": {err['msg']}"
                    for err in ve.errors()
                ]
                create_validation_artifact(raw, details)
                invalid += 1
                logger.warning(f"⏭  Skipped invalid payload ({invalid} total)")
                continue

            # 2️⃣ Insert
            try:
                insert_activity.submit(activity, db_uri, config.SPORT_TABLE)
            except Exception as exc:
                create_insert_artifact(activity, exc)

            # 3️⃣ Progress log
            if processed % 100 == 0:
                logger.info(
                    f"Processed {processed} messages — "
                    f"{invalid} validation errors"
                )

    except KeyboardInterrupt:
        logger.info("⏹  Interrupted by user")

    finally:
        consumer.close()
        logger.info(
            f"Consumer closed — {processed} processed, {invalid} validation errors"
        )


# ──────────────────────────── CLI entry‑point ──────────────────────


def build_parser():
    """
    Construct the command‑line interface.
    """
    parser = ArgumentParser(
        description="Prefect Kafka → Postgres consumer with validation",
    )
    parser.add_argument("--ip",        default=os.getenv("REDPANDA_EXTERNAL_HOST", "localhost"))
    parser.add_argument("--port", type=int, default=int(os.getenv("REDPANDA_EXTERNAL_PORT", 9092)))
    parser.add_argument("--topic",     default=os.getenv("TOPIC", "strava-data"))
    parser.add_argument("--db-host",   default=os.getenv("POSTGRES_HOST", "localhost"))
    parser.add_argument("--db-port", type=int, default=int(os.getenv("POSTGRES_PORT", 5432)))
    parser.add_argument("--db",        default=os.getenv("POSTGRES_DB", "db"))
    parser.add_argument("--user",      default=os.getenv("POSTGRES_USER", "postgres"))
    parser.add_argument("--pwd",       default=os.getenv("POSTGRES_PASSWORD", "postgres"))
    return parser


if __name__ == "__main__":
    args = build_parser().parse_args(sys.argv[1:])
    kafka_consumer_flow(
        ip=args.ip,
        port=args.port,
        topic=args.topic,
        db_user=args.user,
        db_pwd=args.pwd,
        db_host=args.db_host,
        db_port=args.db_port,
        db_name=args.db,
    )
