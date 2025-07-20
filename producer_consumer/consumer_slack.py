"""
Kafka → Slack notifier **plus** Strava in‑app comments
──────────────────────────────────────────────────────
• Prefect‑powered  
• Looks up the employee’s *prenom / nom* in the company RH table  
• Posts a congratulatory Slack message that both tags the user and shows the
  friendly name  
• Adds a public comment under the Strava activity itself  
• Validation / API failures are surfaced as Prefect artifacts at the **flow**
  level

Environment variables required
──────────────────────────────
REDPANDA_EXTERNAL_HOST, REDPANDA_EXTERNAL_PORT, TOPIC  
POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD  
SLACK_BOT_TOKEN, SLACK_CHANNEL_ID  
STRAVA_TOKEN   (personal access token with `activity:write`)  
"""

# ───────────────────────────── Imports ─────────────────────────────
from argparse import ArgumentParser
from datetime import datetime, timezone
from dotenv import load_dotenv
import json
import logging
import os
import random
import sys

import polars as pl
import psycopg
import requests
from kafka import KafkaConsumer
from pydantic import ValidationError
from prefect import flow, task, get_run_logger
from prefect.artifacts import create_markdown_artifact

from model_validation import StravaEvent
import config

# ─────────────────── Configuration & constants ────────────────────
load_dotenv()
logging.basicConfig(level=logging.INFO)

SLACK_TOKEN   = os.getenv("SLACK_BOT_TOKEN")
SLACK_CHANNEL = os.getenv("SLACK_CHANNEL_ID")
STRAVA_TOKEN  = os.getenv("STRAVA_TOKEN")

GENERIC_MESSAGES = [
    "🏅  Great job, <@{name}>! Keep it up!",
    "🎉  Way to go, <@|{name}>! Another activity crushed!",
    "👏  Awesome effort, <@{name}> – keep the momentum!"
]

RH_LOOKUP_SQL = f"""
    SELECT
        id_salarie,
        CONCAT(prenom, ' ', nom) AS full_name
    FROM
        {config.RH_TABLE}
"""

# ────────────────────────── CLI builder ────────────────────────────
def build_parser():
    parser = ArgumentParser("Kafka → Slack & Strava notifier")
    parser.add_argument("--ip",   default=os.getenv("REDPANDA_EXTERNAL_HOST", "localhost"))
    parser.add_argument("--port", type=int,
                        default=int(os.getenv("REDPANDA_EXTERNAL_PORT", 9092)))
    parser.add_argument("--topic", default=os.getenv("TOPIC", "strava-data"))
    return parser

# ───────────────────────────── Tasks ───────────────────────────────
@task(retries=2, retry_delay_seconds=30, log_prints=True)
def load_rh_lookup():
    """
    Pull « id_salarie → full_name » mapping from Postgres and return as dict.
    """
    logger = get_run_logger()
    with psycopg.connect(config.PG_URI) as conn:
        df = pl.read_database(RH_LOOKUP_SQL, conn)
    mapping = dict(zip(df["id_salarie"], df["full_name"]))
    logger.info("Loaded %s employee rows", len(mapping))
    return mapping


@task(log_prints=True)
def build_consumer(ip, port, topic):
    """
    Return a ready‑to‑go KafkaConsumer.
    """
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=f"{ip}:{port}",
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id="strava-slack-notifier",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )
    get_run_logger().info("Connected to %s:%s – topic ‘%s’", ip, port, topic)
    return consumer


@task(log_prints=True, retries=3)
def send_slack_message(activity, full_name):
    """
    Push a congratulation message to Slack.
    """
    logger = get_run_logger()
    template   = random.choice(GENERIC_MESSAGES)
    slack_text = template.format(name=full_name)

    payload = {"channel": SLACK_CHANNEL, "text": slack_text}
    headers = {
        "Authorization": f"Bearer {SLACK_TOKEN}",
        "Content-Type":  "application/json; charset=utf-8",
    }

    resp = requests.post("https://slack.com/api/chat.postMessage",
                         headers=headers, json=payload, timeout=10)

    if not resp.ok or not resp.json().get("ok"):
        raise RuntimeError(f"Slack API error: {resp.text}")

    logger.info("✅ Slack message sent for %s (%s)", activity.id_salarie, full_name)


@task(log_prints=True, retries=3)
def post_strava_comment(activity, full_name):
    """
    Add a ‘Nice effort!’ comment directly on the Strava activity.
    """
    logger = get_run_logger()
    if not STRAVA_TOKEN:
        logger.warning("STRAVA_TOKEN missing – skipping comment")
        return

    comment_text = f"👏 Nice effort, {full_name}!"
    url          = f"https://www.strava.com/api/v3/activities/{activity.id}/comments"
    headers      = {"Authorization": f"Bearer {STRAVA_TOKEN}"}
    resp         = requests.post(url, headers=headers,
                                 json={"text": comment_text}, timeout=10)

    if resp.status_code != 201:
        raise RuntimeError(f"Strava API error: {resp.status_code} – {resp.text}")

    logger.info("✅ Strava comment added for activity %s", activity.id)


def create_failure_artifact(payload, error):
    """
    Surface any validation or API failure in the Prefect UI.
    """
    ts = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    key     = f"notify-fail-{ts}"

    create_markdown_artifact(
        key=key,
        markdown=(
            "## ❌ Notification failure\n"
            f"**Error** : `{error}`\n\n"
            f"```json\n{payload}\n```"
        ),
    )

# ───────────────────────────── Flow ────────────────────────────────
@flow(name="Kafka‑Slack‑Strava‑Notifier")
def notifier_flow(ip="localhost", port=9092, topic="strava-data"):
    """
    Main loop: consume → validate → post Slack + Strava comment.
    """
    logger = get_run_logger()

    # Pre‑flight checks
    if not SLACK_TOKEN or not SLACK_CHANNEL:
        logger.error("Slack credentials missing – set SLACK_BOT_TOKEN & SLACK_CHANNEL_ID")
        return

    rh_lookup = load_rh_lookup()
    consumer  = build_consumer(ip, port, topic)

    try:
        for msg in consumer:
            raw = msg.value

            # 1️⃣ Validate the payload
            try:
                activity = StravaEvent.model_validate(raw)
            except ValidationError as ve:
                logger.error("Validation failed: %s – payload %s", ve, raw)
                create_failure_artifact(json.dumps(raw, indent=2), f"ValidationError: {ve}")
                continue

            full_name = rh_lookup.get(activity.id_salarie, "Sporty colleague")

            # 2️⃣ Fan‑out notification tasks
            try:
                send_slack_message.submit(activity, full_name)
                post_strava_comment.submit(activity, full_name)
            except Exception as exc:
                create_failure_artifact(activity.json(indent=2), str(exc))

    except KeyboardInterrupt:
        logger.info("⏹  Notifier interrupted by user")
    finally:
        consumer.close()
        logger.info("Consumer closed")

# ────────────────────────── Entry‑point ────────────────────────────
if __name__ == "__main__":
    args = build_parser().parse_args(sys.argv[1:])
    notifier_flow(args.ip, args.port, args.topic)
