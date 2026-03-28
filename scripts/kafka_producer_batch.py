import random
import uuid
import datetime
import json
import logging

import mysql.connector
import pandas as pd
from kafka import KafkaProducer

# ── Logging ───────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────
KAFKA_BROKER    = "localhost:9092"
KAFKA_TOPIC     = "tracking-events"

MYSQL_HOST      = "localhost"
MYSQL_PORT      = 3306
MYSQL_DATABASE  = "logs"
MYSQL_USER      = "root"
MYSQL_PASSWORD  = "1"

BATCH_SIZE      = 50   # số event gửi mỗi lần chạy rồi exit

INTERACT_TYPES   = ["click", "conversion", "qualified", "unqualified"]
INTERACT_WEIGHTS = [70, 10, 10, 10]


def load_reference_data() -> dict:
    cnx = mysql.connector.connect(
        user=MYSQL_USER, password=MYSQL_PASSWORD,
        host=MYSQL_HOST, port=MYSQL_PORT, database=MYSQL_DATABASE,
    )
    jobs_df = pd.read_sql(
        "SELECT id AS job_id, campaign_id, group_id, company_id FROM job", cnx
    )
    pub_df = pd.read_sql(
        "SELECT DISTINCT(id) AS publisher_id FROM master_publisher", cnx
    )
    cnx.close()
    return {
        "job_list"      : jobs_df["job_id"].tolist(),
        "campaign_list" : jobs_df["campaign_id"].tolist(),
        "group_list"    : jobs_df[jobs_df["group_id"].notnull()]["group_id"].astype(int).tolist(),
        "publisher_list": pub_df["publisher_id"].tolist(),
    }


def generate_event(ref: dict) -> dict:
    return {
        "create_time" : str(uuid.uuid1()),
        "bid"         : random.randint(0, 1),
        "campaign_id" : random.choice(ref["campaign_list"]),
        "custom_track": random.choices(INTERACT_TYPES, weights=INTERACT_WEIGHTS)[0],
        "group_id"    : random.choice(ref["group_list"]),
        "job_id"      : random.choice(ref["job_list"]),
        "publisher_id": random.choice(ref["publisher_list"]),
        "ts"          : datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }


def main():
    log.info("=== Kafka Producer Batch START ===")
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: str(k).encode("utf-8"),
        acks="all",
        retries=3,
        max_block_ms=10_000,
    )
    log.info(f"Connected to Kafka: {KAFKA_BROKER} → topic '{KAFKA_TOPIC}'")
    ref = load_reference_data()
    for _ in range(BATCH_SIZE):
        event = generate_event(ref)
        producer.send(topic=KAFKA_TOPIC, key=event["job_id"], value=event)

    producer.flush()
    producer.close()
    log.info(f"=== Batch DONE: sent {BATCH_SIZE} events → '{KAFKA_TOPIC}' ===")


if __name__ == "__main__":
    main()
