import random
import uuid
import datetime
import json
import time
import logging

import mysql.connector
import pandas as pd
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable


# ── Logging ───────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ── Config ────────────────────────────────────────────────────────
KAFKA_BROKER      = "localhost:9092"
KAFKA_TOPIC       = "tracking-events"

MYSQL_HOST        = "localhost"
MYSQL_PORT        = 3306
MYSQL_DATABASE    = "logs"
MYSQL_USER        = "root"
MYSQL_PASSWORD    = "1"

BATCH_SIZE        = 20   # số event tối đa mỗi batch (random 1 ~ BATCH_SIZE)
LOOP_INTERVAL_SEC = 20   

INTERACT_TYPES   = ["click", "conversion", "qualified", "unqualified"]
INTERACT_WEIGHTS = [70, 10, 10, 10]   # click chiếm 70%, còn lại 10% mỗi loại


# ── Load reference data từ MySQL ──────────────────────────────────
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

    ref = {
        "job_list"      : jobs_df["job_id"].tolist(),
        "campaign_list" : jobs_df["campaign_id"].tolist(),
        "group_list"    : jobs_df[jobs_df["group_id"].notnull()]["group_id"].astype(int).tolist(),
        "publisher_list": pub_df["publisher_id"].tolist(),
    }
    return ref


# ── Sinh 1 fake tracking event ────────────────────────────────────
def generate_event(ref: dict) -> dict:
    return {
        "create_time" : str(uuid.uuid1()),                                         # UUID v1 time-based
        "bid"         : random.randint(0, 1),                                      # 0 hoặc 1
        "campaign_id" : random.choice(ref["campaign_list"]),
        "custom_track": random.choices(INTERACT_TYPES, weights=INTERACT_WEIGHTS)[0],
        "group_id"    : random.choice(ref["group_list"]),
        "job_id"      : random.choice(ref["job_list"]),
        "publisher_id": random.choice(ref["publisher_list"]),
        "ts"          : datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }


# ── Main ──────────────────────────────────────────────────────────
def main():
    # 1. Kết nối Kafka
    producer = KafkaProducer(
            bootstrap_servers = [KAFKA_BROKER],
            value_serializer  = lambda v: json.dumps(v).encode("utf-8"),
            key_serializer    = lambda k: str(k).encode("utf-8"),
            acks              = "all",      # chờ tất cả replicas xác nhận
            retries           = 3,
            max_block_ms      = 10_000,
        )
    log.info(f"Connected to Kafka: {KAFKA_BROKER} → topic '{KAFKA_TOPIC}'")

    # 2. Load reference data từ MySQL
    ref = load_reference_data()

    # 3. Vòng lặp sinh data liên tục
    total     = 0
    iteration = 0
    try:
        while True:
            iteration += 1
            n = random.randint(1, BATCH_SIZE)

            for _ in range(n):
                event = generate_event(ref)
                producer.send(
                    topic = KAFKA_TOPIC,
                    key   = event["job_id"],
                    value = event,
                )

            producer.flush()
            total += n
            log.info(
                f"Iteration {iteration:>4} | "
                f"sent {n:>2} events | "
                f"total {total}"
            )
            time.sleep(LOOP_INTERVAL_SEC)

    except KeyboardInterrupt:
        log.info(f"Stopped by user. Total messages sent: {total}")
    finally:
        producer.flush()
        producer.close()
        log.info("Producer closed.")


if __name__ == "__main__":
    main()
