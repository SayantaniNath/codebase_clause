"""
producer.py
-----------
The Producer reads user records from PostgreSQL and publishes
each record as a JSON message to a Kafka topic.

Flow:
  PostgreSQL --> fetch_records() --> publish_to_kafka() --> Kafka Topic

Run with:
  python src/producer.py
"""

import json
import logging
import os
import time

from dotenv import load_dotenv
load_dotenv()

import psycopg2
import psycopg2.extras
from confluent_kafka import Producer, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# PostgreSQL — source database
DB_CONFIG = {
    "host": os.environ.get("DB_HOST", "localhost"),
    "port": int(os.environ.get("DB_PORT", 5432)),
    "dbname": os.environ.get("DB_NAME", "mydb"),
    "user": os.environ.get("DB_USER", "myuser"),
    "password": os.environ.get("DB_PASSWORD", "mypassword"),
}

# Kafka — message broker
KAFKA_CONFIG = {
    "bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
}

KAFKA_TOPIC = "org.users.events"   # topic name — follows org.domain.event convention
KAFKA_TOPIC_PARTITIONS = 3         # 3 partitions for parallel processing
KAFKA_TOPIC_REPLICATION = 1        # 1 replica (fine for local dev)
KAFKA_MAX_RETRIES = 3              # retry up to 3 times on failure
KAFKA_RETRY_BACKOFF = 2            # wait 2s between retries (doubles each attempt)

# SQL query — fetch users created in the last 30 days
FETCH_QUERY = """
    SELECT id, name, email, phone, age, city, country, timezone,
           status, is_verified, two_factor_enabled, subscription_plan,
           monthly_spend, login_count, last_login, referral_source,
           device_type, browser, metadata, created_at
    FROM users
    WHERE created_at >= NOW() - INTERVAL '30 days'
    ORDER BY created_at DESC
    LIMIT 1000
"""

# ---------------------------------------------------------------------------
# Step 1 — Fetch records from PostgreSQL
# ---------------------------------------------------------------------------

def fetch_records() -> list[dict]:
    """
    Connects to PostgreSQL and fetches user records.
    Returns a list of dictionaries, one per row.
    """
    log.info("Connecting to PostgreSQL …")
    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            # RealDictCursor returns each row as {column: value} dict
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                log.info("Running fetch query …")
                cur.execute(FETCH_QUERY)
                rows = [dict(row) for row in cur.fetchall()]
        log.info("Fetched %d records from PostgreSQL", len(rows))
        return rows
    except psycopg2.OperationalError as exc:
        log.error("Could not connect to PostgreSQL: %s", exc)
        raise
    except psycopg2.Error as exc:
        log.error("Query failed: %s", exc)
        raise

# ---------------------------------------------------------------------------
# Step 2 — Ensure Kafka topic exists
# ---------------------------------------------------------------------------

def create_kafka_topic() -> None:
    """
    Creates the Kafka topic if it doesn't already exist.
    Safe to call multiple times — skips if topic already exists.
    """
    admin = AdminClient(KAFKA_CONFIG)
    existing = admin.list_topics(timeout=10).topics

    if KAFKA_TOPIC in existing:
        log.info("Kafka topic '%s' already exists — skipping", KAFKA_TOPIC)
        return

    new_topic = NewTopic(KAFKA_TOPIC, num_partitions=KAFKA_TOPIC_PARTITIONS,
                         replication_factor=KAFKA_TOPIC_REPLICATION)
    futures = admin.create_topics([new_topic])
    for t, future in futures.items():
        try:
            future.result()
            log.info("Created Kafka topic '%s'", t)
        except Exception as exc:
            log.error("Failed to create topic '%s': %s", t, exc)
            raise

# ---------------------------------------------------------------------------
# Step 3 — Publish records to Kafka
# ---------------------------------------------------------------------------

# Tracks keys of messages that failed to deliver
failed_records: list = []

def delivery_report(err, msg):
    """Kafka callback — called after each message is delivered or fails."""
    if err:
        log.error("Delivery failed for key %s: %s", msg.key(), err)
        failed_records.append(msg.key())
    else:
        log.debug("Delivered key=%s to partition=%d offset=%d",
                  msg.key(), msg.partition(), msg.offset())


def produce_with_retry(producer: Producer, key: str, value: str) -> None:
    """
    Sends a single message to Kafka with exponential backoff retry.
    Waits 2s, 4s, 8s between retries before giving up.
    """
    for attempt in range(1, KAFKA_MAX_RETRIES + 1):
        try:
            producer.produce(KAFKA_TOPIC, key=key, value=value, callback=delivery_report)
            producer.poll(0)
            return
        except BufferError:
            log.warning("Queue full (attempt %d/%d) — flushing …", attempt, KAFKA_MAX_RETRIES)
            producer.flush()
        except KafkaException as exc:
            wait = KAFKA_RETRY_BACKOFF * (2 ** (attempt - 1))
            if attempt < KAFKA_MAX_RETRIES:
                log.warning("Retrying in %ds (attempt %d/%d): %s", wait, attempt, KAFKA_MAX_RETRIES, exc)
                time.sleep(wait)
            else:
                log.error("Giving up after %d attempts for key %s", KAFKA_MAX_RETRIES, key)
                raise


def publish_to_kafka(records: list[dict]) -> int:
    """
    Publishes all records to Kafka.
    Returns number of successfully published records.
    """
    producer = Producer(KAFKA_CONFIG)
    log.info("Publishing %d records to topic '%s' …", len(records), KAFKA_TOPIC)

    skipped = 0
    for record in records:
        key = str(record.get("id", ""))
        value = json.dumps(record, default=str)  # default=str handles datetime objects
        try:
            produce_with_retry(producer, key, value)
        except KafkaException:
            log.error("Skipping record id=%s", key)
            skipped += 1

    # Wait for all messages to be delivered before exiting
    remaining = producer.flush(timeout=30)
    if remaining > 0:
        log.warning("%d messages not delivered after flush timeout", remaining)
        skipped += remaining
    else:
        log.info("All messages delivered to Kafka")

    if failed_records:
        skipped += len(failed_records)

    return len(records) - skipped

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    """Runs the producer pipeline: PostgreSQL → Kafka."""
    # Step 1: Fetch records from PostgreSQL
    records = fetch_records()
    if not records:
        log.info("No records found — exiting")
        return

    # Step 2: Ensure Kafka topic exists
    create_kafka_topic()

    # Step 3: Publish records to Kafka
    success = publish_to_kafka(records)

    # Summary
    total = len(records)
    print("\n" + "=" * 50)
    print("PRODUCER SUMMARY")
    print("=" * 50)
    print(f"  Records fetched from PostgreSQL : {total}")
    print(f"  Records published to Kafka      : {success}/{total} ({success/total*100:.1f}%)")
    print(f"  Topic                           : {KAFKA_TOPIC}")
    overall = "SUCCESS" if success == total else "PARTIAL / FAILED"
    print(f"  Status                          : {overall}")
    print("=" * 50 + "\n")


if __name__ == "__main__":
    main()
