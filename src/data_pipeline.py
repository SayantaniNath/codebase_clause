"""
Sample pipeline: reads from a PostgreSQL database, creates a Kafka topic,
publishes records to it, and writes results to Redis.

Requirements:
    pip install psycopg2-binary confluent-kafka redis
"""

import json
import logging
import time
import psycopg2
import psycopg2.extras
import redis
from confluent_kafka import Producer, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration — replace with your actual connection details
# ---------------------------------------------------------------------------
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "mydb",
    "user": "myuser",
    "password": "mypassword",
}

KAFKA_CONFIG = {
    "bootstrap.servers": "localhost:9092",
}

KAFKA_TOPIC = "comsumption-pipeline-events"
KAFKA_TOPIC_PARTITIONS = 3
KAFKA_TOPIC_REPLICATION = 1

REDIS_CONFIG = {
    "host": "localhost",
    "port": 6379,
    "db": 0,
    "decode_responses": True,
}

REDIS_KEY_PREFIX = "user:"
REDIS_TTL_SECONDS = 3600  # 1 hour


# ---------------------------------------------------------------------------
# Step 1 — Read records from the database
# ---------------------------------------------------------------------------
USE_MOCK_DATA = False  # Set to False to use a real PostgreSQL connection

def _generate_mock_records(n: int = 100) -> list[dict]:
    import random
    from datetime import datetime, timedelta

    first_names = [
        "Alice", "Bob", "Carol", "David", "Eve", "Frank", "Grace", "Hank",
        "Iris", "Jack", "Karen", "Liam", "Mia", "Noah", "Olivia", "Paul",
        "Quinn", "Rachel", "Sam", "Tina", "Uma", "Victor", "Wendy", "Xander",
        "Yara", "Zoe", "Aaron", "Beth", "Carlos", "Diana",
    ]
    last_names = [
        "Smith", "Jones", "White", "Brown", "Davis", "Miller", "Wilson",
        "Moore", "Taylor", "Anderson", "Thomas", "Jackson", "Harris", "Martin",
        "Thompson", "Garcia", "Martinez", "Robinson", "Clark", "Rodriguez",
    ]
    cities = [
        "New York", "Los Angeles", "Chicago", "Houston", "Phoenix",
        "Philadelphia", "San Antonio", "San Diego", "Dallas", "San Jose",
        "Austin", "Jacksonville", "Denver", "Seattle", "Boston",
    ]
    countries = ["US", "CA", "GB", "AU", "DE", "FR", "IN", "BR", "MX", "JP"]
    statuses = ["active", "active", "active", "inactive", "pending"]
    plans = ["free", "basic", "pro", "enterprise"]
    domains = ["gmail.com", "yahoo.com", "outlook.com", "example.com", "company.io"]

    base_dt = datetime(2026, 4, 13, 0, 0, 0)
    records = []
    for i in range(1, n + 1):
        first = random.choice(first_names)
        last = random.choice(last_names)
        name = f"{first} {last}"
        email = f"{first.lower()}.{last.lower()}{random.randint(1, 99)}@{random.choice(domains)}"
        phone = f"+1-{random.randint(200,999)}-{random.randint(100,999)}-{random.randint(1000,9999)}"
        created_at = base_dt - timedelta(minutes=random.randint(0, 43200))  # within last 30 days
        records.append({
            "id": i,
            "name": name,
            "email": email,
            "phone": phone,
            "age": random.randint(18, 72),
            "city": random.choice(cities),
            "country": random.choice(countries),
            "status": random.choice(statuses),
            "subscription_plan": random.choice(plans),
            "login_count": random.randint(0, 500),
            "created_at": created_at.strftime("%Y-%m-%d %H:%M:%S"),
        })
    return records


MOCK_RECORDS = _generate_mock_records(100)


def fetch_records(query: str) -> list[dict]:
    if USE_MOCK_DATA:
        log.info("Using mock data (%d records)", len(MOCK_RECORDS))
        return MOCK_RECORDS

    log.info("Connecting to database …")
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            log.info("Executing query: %s", query)
            cur.execute(query)
            rows = [dict(row) for row in cur.fetchall()]
    log.info("Fetched %d records", len(rows))
    return rows


# ---------------------------------------------------------------------------
# Step 2 — Create Kafka topic (idempotent — skips if it already exists)
# ---------------------------------------------------------------------------
def create_kafka_topic(topic: str, num_partitions: int, replication_factor: int) -> None:
    if USE_MOCK_DATA:
        log.info("[MOCK] Skipping Kafka topic creation for '%s'", topic)
        return

    admin = AdminClient(KAFKA_CONFIG)
    existing = admin.list_topics(timeout=10).topics
    if topic in existing:
        log.info("Kafka topic '%s' already exists — skipping creation", topic)
        return

    new_topic = NewTopic(
        topic,
        num_partitions=num_partitions,
        replication_factor=replication_factor,
    )
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
KAFKA_MAX_RETRIES = 3
KAFKA_RETRY_BACKOFF = 2  # seconds (doubles each attempt)

failed_records: list[dict] = []


def delivery_report(err, msg):
    if err:
        log.error("Delivery failed for key %s: %s", msg.key(), err)
        failed_records.append(msg.key())
    else:
        log.debug("Delivered to %s [partition %d] offset %d", msg.topic(), msg.partition(), msg.offset())


def produce_with_retry(producer: Producer, topic: str, key: str, value: str) -> None:
    """Produce a single message with exponential backoff retry."""
    for attempt in range(1, KAFKA_MAX_RETRIES + 1):
        try:
            producer.produce(topic, key=key, value=value, callback=delivery_report)
            producer.poll(0)
            return
        except BufferError:
            log.warning("Producer queue full (attempt %d/%d) — flushing and retrying …", attempt, KAFKA_MAX_RETRIES)
            producer.flush()
        except KafkaException as exc:
            wait = KAFKA_RETRY_BACKOFF * (2 ** (attempt - 1))
            if attempt < KAFKA_MAX_RETRIES:
                log.warning("KafkaException on attempt %d/%d: %s — retrying in %ds …", attempt, KAFKA_MAX_RETRIES, exc, wait)
                time.sleep(wait)
            else:
                log.error("KafkaException after %d attempts for key %s: %s — giving up", KAFKA_MAX_RETRIES, key, exc)
                raise


def publish_to_kafka(topic: str, records: list[dict]) -> int:
    """Returns the number of successfully published records."""
    if USE_MOCK_DATA:
        for record in records:
            log.info("[MOCK] Published record id=%s to topic '%s'", record.get("id"), topic)
        return len(records)

    producer = Producer(KAFKA_CONFIG)
    log.info("Publishing %d records to topic '%s' …", len(records), topic)

    skipped = 0
    for record in records:
        key = str(record.get("id", ""))
        value = json.dumps(record, default=str)
        try:
            produce_with_retry(producer, topic, key, value)
        except KafkaException:
            log.error("Skipping record id=%s after exhausting retries", key)
            skipped += 1
            continue

    try:
        remaining = producer.flush(timeout=30)
        if remaining > 0:
            log.warning("%d message(s) were not delivered after flush timeout", remaining)
            skipped += remaining
        else:
            log.info("All records published to Kafka")
    except KafkaException as exc:
        log.error("Fatal error during final flush: %s", exc)
        raise

    if failed_records:
        log.warning("%d message(s) reported delivery failure via callback: %s", len(failed_records), failed_records)
        skipped += len(failed_records)

    return len(records) - skipped


# ---------------------------------------------------------------------------
# Step 4 — Write records to Redis
# ---------------------------------------------------------------------------
def write_to_redis(records: list[dict], key_prefix: str, ttl: int) -> int:
    """Returns the number of successfully written records."""
    if USE_MOCK_DATA:
        for record in records:
            log.info("[MOCK] Wrote key '%s%s' to Redis (TTL=%ds)", key_prefix, record.get("id"), ttl)
        return len(records)

    client = redis.Redis(**REDIS_CONFIG)
    pipe = client.pipeline()
    log.info("Writing %d records to Redis …", len(records))
    for record in records:
        redis_key = f"{key_prefix}{record.get('id', 'unknown')}"
        pipe.hset(redis_key, mapping={k: str(v) for k, v in record.items()})
        pipe.expire(redis_key, ttl)

    try:
        pipe.execute()
        log.info("All records written to Redis with TTL=%ds", ttl)
        return len(records)
    except Exception as exc:
        log.error("Redis pipeline failed: %s", exc)
        return 0


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    query = """
        SELECT id, name, email, phone, age, city, country,
               status, subscription_plan, login_count, created_at
        FROM users
        WHERE created_at >= NOW() - INTERVAL '1 day'
        ORDER BY created_at DESC
        LIMIT 1000
    """

    # 1. Read from database
    records = fetch_records(query)
    if not records:
        log.info("No records found — exiting")
        return

    # 2. Ensure the Kafka topic exists
    create_kafka_topic(KAFKA_TOPIC, KAFKA_TOPIC_PARTITIONS, KAFKA_TOPIC_REPLICATION)

    # 3. Publish to Kafka
    kafka_success = publish_to_kafka(KAFKA_TOPIC, records)

    # 4. Cache in Redis
    redis_success = write_to_redis(records, REDIS_KEY_PREFIX, REDIS_TTL_SECONDS)

    # Summary report
    total = len(records)
    print("\n" + "=" * 50)
    print("PIPELINE SUMMARY REPORT")
    print("=" * 50)
    print(f"  Records fetched      : {total}")
    print(f"  Kafka published      : {kafka_success}/{total}  ({kafka_success/total*100:.1f}%)")
    print(f"  Redis written        : {redis_success}/{total}  ({redis_success/total*100:.1f}%)")
    print(f"  Kafka failures       : {total - kafka_success}")
    print(f"  Redis failures       : {total - redis_success}")
    overall = "SUCCESS" if kafka_success == total and redis_success == total else "PARTIAL / FAILED"
    print(f"  Overall status       : {overall}")
    print("=" * 50 + "\n")
    log.info("Pipeline complete")


if __name__ == "__main__":
    main()
