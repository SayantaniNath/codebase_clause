"""
Sample pipeline: reads from a PostgreSQL database, creates a Kafka topic,
publishes records to it, and writes results to Redis.

Requirements:
    pip install psycopg2-binary confluent-kafka redis
"""

import json
import logging
import os
import time
from dotenv import load_dotenv

load_dotenv()
import psycopg2
import psycopg2.extras
import redis
from confluent_kafka import Producer, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration — reads from environment variables (set via .env or shell)
# ---------------------------------------------------------------------------
DB_CONFIG = {
    "host": os.environ.get("DB_HOST", "localhost"),
    "port": int(os.environ.get("DB_PORT", 5432)),
    "dbname": os.environ.get("DB_NAME", "mydb"),
    "user": os.environ.get("DB_USER", "myuser"),
    "password": os.environ.get("DB_PASSWORD", "mypassword"),
}

KAFKA_CONFIG = {
    "bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
}

KAFKA_TOPIC = "comsumption-pipeline-events"
KAFKA_TOPIC_PARTITIONS = 3
KAFKA_TOPIC_REPLICATION = 1

REDIS_CONFIG = {
    "host": os.environ.get("REDIS_HOST", "localhost"),
    "port": int(os.environ.get("REDIS_PORT", 6379)),
    "db": int(os.environ.get("REDIS_DB", 0)),
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
    timezones = [
        "America/New_York", "America/Chicago", "America/Denver", "America/Los_Angeles",
        "Europe/London", "Europe/Berlin", "Asia/Tokyo", "Asia/Kolkata",
        "Australia/Sydney", "America/Sao_Paulo",
    ]
    statuses = ["active", "active", "active", "inactive", "pending"]
    plans = ["free", "basic", "pro", "enterprise"]
    plan_spend = {"free": 0.0, "basic": 9.99, "pro": 29.99, "enterprise": 99.99}
    domains = ["gmail.com", "yahoo.com", "outlook.com", "example.com", "company.io"]
    referral_sources = ["organic", "social", "email", "paid_search", "referral", "direct"]
    device_types = ["mobile", "desktop", "tablet"]
    browsers = ["Chrome", "Safari", "Firefox", "Edge"]
    themes = ["dark", "light", "system"]
    languages = ["en", "es", "fr", "de", "ja", "pt", "hi"]
    tags_pool = ["early-adopter", "premium", "power-user", "at-risk", "churned", "trial", "vip"]

    base_dt = datetime(2026, 4, 13, 0, 0, 0)
    records = []
    for i in range(1, n + 1):
        first = random.choice(first_names)
        last = random.choice(last_names)
        name = f"{first} {last}"
        email = f"{first.lower()}.{last.lower()}{random.randint(1, 99)}@{random.choice(domains)}"
        phone = f"+1-{random.randint(200,999)}-{random.randint(100,999)}-{random.randint(1000,9999)}"
        created_at = base_dt - timedelta(minutes=random.randint(0, 43200))
        login_count = random.randint(0, 500)
        last_login = created_at + timedelta(minutes=random.randint(1, 20000)) if login_count > 0 else None
        plan = random.choice(plans)
        metadata = {
            "preferences": {
                "theme": random.choice(themes),
                "language": random.choice(languages),
                "notifications": {
                    "email": random.choice([True, False]),
                    "sms": random.choice([True, False]),
                    "push": random.choice([True, False]),
                },
            },
            "tags": random.sample(tags_pool, k=random.randint(1, 3)),
            "scores": {
                "engagement": round(random.uniform(1.0, 10.0), 2),
                "retention": round(random.uniform(1.0, 10.0), 2),
                "satisfaction": round(random.uniform(1.0, 10.0), 2),
            },
            "last_purchase": {
                "amount": round(random.uniform(5.0, 500.0), 2),
                "currency": "USD",
                "days_ago": random.randint(1, 365),
            },
        }
        records.append({
            "id": i,
            "name": name,
            "email": email,
            "phone": phone,
            "age": random.randint(18, 72),
            "city": random.choice(cities),
            "country": random.choice(countries),
            "timezone": random.choice(timezones),
            "status": random.choice(statuses),
            "is_verified": random.random() > 0.2,
            "two_factor_enabled": random.random() > 0.6,
            "subscription_plan": plan,
            "monthly_spend": round(plan_spend[plan] * random.uniform(0.8, 1.2), 2),
            "login_count": login_count,
            "last_login": last_login.strftime("%Y-%m-%d %H:%M:%S") if last_login else None,
            "referral_source": random.choice(referral_sources),
            "device_type": random.choice(device_types),
            "browser": random.choice(browsers),
            "metadata": metadata,
            "created_at": created_at.strftime("%Y-%m-%d %H:%M:%S"),
        })
    return records


MOCK_RECORDS = _generate_mock_records(100)


def fetch_records(query: str) -> list[dict]:
    if USE_MOCK_DATA:
        log.info("Using mock data (%d records)", len(MOCK_RECORDS))
        return MOCK_RECORDS

    log.info("Connecting to database …")
    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                log.info("Executing query: %s", query)
                cur.execute(query)
                rows = [dict(row) for row in cur.fetchall()]
        log.info("Fetched %d records", len(rows))
        return rows
    except psycopg2.OperationalError as exc:
        log.error("Database connection failed: %s", exc)
        raise
    except psycopg2.Error as exc:
        log.error("Database query failed: %s", exc)
        raise


def query_by_tag(tag: str) -> list[dict]:
    """Fetch users by a JSONB metadata tag — demonstrates NoSQL-style querying."""
    log.info("Querying users with tag '%s' …", tag)
    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    "SELECT id, name, subscription_plan, metadata->'scores' AS scores "
                    "FROM users WHERE metadata->'tags' ? %s ORDER BY id",
                    (tag,)
                )
                rows = [dict(row) for row in cur.fetchall()]
        log.info("Found %d users with tag '%s'", len(rows), tag)
        return rows
    except psycopg2.Error as exc:
        log.error("JSONB tag query failed: %s", exc)
        raise


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
        SELECT id, name, email, phone, age, city, country, timezone,
               status, is_verified, two_factor_enabled, subscription_plan,
               monthly_spend, login_count, last_login, referral_source,
               device_type, browser, metadata, created_at
        FROM users
        WHERE created_at >= NOW() - INTERVAL '30 days'
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

    # Metadata stats from JSONB field
    plan_counts = {}
    verified_count = 0
    avg_engagement = 0.0
    for r in records:
        plan = r.get("subscription_plan", "unknown")
        plan_counts[plan] = plan_counts.get(plan, 0) + 1
        if r.get("is_verified"):
            verified_count += 1
        meta = r.get("metadata") or {}
        if isinstance(meta, str):
            meta = json.loads(meta)
        avg_engagement += float((meta.get("scores") or {}).get("engagement", 0))
    avg_engagement = avg_engagement / total if total else 0

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
    print("=" * 50)
    print("METADATA INSIGHTS")
    print("=" * 50)
    print(f"  Verified users       : {verified_count}/{total} ({verified_count/total*100:.1f}%)")
    print(f"  Avg engagement score : {avg_engagement:.2f}/10")
    for plan, count in sorted(plan_counts.items()):
        print(f"  Plan [{plan:<12}]: {count} users")
    print("=" * 50 + "\n")
    log.info("Pipeline complete")


if __name__ == "__main__":
    main()
