"""
data_pipeline.py
----------------
This is the main pipeline script. It does 4 things in order:
  1. Reads user records from a PostgreSQL database
  2. Creates a Kafka topic (a channel to send messages)
  3. Publishes each user record as a message to Kafka
  4. Caches each user record in Redis (fast in-memory storage)

Think of it like a conveyor belt:
  PostgreSQL (source) --> Python (processor) --> Kafka (events) + Redis (cache)
"""

# ===========================================================================
# IMPORTS — bringing in external libraries we need
# ===========================================================================

import json        # used to convert Python dictionaries to JSON strings
import logging     # used to print informational messages with timestamps
import os          # used to read environment variables (like DB password)
import time        # used to pause/wait between retries

# python-dotenv: automatically loads variables from the .env file
# so we don't have to manually run "source .env" every time
from dotenv import load_dotenv
load_dotenv()  # this must run before we read any os.environ variables

import psycopg2                    # library to connect and talk to PostgreSQL
import psycopg2.extras             # extra utilities like RealDictCursor (returns rows as dicts)
import redis                       # library to connect and talk to Redis
from confluent_kafka import Producer, KafkaException          # Kafka message producer
from confluent_kafka.admin import AdminClient, NewTopic       # Kafka admin tools (create topics)


# ===========================================================================
# LOGGING SETUP
# Logging prints messages to the terminal so we can see what the pipeline
# is doing at each step. Format: timestamp + level (INFO/ERROR) + message
# ===========================================================================
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)  # creates a logger for this specific file


# ===========================================================================
# CONFIGURATION
# All connection settings are read from environment variables.
# The .env file stores these values (e.g. DB_HOST=localhost).
# os.environ.get("KEY", "default") means: read KEY from .env, or use "default"
# ===========================================================================

# PostgreSQL connection settings
DB_CONFIG = {
    "host": os.environ.get("DB_HOST", "localhost"),       # where the database is running
    "port": int(os.environ.get("DB_PORT", 5432)),         # PostgreSQL default port is 5432
    "dbname": os.environ.get("DB_NAME", "mydb"),          # name of the database
    "user": os.environ.get("DB_USER", "myuser"),          # database username
    "password": os.environ.get("DB_PASSWORD", "mypassword"),  # database password
}

# Kafka connection settings
KAFKA_CONFIG = {
    "bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    # "bootstrap.servers" tells the Kafka client where the Kafka broker is running
}

# Kafka topic settings
# A "topic" is like a named channel where messages are sent and received
KAFKA_TOPIC = "comsumption-pipeline-events"  # name of the Kafka topic
KAFKA_TOPIC_PARTITIONS = 3    # splits the topic into 3 parts for parallel processing
KAFKA_TOPIC_REPLICATION = 1   # how many copies of data to keep (1 = no replication, fine for local dev)

# Redis connection settings
REDIS_CONFIG = {
    "host": os.environ.get("REDIS_HOST", "localhost"),    # where Redis is running
    "port": int(os.environ.get("REDIS_PORT", 6379)),      # Redis default port is 6379
    "db": int(os.environ.get("REDIS_DB", 0)),             # Redis supports multiple databases (0-15)
    "decode_responses": True,  # automatically converts bytes to strings when reading
}

# Redis key settings
REDIS_KEY_PREFIX = "user:"       # each user is stored with key like "user:1", "user:2", etc.
REDIS_TTL_SECONDS = 3600         # TTL = Time To Live — data auto-deletes after 1 hour (3600 seconds)


# ===========================================================================
# STEP 1 — READ RECORDS FROM THE DATABASE
# ===========================================================================

# If USE_MOCK_DATA is True, we skip the real database and use fake generated data.
# Useful for testing without needing a running database.
USE_MOCK_DATA = False  # Set to True to use mock data, False to use real PostgreSQL


def _generate_mock_records(n: int = 100) -> list[dict]:
    """
    Generates 'n' fake user records for testing purposes.
    Each record is a Python dictionary with realistic-looking user data.
    The underscore prefix (_) means this is a private/internal function.
    """
    # These imports are only needed inside this function, so we put them here
    import random
    from datetime import datetime, timedelta

    # Lists of sample values to randomly pick from
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
    statuses = ["active", "active", "active", "inactive", "pending"]  # "active" appears 3x = more likely to be picked
    plans = ["free", "basic", "pro", "enterprise"]
    plan_spend = {"free": 0.0, "basic": 9.99, "pro": 29.99, "enterprise": 99.99}  # base price per plan
    domains = ["gmail.com", "yahoo.com", "outlook.com", "example.com", "company.io"]
    referral_sources = ["organic", "social", "email", "paid_search", "referral", "direct"]
    device_types = ["mobile", "desktop", "tablet"]
    browsers = ["Chrome", "Safari", "Firefox", "Edge"]
    themes = ["dark", "light", "system"]
    languages = ["en", "es", "fr", "de", "ja", "pt", "hi"]
    tags_pool = ["early-adopter", "premium", "power-user", "at-risk", "churned", "trial", "vip"]

    # Base date to generate timestamps relative to
    base_dt = datetime(2026, 4, 13, 0, 0, 0)
    records = []  # start with an empty list, we'll add records one by one

    # Loop from 1 to n (e.g. 1 to 100) to create n records
    for i in range(1, n + 1):
        # Pick random first and last name, combine them
        first = random.choice(first_names)
        last = random.choice(last_names)
        name = f"{first} {last}"

        # Build a realistic-looking email from the name
        email = f"{first.lower()}.{last.lower()}{random.randint(1, 99)}@{random.choice(domains)}"

        # Build a US phone number in format +1-XXX-XXX-XXXX
        phone = f"+1-{random.randint(200,999)}-{random.randint(100,999)}-{random.randint(1000,9999)}"

        # created_at = some time in the past 30 days (43200 minutes = 30 days)
        created_at = base_dt - timedelta(minutes=random.randint(0, 43200))

        # login_count = how many times the user has logged in
        login_count = random.randint(0, 500)

        # last_login = only set if the user has logged in at least once
        last_login = created_at + timedelta(minutes=random.randint(1, 20000)) if login_count > 0 else None

        plan = random.choice(plans)

        # metadata is a nested JSON object — this is the NoSQL part
        # stored as JSONB in PostgreSQL, allowing flexible querying
        metadata = {
            "preferences": {
                "theme": random.choice(themes),        # UI theme preference
                "language": random.choice(languages),  # preferred language
                "notifications": {
                    "email": random.choice([True, False]),  # email notifications on/off
                    "sms": random.choice([True, False]),    # SMS notifications on/off
                    "push": random.choice([True, False]),   # push notifications on/off
                },
            },
            # random.sample picks k unique items from the list
            "tags": random.sample(tags_pool, k=random.randint(1, 3)),
            "scores": {
                # round(x, 2) rounds to 2 decimal places
                "engagement": round(random.uniform(1.0, 10.0), 2),   # how engaged the user is
                "retention": round(random.uniform(1.0, 10.0), 2),    # likelihood to stay
                "satisfaction": round(random.uniform(1.0, 10.0), 2), # how happy they are
            },
            "last_purchase": {
                "amount": round(random.uniform(5.0, 500.0), 2),  # purchase amount in USD
                "currency": "USD",
                "days_ago": random.randint(1, 365),              # how many days since purchase
            },
        }

        # Add the completed record dictionary to the list
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
            "is_verified": random.random() > 0.2,         # 80% chance of being verified
            "two_factor_enabled": random.random() > 0.6,  # 40% chance of 2FA enabled
            "subscription_plan": plan,
            # multiply base price by a small random factor to simulate price variation
            "monthly_spend": round(plan_spend[plan] * random.uniform(0.8, 1.2), 2),
            "login_count": login_count,
            # strftime formats a datetime object into a readable string
            "last_login": last_login.strftime("%Y-%m-%d %H:%M:%S") if last_login else None,
            "referral_source": random.choice(referral_sources),
            "device_type": random.choice(device_types),
            "browser": random.choice(browsers),
            "metadata": metadata,
            "created_at": created_at.strftime("%Y-%m-%d %H:%M:%S"),
        })

    return records  # return the full list of generated records


# Generate mock records once when the script loads (used if USE_MOCK_DATA = True)
MOCK_RECORDS = _generate_mock_records(100)


def fetch_records(query: str) -> list[dict]:
    """
    Fetches user records from PostgreSQL using the given SQL query.
    Returns a list of dictionaries, one per row.
    If USE_MOCK_DATA is True, returns fake data instead.
    """
    # If mock mode is on, skip the database and return fake records
    if USE_MOCK_DATA:
        log.info("Using mock data (%d records)", len(MOCK_RECORDS))
        return MOCK_RECORDS

    log.info("Connecting to database …")
    try:
        # "with" statement automatically closes the connection when done
        # psycopg2.connect(**DB_CONFIG) unpacks the dictionary as keyword arguments
        with psycopg2.connect(**DB_CONFIG) as conn:
            # RealDictCursor makes each row return as a dictionary {column: value}
            # instead of a plain tuple (value1, value2, ...)
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                log.info("Executing query: %s", query)
                cur.execute(query)           # run the SQL query
                rows = [dict(row) for row in cur.fetchall()]  # fetch all rows and convert to dicts
        log.info("Fetched %d records", len(rows))
        return rows

    except psycopg2.OperationalError as exc:
        # OperationalError = could not connect (wrong host, port, credentials, DB not running)
        log.error("Database connection failed: %s", exc)
        raise  # re-raise the error so the pipeline stops and reports the failure

    except psycopg2.Error as exc:
        # Generic database error (bad SQL, table not found, permission denied, etc.)
        log.error("Database query failed: %s", exc)
        raise


def query_by_tag(tag: str) -> list[dict]:
    """
    NoSQL-style query — finds users by a tag stored inside the JSONB metadata column.
    Example: query_by_tag("vip") returns all users tagged as "vip".

    This demonstrates that PostgreSQL can behave like a NoSQL database
    by querying inside JSON fields using the -> and ? operators.
    """
    log.info("Querying users with tag '%s' …", tag)
    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    # metadata->'tags' accesses the "tags" array inside the JSON column
                    # ? operator checks if the tag exists in the array
                    "SELECT id, name, subscription_plan, metadata->'scores' AS scores "
                    "FROM users WHERE metadata->'tags' ? %s ORDER BY id",
                    (tag,)  # %s is safely replaced with the tag value (prevents SQL injection)
                )
                rows = [dict(row) for row in cur.fetchall()]
        log.info("Found %d users with tag '%s'", len(rows), tag)
        return rows
    except psycopg2.Error as exc:
        log.error("JSONB tag query failed: %s", exc)
        raise


# ===========================================================================
# STEP 2 — CREATE KAFKA TOPIC
# A Kafka "topic" is like a named inbox where messages are sent.
# This function creates the topic if it doesn't already exist.
# ===========================================================================

def create_kafka_topic(topic: str, num_partitions: int, replication_factor: int) -> None:
    """
    Creates a Kafka topic if it doesn't already exist.
    This is "idempotent" — safe to call multiple times, won't create duplicates.
    """
    # Skip if running in mock mode
    if USE_MOCK_DATA:
        log.info("[MOCK] Skipping Kafka topic creation for '%s'", topic)
        return

    # AdminClient is used for managing Kafka (creating/deleting topics, etc.)
    admin = AdminClient(KAFKA_CONFIG)

    # Get list of all existing topics
    existing = admin.list_topics(timeout=10).topics

    # If topic already exists, skip creation
    if topic in existing:
        log.info("Kafka topic '%s' already exists — skipping creation", topic)
        return

    # Define the new topic with its settings
    new_topic = NewTopic(
        topic,
        num_partitions=num_partitions,         # how many partitions (parallel lanes)
        replication_factor=replication_factor,  # how many copies to keep
    )

    # create_topics returns a dictionary of {topic_name: Future}
    # A Future is a result that will be available later (async operation)
    futures = admin.create_topics([new_topic])
    for t, future in futures.items():
        try:
            future.result()  # wait for the topic creation to complete
            log.info("Created Kafka topic '%s'", t)
        except Exception as exc:
            log.error("Failed to create topic '%s': %s", t, exc)
            raise


# ===========================================================================
# STEP 3 — PUBLISH RECORDS TO KAFKA
# Each user record is converted to JSON and sent as a Kafka message.
# ===========================================================================

KAFKA_MAX_RETRIES = 3      # try up to 3 times before giving up
KAFKA_RETRY_BACKOFF = 2    # wait 2 seconds between retries (doubles each attempt)

# Tracks records that failed to deliver (filled by the delivery_report callback)
failed_records: list[dict] = []


def delivery_report(err, msg):
    """
    Callback function — Kafka calls this automatically after each message
    is either successfully delivered or fails.
    We use it to track failures.
    """
    if err:
        # Delivery failed — log the error and record the failed key
        log.error("Delivery failed for key %s: %s", msg.key(), err)
        failed_records.append(msg.key())
    else:
        # Delivery succeeded — log at DEBUG level (not shown unless debug mode is on)
        log.debug("Delivered to %s [partition %d] offset %d", msg.topic(), msg.partition(), msg.offset())


def produce_with_retry(producer: Producer, topic: str, key: str, value: str) -> None:
    """
    Sends a single message to Kafka with retry logic.
    If it fails, it waits and tries again (up to KAFKA_MAX_RETRIES times).
    The wait time doubles each attempt — this is called "exponential backoff".
    Example: wait 2s, then 4s, then 8s before giving up.
    """
    for attempt in range(1, KAFKA_MAX_RETRIES + 1):
        try:
            # produce() sends the message to Kafka
            # key = user id (used to route to the same partition consistently)
            # value = the full user record as a JSON string
            # callback = function to call when delivery is confirmed or fails
            producer.produce(topic, key=key, value=value, callback=delivery_report)
            producer.poll(0)  # poll(0) triggers delivery callbacks without waiting
            return           # message sent successfully — exit the function

        except BufferError:
            # BufferError means the producer's internal queue is full
            # Solution: flush (wait for all queued messages to be sent) then retry
            log.warning("Producer queue full (attempt %d/%d) — flushing and retrying …", attempt, KAFKA_MAX_RETRIES)
            producer.flush()

        except KafkaException as exc:
            # Calculate how long to wait before retrying
            # attempt 1: 2 * 2^0 = 2s, attempt 2: 2 * 2^1 = 4s, attempt 3: give up
            wait = KAFKA_RETRY_BACKOFF * (2 ** (attempt - 1))
            if attempt < KAFKA_MAX_RETRIES:
                log.warning("KafkaException on attempt %d/%d: %s — retrying in %ds …", attempt, KAFKA_MAX_RETRIES, exc, wait)
                time.sleep(wait)  # wait before retrying
            else:
                # All retries exhausted — give up and raise the error
                log.error("KafkaException after %d attempts for key %s: %s — giving up", KAFKA_MAX_RETRIES, key, exc)
                raise


def publish_to_kafka(topic: str, records: list[dict]) -> int:
    """
    Publishes all records to Kafka.
    Returns the number of successfully published records.
    """
    # Skip if running in mock mode — just log and return
    if USE_MOCK_DATA:
        for record in records:
            log.info("[MOCK] Published record id=%s to topic '%s'", record.get("id"), topic)
        return len(records)

    # Create a Kafka producer using the config (bootstrap server address)
    producer = Producer(KAFKA_CONFIG)
    log.info("Publishing %d records to topic '%s' …", len(records), topic)

    skipped = 0  # count of records that failed to send

    for record in records:
        key = str(record.get("id", ""))         # use user ID as message key
        value = json.dumps(record, default=str)  # convert dict to JSON string
        # default=str handles non-serializable types (like datetime) by converting to string

        try:
            produce_with_retry(producer, topic, key, value)
        except KafkaException:
            # If all retries failed, skip this record and continue with the rest
            log.error("Skipping record id=%s after exhausting retries", key)
            skipped += 1
            continue  # move to the next record

    try:
        # flush() waits for all pending messages to be delivered
        # timeout=30 means wait up to 30 seconds
        remaining = producer.flush(timeout=30)
        if remaining > 0:
            # Some messages didn't deliver within the timeout
            log.warning("%d message(s) were not delivered after flush timeout", remaining)
            skipped += remaining
        else:
            log.info("All records published to Kafka")
    except KafkaException as exc:
        log.error("Fatal error during final flush: %s", exc)
        raise

    # If any messages failed via the delivery callback, count them
    if failed_records:
        log.warning("%d message(s) reported delivery failure via callback: %s", len(failed_records), failed_records)
        skipped += len(failed_records)

    # Return total records minus the ones that failed
    return len(records) - skipped


# ===========================================================================
# STEP 4 — WRITE RECORDS TO REDIS
# Redis is used as a fast cache. Each user record is stored as a Redis hash
# (similar to a dictionary) with an automatic expiry time (TTL).
# ===========================================================================

def write_to_redis(records: list[dict], key_prefix: str, ttl: int) -> int:
    """
    Writes all records to Redis as hashes.
    Each record is stored under a key like "user:1", "user:2", etc.
    Records automatically expire after ttl seconds.
    Returns the number of successfully written records.
    """
    # Skip if running in mock mode
    if USE_MOCK_DATA:
        for record in records:
            log.info("[MOCK] Wrote key '%s%s' to Redis (TTL=%ds)", key_prefix, record.get("id"), ttl)
        return len(records)

    # Create a Redis client using the config
    client = redis.Redis(**REDIS_CONFIG)

    # A pipeline batches multiple commands together and sends them in one go
    # Much faster than sending each command individually
    pipe = client.pipeline()

    log.info("Writing %d records to Redis …", len(records))
    for record in records:
        # Build the Redis key e.g. "user:42"
        redis_key = f"{key_prefix}{record.get('id', 'unknown')}"

        # hset stores the record as a hash (field:value pairs)
        # We convert all values to strings because Redis stores everything as strings
        pipe.hset(redis_key, mapping={k: str(v) for k, v in record.items()})

        # expire sets the TTL — after this many seconds, Redis auto-deletes the key
        pipe.expire(redis_key, ttl)

    try:
        pipe.execute()  # send all the queued commands to Redis at once
        log.info("All records written to Redis with TTL=%ds", ttl)
        return len(records)
    except Exception as exc:
        log.error("Redis pipeline failed: %s", exc)
        return 0  # return 0 to indicate nothing was written


# ===========================================================================
# MAIN FUNCTION — ties all 4 steps together
# This is the entry point of the pipeline.
# ===========================================================================

def main():
    """
    Runs the full pipeline:
      1. Fetch records from PostgreSQL
      2. Create Kafka topic
      3. Publish records to Kafka
      4. Cache records in Redis
      5. Print a summary report
    """

    # SQL query to fetch users created in the last 30 days
    # NOW() = current timestamp
    # INTERVAL '30 days' = 30 days duration
    # ORDER BY created_at DESC = newest records first
    # LIMIT 1000 = fetch at most 1000 records
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

    # --- Step 1: Fetch records from the database ---
    records = fetch_records(query)
    if not records:
        log.info("No records found — exiting")
        return  # stop the pipeline early if there's nothing to process

    # --- Step 2: Make sure the Kafka topic exists ---
    create_kafka_topic(KAFKA_TOPIC, KAFKA_TOPIC_PARTITIONS, KAFKA_TOPIC_REPLICATION)

    # --- Step 3: Publish all records to Kafka ---
    kafka_success = publish_to_kafka(KAFKA_TOPIC, records)

    # --- Step 4: Cache all records in Redis ---
    redis_success = write_to_redis(records, REDIS_KEY_PREFIX, REDIS_TTL_SECONDS)

    # --- Step 5: Calculate and print summary ---
    total = len(records)

    # Loop through all records to gather metadata statistics
    plan_counts = {}    # dictionary to count users per subscription plan
    verified_count = 0  # count of verified users
    avg_engagement = 0.0  # sum of engagement scores (we'll divide at the end)

    for r in records:
        # Count how many users are on each plan
        plan = r.get("subscription_plan", "unknown")
        plan_counts[plan] = plan_counts.get(plan, 0) + 1

        # Count verified users
        if r.get("is_verified"):
            verified_count += 1

        # Extract engagement score from the nested metadata JSON
        meta = r.get("metadata") or {}
        if isinstance(meta, str):
            meta = json.loads(meta)  # if stored as a string, parse it back to a dict
        avg_engagement += float((meta.get("scores") or {}).get("engagement", 0))

    # Calculate the average by dividing total by number of records
    avg_engagement = avg_engagement / total if total else 0

    # Print the summary report
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


# ===========================================================================
# ENTRY POINT
# This block runs only when you execute this file directly:
#   python src/data_pipeline.py
# It does NOT run if this file is imported by another Python file.
# ===========================================================================
if __name__ == "__main__":
    main()
