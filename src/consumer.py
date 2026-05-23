"""
consumer.py
-----------
The Consumer reads messages from a Kafka topic, writes each user record
into Redis, and runs fraud detection on every record.

Flow:
  Kafka Topic --> poll messages --> fraud scoring --> write to Redis
                                                  --> write fraud result to Redis

Fraud results are stored under the key prefix "fraud:" so the GraphQL API
can serve them alongside normal user data.

Run with:
  python src/consumer.py
"""

import json
import logging
import os
import signal
import sys

from dotenv import load_dotenv
load_dotenv()

import redis
from confluent_kafka import Consumer, KafkaException, KafkaError

from fraud_detector import score_record, is_flagged, is_high_risk

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# Kafka — message broker
KAFKA_CONFIG = {
    "bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),

    # group.id: identifies this consumer as part of a group.
    # Kafka tracks which messages each group has already read (called "offset").
    # If you restart the consumer, it picks up from where it left off.
    "group.id": "redis-writer-group",

    # auto.offset.reset: what to do if no saved offset exists for this group.
    # "earliest" = start from the very first message in the topic
    # "latest"   = start from new messages only (ignore old ones)
    "auto.offset.reset": "earliest",
}

KAFKA_TOPIC = "org.users.events"   # topic to listen to
KAFKA_POLL_TIMEOUT = 1.0           # wait up to 1 second for a new message

# Redis — cache
REDIS_CONFIG = {
    "host": os.environ.get("REDIS_HOST", "localhost"),
    "port": int(os.environ.get("REDIS_PORT", 6379)),
    "db": int(os.environ.get("REDIS_DB", 0)),
    "decode_responses": True,
}

REDIS_KEY_PREFIX = "user:"         # user records: "user:1", "user:2", etc.
REDIS_FRAUD_PREFIX = "fraud:"      # fraud results: "fraud:1", "fraud:2", etc.
REDIS_TTL_SECONDS = 3600           # records expire after 1 hour

# ---------------------------------------------------------------------------
# Redis writers
# ---------------------------------------------------------------------------

def write_to_redis(client: redis.Redis, record: dict) -> None:
    """
    Writes a single user record to Redis as a hash.
    Key format: "user:<id>"
    """
    redis_key = f"{REDIS_KEY_PREFIX}{record.get('id', 'unknown')}"
    client.hset(redis_key, mapping={k: str(v) for k, v in record.items()})
    client.expire(redis_key, REDIS_TTL_SECONDS)
    log.debug("Cached record to Redis key '%s'", redis_key)


def write_fraud_result_to_redis(client: redis.Redis, result: dict) -> None:
    """
    Stores a fraud detection result in Redis.
    Key format: "fraud:<user_id>"
    Signals are JSON-serialised so they survive the Redis hash round-trip.
    """
    redis_key = f"{REDIS_FRAUD_PREFIX}{result['user_id']}"
    payload = {
        "user_id":      result["user_id"],
        "score":        str(result["score"]),
        "risk_level":   result["risk_level"],
        "signal_count": str(len(result["signals"])),
        "signals":      json.dumps(result["signals"]),
        "evaluated_at": result["evaluated_at"],
    }
    client.hset(redis_key, mapping=payload)
    client.expire(redis_key, REDIS_TTL_SECONDS)
    log.debug("Stored fraud result to Redis key '%s'", redis_key)

# ---------------------------------------------------------------------------
# Consumer loop
# ---------------------------------------------------------------------------

# Flag to gracefully stop the consumer on Ctrl+C
running = True

def handle_shutdown(signum, frame):
    """Called when user presses Ctrl+C — sets running=False to exit the loop cleanly."""
    global running
    log.info("Shutdown signal received — stopping consumer …")
    running = False


def run_consumer() -> None:
    """
    Main consumer loop — keeps running until stopped.
    Polls Kafka for new messages and writes them to Redis.
    """
    # Register shutdown handler for Ctrl+C (SIGINT) and kill (SIGTERM)
    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    # Create the Kafka consumer
    consumer = Consumer(KAFKA_CONFIG)

    # Subscribe to the topic — consumer will now receive messages from it
    consumer.subscribe([KAFKA_TOPIC])
    log.info("Consumer subscribed to topic '%s'", KAFKA_TOPIC)

    # Create the Redis client
    redis_client = redis.Redis(**REDIS_CONFIG)

    # Counters for the summary
    processed = 0
    failed = 0
    fraud_flagged = 0
    fraud_high_risk = 0

    log.info("Consumer is running — press Ctrl+C to stop")

    # Keep looping until shutdown signal is received
    while running:
        # poll() waits up to KAFKA_POLL_TIMEOUT seconds for a new message
        # Returns None if no message arrived in that time
        msg = consumer.poll(timeout=KAFKA_POLL_TIMEOUT)

        if msg is None:
            # No message yet — keep waiting
            continue

        if msg.error():
            # Check if we've reached the end of the partition (not a real error)
            if msg.error().code() == KafkaError._PARTITION_EOF:
                log.info("Reached end of partition — waiting for new messages …")
            else:
                # Real error — log it and continue
                log.error("Kafka consumer error: %s", msg.error())
                failed += 1
            continue

        try:
            # msg.value() returns the message as bytes — decode to string, then parse JSON
            record = json.loads(msg.value().decode("utf-8"))

            # Write the user record to Redis
            write_to_redis(redis_client, record)

            # Run fraud detection and store the result
            fraud_result = score_record(record)
            write_fraud_result_to_redis(redis_client, fraud_result)

            if is_high_risk(fraud_result):
                fraud_high_risk += 1
                log.warning(
                    "HIGH_RISK user id=%s score=%d signals=%s",
                    fraud_result["user_id"],
                    fraud_result["score"],
                    [s["rule"] for s in fraud_result["signals"]],
                )
            elif is_flagged(fraud_result):
                fraud_flagged += 1
                log.info(
                    "SUSPICIOUS user id=%s score=%d",
                    fraud_result["user_id"],
                    fraud_result["score"],
                )

            processed += 1
            log.info(
                "Processed record id=%s risk=%s | total=%d",
                record.get("id"),
                fraud_result["risk_level"],
                processed,
            )

        except json.JSONDecodeError as exc:
            # Message was not valid JSON — skip it
            log.error("Failed to decode message: %s", exc)
            failed += 1

        except redis.RedisError as exc:
            # Redis write failed — log and continue
            log.error("Failed to write to Redis: %s", exc)
            failed += 1

    # Clean up when loop exits
    consumer.close()
    log.info("Consumer stopped cleanly")

    # Final summary
    print("\n" + "=" * 50)
    print("CONSUMER SUMMARY")
    print("=" * 50)
    print(f"  Messages processed : {processed}")
    print(f"  Messages failed    : {failed}")
    print(f"  Fraud HIGH_RISK    : {fraud_high_risk}")
    print(f"  Fraud SUSPICIOUS   : {fraud_flagged}")
    print(f"  Fraud CLEAN        : {processed - fraud_high_risk - fraud_flagged}")
    print(f"  Redis key prefix   : {REDIS_KEY_PREFIX}")
    print(f"  Fraud key prefix   : {REDIS_FRAUD_PREFIX}")
    print(f"  Redis TTL          : {REDIS_TTL_SECONDS}s")
    print("=" * 50 + "\n")


if __name__ == "__main__":
    run_consumer()
