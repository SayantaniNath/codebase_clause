# Data Pipeline

A production-style data pipeline that reads user records from PostgreSQL, publishes them as events to Apache Kafka, and caches them in Redis — with JSONB NoSQL support for flexible metadata querying.

## Tech Stack

### PostgreSQL — Source Database
- **Role:** Stores 100 user records with 19 structured fields + a `metadata` JSONB column
- **Why:** Reliable, ACID-compliant, and supports both relational SQL and NoSQL-style JSONB queries
- **Library:** `psycopg2-binary`
- **Key feature:** JSONB column enables MongoDB-style queries:
  ```sql
  SELECT name FROM users WHERE metadata->'tags' ? 'vip';
  SELECT name FROM users WHERE (metadata->'scores'->>'engagement')::float > 9.0;
  ```

### Apache Kafka — Message Broker
- **Role:** Receives user records as JSON events on the `comsumption-pipeline-events` topic
- **Why:** Decouples data producers from consumers, handles high-volume event streams
- **Library:** `confluent-kafka`
- **Key feature:** Retry logic with exponential backoff on publish failures

### Apache Zookeeper — Kafka Coordinator
- **Role:** Manages Kafka broker metadata and leader election
- **Why:** Required by Kafka for distributed coordination
- **Runs as:** Docker container

### Redis — Cache Layer
- **Role:** Caches user records as hashes with a 1-hour TTL for fast lookups
- **Why:** Sub-millisecond reads, ideal for caching frequently accessed data
- **Library:** `redis`
- **Key feature:** Pipeline batch writes for performance (`user:1`, `user:2`, ...)

### Python — Pipeline Language
- **Version:** 3.10+
- **Why:** Simple, readable, rich ecosystem for data engineering

| Library | Purpose |
|---|---|
| `psycopg2-binary` | Connect to PostgreSQL |
| `confluent-kafka` | Publish messages to Kafka |
| `redis` | Write and cache records in Redis |
| `python-dotenv` | Auto-load `.env` config |

### Docker — Local Service Orchestration
- **Role:** Runs Kafka, Zookeeper, and Redis as containers
- **Why:** Consistent, reproducible local development environment
- **Key file:** `docker-compose.yml` — one command starts all services

### GitHub — Version Control
- **Role:** Hosts the codebase, tracks changes via commits and pull requests
- **Workflow:** Feature branches → PR → merge to main

---

## Architecture

```
┌─────────────┐     SQL query      ┌─────────────┐
│  PostgreSQL │ ─────────────────► │   Python    │
│  (source)   │   fetch records    │  Pipeline   │
└─────────────┘                    └──────┬──────┘
                                          │
                         ┌────────────────┴─────────────────┐
                         ▼                                   ▼
                  ┌─────────────┐                   ┌──────────────┐
                  │    Kafka    │                   │    Redis     │
                  │  (events)   │                   │   (cache)    │
                  └─────────────┘                   └──────────────┘
```

### Data Flow

```
PostgreSQL row
    │
    ▼
Python dict (19 fields + JSONB metadata)
    │
    ├──► JSON string ──► Kafka message  (key = user id)
    │
    └──► Redis hash  ──► key: user:1, user:2 ... (TTL 3600s)
```

### Pipeline Steps

1. **Fetch** — Connect to PostgreSQL, run SQL query, retrieve user records
2. **Create Topic** — Ensure Kafka topic exists (idempotent)
3. **Publish** — Serialize each record to JSON and publish to Kafka
4. **Cache** — Write records to Redis as hashes with 1hr TTL
5. **Report** — Print summary with success rates and metadata insights

---

## Project Structure

```
.
├── src/
│   ├── __init__.py
│   └── data_pipeline.py     # Pipeline logic
├── tests/
│   └── __init__.py
├── .env                     # Local credentials (gitignored)
├── .env.example             # Credentials template
├── .gitignore
├── docker-compose.yml       # Local services (Postgres, Kafka, Redis)
├── Makefile                 # Common commands
├── requirements.txt         # Runtime dependencies
└── requirements-dev.txt     # Dev/test dependencies
```

---

## Quick Start

```bash
# 1. Clone and enter the project
cd codebase_clause

# 2. Install dependencies
pip install -r requirements.txt

# 3. Configure environment
cp .env.example .env
# Edit .env with your credentials if needed

# 4. Start local services
docker-compose up -d

# 5. Run the pipeline
python src/data_pipeline.py
```

---

## Environment Variables

All configuration is via environment variables. Copy `.env.example` to `.env` and adjust:

| Variable | Default | Description |
|---|---|---|
| `DB_HOST` | `localhost` | PostgreSQL host |
| `DB_PORT` | `5432` | PostgreSQL port |
| `DB_NAME` | `mydb` | Database name |
| `DB_USER` | `myuser` | Database user |
| `DB_PASSWORD` | `mypassword` | Database password |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker address |
| `REDIS_HOST` | `localhost` | Redis host |
| `REDIS_PORT` | `6379` | Redis port |
| `REDIS_DB` | `0` | Redis database index |

---

## User Record Fields

Each record fetched from PostgreSQL and published to Kafka contains:

| Field | Type | Description |
|---|---|---|
| `id` | int | Unique user ID |
| `name` | str | Full name |
| `email` | str | Email address |
| `phone` | str | Phone number |
| `age` | int | Age |
| `city` | str | City |
| `country` | str | Country code |
| `timezone` | str | IANA timezone |
| `status` | str | `active`, `inactive`, or `pending` |
| `is_verified` | bool | Email verified |
| `two_factor_enabled` | bool | 2FA enabled |
| `subscription_plan` | str | `free`, `basic`, `pro`, or `enterprise` |
| `monthly_spend` | float | Monthly spend in USD |
| `login_count` | int | Total login count |
| `last_login` | timestamp | Last login time |
| `referral_source` | str | Acquisition channel |
| `device_type` | str | `mobile`, `desktop`, or `tablet` |
| `browser` | str | Browser name |
| `metadata` | JSONB | Nested JSON: preferences, tags, scores, last_purchase |
| `created_at` | timestamp | Account creation time |

### JSONB Metadata Structure

```json
{
  "preferences": {
    "theme": "dark",
    "language": "en",
    "notifications": { "email": true, "sms": false, "push": true }
  },
  "tags": ["vip", "premium"],
  "scores": { "engagement": 9.74, "retention": 7.2, "satisfaction": 8.1 },
  "last_purchase": { "amount": 149.99, "currency": "USD", "days_ago": 12 }
}
```

---

## Make Commands

```bash
make install      # Install runtime dependencies
make install-dev  # Install dev + runtime dependencies
make run          # Run the pipeline (uses .env)
make run-mock     # Run with mock data (no live services needed)
make test         # Run tests
make lint         # Lint with ruff
make up           # Start Docker services
make down         # Stop Docker services
```

---

## Output

```
==================================================
PIPELINE SUMMARY REPORT
==================================================
  Records fetched      : 93
  Kafka published      : 93/93  (100.0%)
  Redis written        : 93/93  (100.0%)
  Kafka failures       : 0
  Redis failures       : 0
  Overall status       : SUCCESS
==================================================
METADATA INSIGHTS
==================================================
  Verified users       : 82/93 (88.2%)
  Avg engagement score : 5.17/10
  Plan [basic        ]: 21 users
  Plan [enterprise   ]: 25 users
  Plan [free         ]: 21 users
  Plan [pro          ]: 26 users
==================================================
```

---

## Development

```bash
# Install dev dependencies
make install-dev

# Run tests
make test

# Lint
make lint
```
