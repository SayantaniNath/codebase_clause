# Data Pipeline

Reads user records from PostgreSQL, publishes them to a Kafka topic, and caches them in Redis.

## Architecture

```
PostgreSQL ──► fetch_records ──► publish_to_kafka ──► Kafka
                                └──► write_to_redis ──► Redis
```

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

## Prerequisites

- Python 3.10+
- Docker (for running services locally)

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
| `created_at` | timestamp | Account creation time |

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

## Mock Mode

To run without any live services, set `USE_MOCK_DATA = True` in `src/data_pipeline.py`:

```bash
make run-mock
```

This generates 100 realistic user records locally and simulates Kafka/Redis writes.

## Output

```
==================================================
PIPELINE SUMMARY REPORT
==================================================
  Records fetched      : 98
  Kafka published      : 98/98  (100.0%)
  Redis written        : 98/98  (100.0%)
  Kafka failures       : 0
  Redis failures       : 0
  Overall status       : SUCCESS
==================================================
```

## Development

```bash
# Install dev dependencies
make install-dev

# Run tests
make test

# Lint
make lint
```
