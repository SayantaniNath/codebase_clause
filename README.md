# Data Pipeline

Reads user records from PostgreSQL, publishes them to a Kafka topic, and caches them in Redis.

## Architecture

```
PostgreSQL → fetch_records → publish_to_kafka → Kafka topic
                                              → write_to_redis → Redis
```

## Prerequisites

- Python 3.10+
- PostgreSQL running on `localhost:5432`
- Kafka broker running on `localhost:9092`
- Redis running on `localhost:6379`

## Setup

```bash
# Install dependencies
pip install -r requirements.txt

# Configure environment
cp .env.example .env
# Edit .env with your actual credentials
```

## Configuration

All connection settings live at the top of `src/data_pipeline.py`:

| Variable | Default | Description |
|---|---|---|
| `DB_CONFIG` | `localhost:5432/mydb` | PostgreSQL connection |
| `KAFKA_CONFIG` | `localhost:9092` | Kafka broker address |
| `KAFKA_TOPIC` | `comsumption-pipeline-events` | Topic name |
| `REDIS_CONFIG` | `localhost:6379` | Redis connection |
| `USE_MOCK_DATA` | `False` | Use generated mock records instead of live DB |

## Running

```bash
python src/data_pipeline.py
```

With mock data (no live services needed):

```bash
# Set USE_MOCK_DATA = True in src/data_pipeline.py, then:
python src/data_pipeline.py
```

## Output

The pipeline prints a summary report on completion:

```
==================================================
PIPELINE SUMMARY REPORT
==================================================
  Records fetched      : 100
  Kafka published      : 100/100  (100.0%)
  Redis written        : 100/100  (100.0%)
  Kafka failures       : 0
  Redis failures       : 0
  Overall status       : SUCCESS
==================================================
```

## Development

```bash
pip install -r requirements-dev.txt
pytest tests/
```

## Project Structure

```
.
├── src/
│   └── data_pipeline.py   # Pipeline logic
├── tests/
│   └── __init__.py
├── .env.example           # Environment variable template
├── .gitignore
├── requirements.txt       # Runtime dependencies
└── requirements-dev.txt   # Dev/test dependencies
```
