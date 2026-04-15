.PHONY: install install-dev producer consumer api test lint up down

install:
	pip install -r requirements.txt

install-dev:
	pip install -r requirements-dev.txt

# Run the producer — reads from PostgreSQL and publishes to Kafka
producer:
	python src/producer.py

# Run the consumer — reads from Kafka and writes to Redis
consumer:
	python src/consumer.py

# Run the GraphQL API — serves data from Redis to consumers
api:
	uvicorn src.api:app --reload --port 8000

test:
	pytest tests/ -v

lint:
	ruff check src/ tests/

up:
	docker-compose up -d

down:
	docker-compose down
