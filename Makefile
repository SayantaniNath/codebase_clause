.PHONY: install install-dev run run-mock test lint up down

install:
	pip install -r requirements.txt

install-dev:
	pip install -r requirements-dev.txt

run:
	python src/data_pipeline.py

run-mock:
	USE_MOCK_DATA=true python src/data_pipeline.py

test:
	pytest tests/ -v

lint:
	ruff check src/ tests/

up:
	docker-compose up -d

down:
	docker-compose down
