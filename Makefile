.PHONY: help install dev-install lint format test run docker-up docker-down clean lambda-invoke lambda-baseline lambda-drift lambda-quality

help:
	@echo "Chalk and Duster - Development Commands"
	@echo ""
	@echo "Setup:"
	@echo "  install       Install production dependencies"
	@echo "  dev-install   Install development dependencies"
	@echo ""
	@echo "Development:"
	@echo "  lint          Run linting (ruff + mypy)"
	@echo "  format        Format code (black + ruff)"
	@echo "  test          Run tests"
	@echo "  run           Run FastAPI server locally"
	@echo ""
	@echo "Docker:"
	@echo "  docker-up     Start all services (PostgreSQL, LocalStack, Ollama, etc.)"
	@echo "  docker-down   Stop all services"
	@echo "  docker-logs   View logs from all services"
	@echo "  docker-build  Build the application image"
	@echo ""
	@echo "Lambda (Local Testing):"
	@echo "  lambda-baseline  Test baseline Lambda locally"
	@echo "  lambda-drift     Test drift detection Lambda locally"
	@echo "  lambda-quality   Test quality check Lambda locally"
	@echo ""
	@echo "Database:"
	@echo "  db-migrate    Run database migrations"
	@echo "  db-revision   Create new migration"
	@echo ""
	@echo "Cleanup:"
	@echo "  clean         Remove cache and build artifacts"

# Setup
install:
	pip install -e .

dev-install:
	pip install -e ".[dev]"
	pre-commit install

# Development
lint:
	ruff check src tests
	mypy src

format:
	black src tests
	ruff check --fix src tests

test:
	pytest tests/ -v --cov=chalkandduster --cov-report=term-missing

test-unit:
	pytest tests/unit -v

test-integration:
	pytest tests/integration -v

run:
	uvicorn chalkandduster.main:app --reload --host 0.0.0.0 --port 8000

# Docker
docker-up:
	docker compose up -d
	@echo "Waiting for services to be ready..."
	@sleep 10
	@echo "Services are up! Access:"
	@echo "  API:        http://localhost:8000"
	@echo "  Grafana:    http://localhost:3000 (admin/admin)"
	@echo "  Prometheus: http://localhost:9090"
	@echo "  Airflow:    http://localhost:8080 (admin/admin)"
	@echo "  LocalStack: http://localhost:4566"

docker-down:
	docker compose down

docker-logs:
	docker compose logs -f

docker-build:
	docker compose build

docker-restart:
	docker compose restart

# Database
db-migrate:
	alembic upgrade head

db-revision:
	@read -p "Migration message: " msg; \
	alembic revision --autogenerate -m "$$msg"

db-downgrade:
	alembic downgrade -1

# Cleanup
clean:
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".mypy_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".ruff_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
	rm -rf build dist .coverage htmlcov

# Lambda Local Testing (native - requires Python 3.11+)
lambda-baseline:
	python3 scripts/lambda_local.py invoke baseline --event tests/events/baseline_event.json

lambda-drift:
	python3 scripts/lambda_local.py invoke drift --event tests/events/drift_event.json

lambda-quality:
	python3 scripts/lambda_local.py invoke quality --event tests/events/quality_event.json

lambda-invoke:
	@echo "Usage: make lambda-invoke FUNCTION=<baseline|drift|quality> EVENT=<path/to/event.json>"
	python3 scripts/lambda_local.py invoke $(FUNCTION) --event $(EVENT)

# Lambda Local Testing (via Docker - recommended)
docker-lambda-baseline:
	docker compose exec api python scripts/lambda_local.py invoke baseline --event tests/events/baseline_event.json

docker-lambda-drift:
	docker compose exec api python scripts/lambda_local.py invoke drift --event tests/events/drift_event.json

docker-lambda-quality:
	docker compose exec api python scripts/lambda_local.py invoke quality --event tests/events/quality_event.json

docker-lambda-invoke:
	@echo "Usage: make docker-lambda-invoke FUNCTION=<baseline|drift|quality> EVENT=<path/to/event.json>"
	docker compose exec api python scripts/lambda_local.py invoke $(FUNCTION) --event tests/events/$(EVENT)

# Snowflake LocalStack Testing
test-snowflake-localstack:
	docker compose exec api python scripts/test_snowflake_localstack.py

# Setup test data (PostgreSQL, Secrets Manager, Snowflake)
setup-test-data:
	docker compose exec api python scripts/setup_test_data.py

# Quick commands
up: docker-up
down: docker-down
logs: docker-logs

