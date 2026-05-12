# ============================================================
# Merchant Reconciliation Platform — Makefile
#
# Prerequisites: Python 3.9+, Docker Desktop, Java 11+
# ============================================================

.PHONY: help install dev-install lint typecheck test test-unit test-integration \
        infra-up infra-down infra-logs register-schemas setup-snowflake \
        build deploy-staging deploy-prod clean

PYTHON := python
PIP    := pip
PYTEST := pytest

# ── Help ─────────────────────────────────────────────────────────────────────

help:
	@echo ""
	@echo "Merchant Reconciliation Platform — available targets:"
	@echo ""
	@echo "  Setup"
	@echo "    install            Install production dependencies"
	@echo "    dev-install        Install all dependencies including dev/test"
	@echo ""
	@echo "  Code quality"
	@echo "    lint               Run ruff linter"
	@echo "    fmt                Auto-format with ruff"
	@echo "    typecheck          Run mypy type checker"
	@echo ""
	@echo "  Tests"
	@echo "    test               Run all unit tests"
	@echo "    test-unit          Run unit tests only (fast, no Docker)"
	@echo "    test-integration   Run integration tests (requires Docker)"
	@echo "    test-coverage      Run unit tests with HTML coverage report"
	@echo ""
	@echo "  Infrastructure"
	@echo "    infra-up           Start local Kafka stack (docker-compose)"
	@echo "    infra-down         Stop and remove local Kafka stack"
	@echo "    infra-logs         Tail logs from all infra containers"
	@echo "    infra-reset        Destroy and recreate infra (clears all data)"
	@echo ""
	@echo "  Platform setup"
	@echo "    register-schemas   Register Avro schemas with Schema Registry"
	@echo "    setup-snowflake    Execute DDL + RBAC against Snowflake"
	@echo "    setup-snowflake-dry Run Snowflake setup in dry-run mode"
	@echo ""
	@echo "  Producers"
	@echo "    produce-txn        Start transaction producer (1000 TPS)"
	@echo "    produce-settle     Start settlement producer"
	@echo ""
	@echo "  Build & Deploy"
	@echo "    build              Build the Python wheel"
	@echo "    deploy-staging     Deploy Databricks bundle to staging"
	@echo "    deploy-prod        Deploy Databricks bundle to production"
	@echo ""
	@echo "  Cleanup"
	@echo "    clean              Remove build artefacts and caches"
	@echo ""

# ── Setup ─────────────────────────────────────────────────────────────────────

install:
	$(PIP) install -e .

dev-install:
	$(PIP) install -e ".[dev]"
	@echo "Dev dependencies installed."

# ── Code quality ──────────────────────────────────────────────────────────────

lint:
	ruff check . --output-format=concise

fmt:
	ruff format .
	ruff check . --fix

typecheck:
	mypy pipeline/ producer/ --ignore-missing-imports

# ── Tests ─────────────────────────────────────────────────────────────────────

test: test-unit

test-unit:
	$(PYTEST) tests/unit/ -m unit -v --tb=short

test-integration:
	$(PYTEST) tests/integration/ -m integration -v --tb=short --timeout=120

test-coverage:
	$(PYTEST) tests/unit/ -m unit \
		--cov=pipeline \
		--cov=producer \
		--cov-report=html:htmlcov \
		--cov-report=term-missing
	@echo "Coverage report: htmlcov/index.html"

test-all:
	$(PYTEST) tests/ -v --tb=short

# ── Infrastructure ────────────────────────────────────────────────────────────

infra-up:
	docker-compose up -d
	@echo ""
	@echo "Kafka stack starting up..."
	@echo "  Kafka UI:       http://localhost:8080"
	@echo "  Schema Registry: http://localhost:8081"
	@echo "  Kafka broker:   localhost:9092"
	@echo ""
	@echo "Wait ~20s for all containers to be healthy."

infra-down:
	docker-compose down

infra-logs:
	docker-compose logs -f

infra-reset:
	docker-compose down -v
	docker-compose up -d

infra-status:
	docker-compose ps

# ── Platform setup ────────────────────────────────────────────────────────────

register-schemas:
	$(PYTHON) scripts/register_schemas.py

setup-snowflake:
	$(PYTHON) scripts/setup_snowflake.py

setup-snowflake-dry:
	$(PYTHON) scripts/setup_snowflake.py --dry-run

# ── Producers ─────────────────────────────────────────────────────────────────

produce-txn:
	$(PYTHON) -m producer.transaction_producer

produce-settle:
	$(PYTHON) -m producer.settlement_producer

# ── Build & Deploy ────────────────────────────────────────────────────────────

build:
	$(PIP) install build
	$(PYTHON) -m build --wheel
	@echo "Wheel built in dist/"

deploy-staging: build
	databricks bundle deploy --target staging

deploy-prod: build
	@echo "Deploying to PRODUCTION — this affects live data."
	@read -p "Confirm? [y/N]: " confirm && [ "$$confirm" = "y" ]
	databricks bundle deploy --target production

# ── Cleanup ───────────────────────────────────────────────────────────────────

clean:
	rm -rf dist/ build/ *.egg-info htmlcov/ .coverage coverage.xml
	find . -type d -name __pycache__ -not -path './.venv/*' -exec rm -rf {} + 2>/dev/null || true
	find . -name "*.pyc" -not -path './.venv/*' -delete
	@echo "Build artefacts cleaned."
