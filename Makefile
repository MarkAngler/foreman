.PHONY: help install venv deps unit smoke validate deploy bootstrap migrate run-app fmt lint clean

PY := .venv/Scripts/python.exe

help:
	@echo "foreman dev tasks"
	@echo ""
	@echo "  make venv         create local virtualenv"
	@echo "  make deps         install runtime + dev dependencies into the venv"
	@echo "  make unit         run unit tests (no Databricks needed)"
	@echo "  make smoke        run spine smoke test (needs deployed bundle + auth)"
	@echo "  make validate     databricks bundle validate"
	@echo "  make bootstrap    run install/bootstrap.py (Lakebase + VS endpoint)"
	@echo "  make deploy       databricks bundle deploy"
	@echo "  make migrate      databricks bundle run schema_init"
	@echo "  make run-app      databricks bundle run foreman_app"
	@echo "  make install      one-shot full install"
	@echo "  make fmt          ruff format"
	@echo "  make lint         ruff check"
	@echo "  make clean        remove caches"

venv:
	python -m venv .venv

deps:
	$(PY) -m pip install --upgrade pip
	$(PY) -m pip install -r requirements.txt pytest pytest-asyncio ruff

unit:
	PYTHONPATH=src $(PY) -m pytest tests/unit/ -v

smoke:
	PYTHONPATH=src $(PY) -m pytest tests/spine_smoke.py -s -v

validate:
	databricks bundle validate

bootstrap:
	$(PY) install/bootstrap.py

deploy:
	databricks bundle deploy

migrate:
	databricks bundle run schema_init

run-app:
	databricks bundle run foreman_app

install:
	$(PY) install/install.py

fmt:
	$(PY) -m ruff format src tests install schema

lint:
	$(PY) -m ruff check src tests install schema

clean:
	rm -rf .pytest_cache .ruff_cache .mypy_cache .coverage
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
