.PHONY: setup install download load dbt-deps dbt-seed dbt-run dbt-test dbt-docs \
        quality pipeline clean all

# ── Environment ──────────────────────────────────────────────────────────────
setup:
	python -m venv .venv
	. .venv/bin/activate && pip install -e ".[dev]"

install:
	pip install -e ".[dev]"

# ── Ingestion ─────────────────────────────────────────────────────────────────
download:
	python -m ingestion.download_brfss

load:
	python -m ingestion.load_to_duckdb

# ── dbt ───────────────────────────────────────────────────────────────────────
dbt-deps:
	cd dbt_brfss && dbt deps

dbt-seed:
	cd dbt_brfss && dbt seed

dbt-run:
	cd dbt_brfss && dbt run

dbt-test:
	cd dbt_brfss && dbt test --store-failures

dbt-docs:
	cd dbt_brfss && dbt docs generate && dbt docs serve

dbt-compile:
	cd dbt_brfss && dbt compile

# ── Quality ───────────────────────────────────────────────────────────────────
quality:
	python -m pytest tests/ -v
	cd dbt_brfss && dbt test --store-failures

# ── Orchestration ─────────────────────────────────────────────────────────────
prefect-start:
	prefect server start

pipeline:
	python -m flows.brfss_pipeline

# ── Cleanup ───────────────────────────────────────────────────────────────────
clean-dbt:
	cd dbt_brfss && dbt clean

clean-data:
	rm -f data/duckdb/brfss.duckdb
	rm -f data/raw/*.xpt data/raw/*.zip
	rm -f data/processed/*.parquet

# ── Full pipeline from scratch ────────────────────────────────────────────────
all: install download load dbt-deps dbt-seed dbt-run dbt-test
