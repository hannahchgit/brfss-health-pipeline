# BRFSS Health Data Engineering Pipeline

An end-to-end data engineering portfolio project built on the CDC's
[Behavioral Risk Factor Surveillance System (BRFSS)](https://www.cdc.gov/brfss/) —
the world's largest ongoing telephone health survey (~400K respondents/year).

**Focus**: Diabetes and metabolic health trends across US states, 2021–2023.

## Architecture

```
CDC XPT Files
     │
     ▼
[Python Ingestion]  ──  pyreadstat + requests
     │
     ▼
[DuckDB Raw Layer]  ──  raw.brfss_{year} tables + brfss_all UNION view
     │
     ▼
[Great Expectations]  ──  Raw data validation (column presence, row counts, weight QC)
     │
     ▼
[dbt Staging]  ──  Decode codes → labels, fix BMI units, filter zero-weight rows
     │
     ▼
[dbt Intermediate]  ──  Diabetes flags, metabolic risk scores, weight diagnostics
     │
     ▼
[dbt Marts]  ──  Survey-weighted prevalence tables (Horvitz-Thompson estimator)
     │
     ▼
[dbt Tests]  ──  Schema tests + custom epidemiological sanity checks
     │
     ▼
[Prefect Flow]  ──  Orchestrates all steps with retry logic and caching
```

## Key Design Decisions

**Survey-weighted estimates**: BRFSS uses a complex stratified probability sample.
Raw percentages are biased. All prevalence figures use the Horvitz-Thompson estimator
via `_LLCPWT`. See `dbt_brfss/macros/survey_weighted_mean.sql` and
`docs/BRFSS_variables_reference.md` for details.

**DuckDB**: Local analytical database — no cloud credentials required, instant setup,
full SQL support, and direct pandas/pyreadstat integration.

**Layered dbt architecture**: `staging` → `intermediate` → `marts` ensures each
layer has a single responsibility and is independently testable.

**Custom dbt tests**: `assert_weights_sum_to_population` and
`assert_diabetes_prevalence_reasonable` encode domain knowledge as executable
data contracts, not just documentation.

## Stack

| Layer | Tool |
|-------|------|
| Ingestion | Python 3.11+, pyreadstat, requests |
| Storage | DuckDB 1.2+ |
| Transformation | dbt-core 1.9+, dbt-duckdb |
| Data Quality | Great Expectations 1.3+, dbt tests |
| Orchestration | Prefect 3.2+ |
| CI | GitHub Actions |

## Quick Start

### 1. Install dependencies

```bash
python -m venv .venv
source .venv/bin/activate
make install
```

### 2. Download BRFSS data

```bash
make download
# Downloads ~150-200 MB XPT files for 2021, 2022, 2023 from CDC
# Files are saved to data/raw/ and skipped on re-run (idempotent)
```

### 3. Load to DuckDB

```bash
make load
# Loads XPT files into data/duckdb/brfss.duckdb
# Creates raw.brfss_2021, raw.brfss_2022, raw.brfss_2023, and raw.brfss_all
```

### 4. Run dbt transformations

```bash
make dbt-deps    # Install dbt-utils package
make dbt-seed    # Load state codes and variable labels
make dbt-run     # Run all models: staging → intermediate → marts
make dbt-test    # Run schema tests and custom epidemiological checks
```

### 5. Run the full pipeline with Prefect

```bash
# Optional: start local Prefect UI at http://localhost:4200
prefect server start &

# Run the pipeline
make pipeline
```

### 6. Browse dbt docs

```bash
make dbt-docs
# Opens interactive lineage graph and documentation at http://localhost:8080
```

## Project Structure

```
brfss-health-pipeline/
├── ingestion/               # Python ingestion scripts
│   ├── config.py            # CDC URLs, column allowlist, paths
│   ├── download_brfss.py    # Downloads XPT files from CDC
│   └── load_to_duckdb.py    # Loads XPT → DuckDB raw schema
│
├── dbt_brfss/               # dbt project
│   ├── models/
│   │   ├── staging/         # 1:1 with raw; rename, decode, filter
│   │   ├── intermediate/    # Diabetes flags, metabolic risk scores
│   │   └── marts/diabetes/  # Survey-weighted prevalence tables
│   ├── macros/
│   │   └── survey_weighted_mean.sql   # Horvitz-Thompson estimator
│   ├── seeds/               # State FIPS codes, variable labels
│   └── tests/               # Custom epidemiological sanity checks
│
├── expectations/            # Great Expectations raw data validation
│   └── validate_raw.py
│
├── flows/                   # Prefect orchestration
│   ├── brfss_pipeline.py    # Main flow
│   └── tasks/               # Ingestion, transformation, quality tasks
│
├── docs/
│   ├── data_dictionary.md
│   └── BRFSS_variables_reference.md
│
└── notebooks/
    └── 01_exploratory_analysis.ipynb
```

## Data Model

### Marts (primary outputs)

| Table | Grain | Description |
|-------|-------|-------------|
| `fct_diabetes_prevalence` | state × year × age × race | Survey-weighted diabetes prevalence |
| `fct_diabetes_risk_factors` | diabetes_category × year | Risk factor burden by diabetes status |
| `fct_survey_weighted_estimates` | state × year | Wide KPI table for all outcomes |
| `dim_respondent_demographics` | respondent | Demographic dimension |

### Validation checks

| Check | Expected Range |
|-------|---------------|
| National weighted diabetes prevalence | 8%–15% per year (CDC: ~11–12%) |
| Sum of `_LLCPWT` per year | 200M–280M (US adult population) |
| `_LLCPWT` null rate | < 1% |
| Respondent count per year | 400K–600K |

## Verification

After running the full pipeline, run these queries to confirm everything worked:

```sql
-- Connect to DuckDB
-- duckdb data/duckdb/brfss.duckdb

-- 1. Row counts per year
SELECT survey_year, COUNT(*) as n
FROM raw.brfss_all
GROUP BY 1 ORDER BY 1;
-- Expected: ~400K-500K per year

-- 2. Weight sum (should approximate US adult population ~250M)
SELECT survey_year, ROUND(SUM(llcpwt)/1e6, 1) as estimated_adults_millions
FROM staging.stg_brfss__respondents
GROUP BY 1 ORDER BY 1;

-- 3. National diabetes prevalence (should be ~11-12%)
SELECT
    survey_year,
    ROUND(SUM(weighted_n_diabetic) / SUM(weighted_n_total) * 100, 1) as diabetes_pct
FROM marts.fct_diabetes_prevalence
WHERE weighted_diabetes_prevalence IS NOT NULL
GROUP BY 1 ORDER BY 1;
```

## BRFSS Survey Methodology Note

This project produces **point estimates only**. For confidence intervals and
formal hypothesis testing, use statistical software that implements Taylor Series
Linearization with `_STSTR` (stratum) and `_PSU` (primary sampling unit):

- **R**: `survey::svyglm()`, `survey::svyby()`
- **Stata**: `svy: proportion`, `svy: logistic`
- **SAS**: `PROC SURVEYMEANS`, `PROC SURVEYLOGISTIC`

See `docs/BRFSS_variables_reference.md` for a detailed explanation of BRFSS
survey design and weighting.

## License

Data: CDC BRFSS public use files (public domain).  
Code: MIT License.
