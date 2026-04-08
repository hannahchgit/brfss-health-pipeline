"""
Great Expectations checkpoint for BRFSS raw data validation.

Run before dbt transformations to validate raw XPT data loaded into DuckDB.
Validates: column presence, row counts, key variable ranges, and weight completeness.

Usage:
    python -m expectations.validate_raw
    python -m expectations.validate_raw --year 2022
"""

import argparse
import logging
import sys

import duckdb
import great_expectations as gx
from great_expectations.core.batch import RuntimeBatchRequest

from ingestion.config import COLUMNS_TO_KEEP, COLUMN_NAME_MAP, DUCKDB_PATH, MIN_ROWS_PER_YEAR, YEARS

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# Expected clean column names (after ingestion renaming)
EXPECTED_COLUMNS = list(COLUMN_NAME_MAP.values()) + ["survey_year", "loaded_at"]


def get_raw_dataframe(year: int) -> "pd.DataFrame":
    """Pull one year's raw table from DuckDB into a pandas DataFrame for GE validation."""
    import pandas as pd
    con = duckdb.connect(str(DUCKDB_PATH), read_only=True)
    df = con.execute(f"SELECT * FROM raw.brfss_{year}").df()
    con.close()
    return df


def build_expectation_suite(context: gx.DataContext, suite_name: str) -> gx.core.ExpectationSuite:
    """Build or retrieve the BRFSS raw data expectation suite."""
    try:
        suite = context.get_expectation_suite(suite_name)
        log.info("Loaded existing expectation suite: %s", suite_name)
    except Exception:
        suite = context.add_expectation_suite(suite_name)
        log.info("Created new expectation suite: %s", suite_name)
    return suite


def validate_year(year: int, context: gx.DataContext) -> bool:
    """Run GE validations against one year of raw BRFSS data.

    Returns:
        True if all expectations passed, False otherwise.
    """
    log.info("Validating raw BRFSS data for year %d ...", year)
    df = get_raw_dataframe(year)

    # Use GE's in-memory Pandas datasource
    datasource = context.sources.add_or_update_pandas(name=f"brfss_raw_{year}")
    asset = datasource.add_dataframe_asset(name=f"brfss_{year}_asset")
    batch_request = asset.build_batch_request(dataframe=df)

    suite_name = "brfss_raw_suite"
    suite = build_expectation_suite(context, suite_name)

    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite=suite,
    )

    # ── Expectation 1: Row count ──────────────────────────────────────────────
    validator.expect_table_row_count_to_be_between(
        min_value=MIN_ROWS_PER_YEAR,
        max_value=600_000,
    )

    # ── Expectation 2: Expected columns present ───────────────────────────────
    clean_cols = list(COLUMN_NAME_MAP.values())
    for col in clean_cols:
        validator.expect_column_to_exist(col)

    # ── Expectation 3: Survey weight completeness (< 1% null) ─────────────────
    validator.expect_column_values_to_not_be_null(
        column="llcpwt",
        mostly=0.99,
    )

    # ── Expectation 4: Weight must be positive ────────────────────────────────
    validator.expect_column_values_to_be_between(
        column="llcpwt",
        min_value=0,
        max_value=100_000_000,
        mostly=0.99,
    )

    # ── Expectation 5: State FIPS in valid range ──────────────────────────────
    validator.expect_column_values_to_be_between(
        column="state",
        min_value=1,
        max_value=78,
        mostly=0.99,
    )

    # ── Expectation 6: Survey year is consistent ──────────────────────────────
    validator.expect_column_values_to_be_in_set(
        column="survey_year",
        value_set=[year],
    )

    # ── Expectation 7: Diabetes variable has expected code range ──────────────
    validator.expect_column_values_to_be_in_set(
        column="diabete4",
        value_set=[1, 2, 3, 4, 7, 9],
        mostly=0.95,
    )

    results = validator.validate()
    passed = results.success

    if passed:
        log.info("Year %d: ALL expectations passed.", year)
    else:
        failed = [r for r in results.results if not r.success]
        log.error("Year %d: %d expectation(s) FAILED:", year, len(failed))
        for r in failed:
            log.error("  - %s", r.expectation_config.expectation_type)

    return passed


def main():
    parser = argparse.ArgumentParser(description="Validate BRFSS raw data with Great Expectations")
    parser.add_argument("--years", nargs="+", type=int, default=YEARS)
    args = parser.parse_args()

    context = gx.get_context(mode="ephemeral")

    all_passed = True
    for year in args.years:
        passed = validate_year(year, context)
        if not passed:
            all_passed = False

    if not all_passed:
        log.error("Validation FAILED for one or more years. Halting pipeline.")
        sys.exit(1)

    log.info("All raw data validations passed.")


if __name__ == "__main__":
    main()
