"""Prefect tasks for BRFSS data ingestion."""

from datetime import timedelta
from pathlib import Path

import duckdb
from prefect import get_run_logger, task
from prefect.tasks import task_input_hash

from ingestion.config import DUCKDB_PATH, RAW_DATA_DIR, YEARS
from ingestion.download_brfss import download_year, xpt_path
from ingestion.load_to_duckdb import create_unified_view, initialize_database, load_year


@task(
    name="check-raw-data-exists",
    description="Check if BRFSS XPT file already exists for a given year.",
)
def check_raw_data_exists(year: int, data_dir: Path = RAW_DATA_DIR) -> bool:
    """Returns True if the XPT file for this year already exists."""
    return xpt_path(year, data_dir).exists()


@task(
    name="download-brfss-xpt",
    description="Download BRFSS XPT file from CDC for a given survey year.",
    retries=3,
    retry_delay_seconds=30,
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(days=7),
)
def download_brfss_year(year: int, data_dir: Path = RAW_DATA_DIR, force: bool = False) -> Path:
    """Download and extract BRFSS XPT for a given year. Cached for 7 days."""
    logger = get_run_logger()
    logger.info("Downloading BRFSS XPT for year %d ...", year)
    path = download_year(year, data_dir, force=force)
    logger.info("Downloaded: %s", path)
    return path


@task(
    name="load-year-to-duckdb",
    description="Load one year of BRFSS XPT data into DuckDB raw schema.",
)
def load_year_to_duckdb(
    year: int, db_path: Path = DUCKDB_PATH, data_dir: Path = RAW_DATA_DIR
) -> int:
    """Load raw BRFSS data for a single year into DuckDB. Returns row count."""
    logger = get_run_logger()
    con = initialize_database(db_path)
    try:
        row_count = load_year(con, year, data_dir)
        logger.info("Loaded %d rows for year %d", row_count, year)
        return row_count
    finally:
        con.close()


@task(
    name="create-unified-view",
    description="Create raw.brfss_all UNION ALL view across all loaded years.",
)
def create_brfss_unified_view(years: list[int] = YEARS, db_path: Path = DUCKDB_PATH) -> None:
    """Create the raw.brfss_all view spanning all loaded years."""
    logger = get_run_logger()
    con = duckdb.connect(str(db_path))
    try:
        create_unified_view(con, years)
        total = con.execute("SELECT COUNT(*) FROM raw.brfss_all").fetchone()[0]
        logger.info("Unified view created: %d total rows across years %s", total, years)
    finally:
        con.close()
