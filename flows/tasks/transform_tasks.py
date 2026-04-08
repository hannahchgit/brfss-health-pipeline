"""Prefect tasks for dbt transformations."""

import subprocess
from pathlib import Path

from prefect import get_run_logger, task

DBT_PROJECT_DIR = Path(__file__).resolve().parent.parent.parent / "dbt_brfss"


def _run_dbt(args: list[str]) -> dict:
    """Run a dbt CLI command and return status + output."""
    cmd = ["dbt"] + args
    result = subprocess.run(
        cmd,
        cwd=str(DBT_PROJECT_DIR),
        capture_output=True,
        text=True,
    )
    return {
        "returncode": result.returncode,
        "stdout": result.stdout,
        "stderr": result.stderr,
        "success": result.returncode == 0,
    }


@task(
    name="dbt-deps",
    description="Install dbt package dependencies.",
)
def dbt_deps() -> dict:
    logger = get_run_logger()
    logger.info("Installing dbt packages ...")
    result = _run_dbt(["deps"])
    if not result["success"]:
        raise RuntimeError(f"dbt deps failed:\n{result['stderr']}")
    logger.info("dbt deps complete.")
    return result


@task(
    name="dbt-seed",
    description="Load dbt seed CSV files (state codes, variable labels).",
)
def dbt_seed() -> dict:
    logger = get_run_logger()
    logger.info("Loading dbt seeds ...")
    result = _run_dbt(["seed"])
    if not result["success"]:
        raise RuntimeError(f"dbt seed failed:\n{result['stderr']}")
    logger.info("dbt seed complete.")
    return result


@task(
    name="dbt-run-staging",
    description="Run dbt staging models.",
    timeout_seconds=300,
)
def dbt_run_staging() -> dict:
    logger = get_run_logger()
    logger.info("Running dbt staging models ...")
    result = _run_dbt(["run", "--select", "staging"])
    if not result["success"]:
        raise RuntimeError(f"dbt run staging failed:\n{result['stderr']}")
    logger.info("Staging models complete.")
    return result


@task(
    name="dbt-run-intermediate",
    description="Run dbt intermediate models.",
    timeout_seconds=300,
)
def dbt_run_intermediate() -> dict:
    logger = get_run_logger()
    logger.info("Running dbt intermediate models ...")
    result = _run_dbt(["run", "--select", "intermediate"])
    if not result["success"]:
        raise RuntimeError(f"dbt run intermediate failed:\n{result['stderr']}")
    logger.info("Intermediate models complete.")
    return result


@task(
    name="dbt-run-marts",
    description="Run dbt mart models.",
    timeout_seconds=600,
)
def dbt_run_marts() -> dict:
    logger = get_run_logger()
    logger.info("Running dbt mart models ...")
    result = _run_dbt(["run", "--select", "marts"])
    if not result["success"]:
        raise RuntimeError(f"dbt run marts failed:\n{result['stderr']}")
    logger.info("Mart models complete.")
    return result


@task(
    name="dbt-test",
    description="Run all dbt tests. Logs failures but does not stop the flow.",
)
def dbt_test() -> dict:
    logger = get_run_logger()
    logger.info("Running dbt tests ...")
    result = _run_dbt(["test", "--store-failures"])
    if not result["success"]:
        logger.warning("dbt test: some tests failed. Review output:\n%s", result["stdout"][-3000:])
    else:
        logger.info("All dbt tests passed.")
    return result
