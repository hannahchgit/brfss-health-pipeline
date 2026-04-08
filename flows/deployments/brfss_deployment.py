"""
Prefect deployment for scheduled annual BRFSS pipeline runs.

Deploy with:
    python flows/deployments/brfss_deployment.py

This creates a deployment named "brfss-annual-refresh" that triggers on January 1st
each year — aligned with CDC's annual BRFSS data release schedule.

Run manually via Prefect UI or CLI:
    prefect deployment run 'brfss-health-pipeline/brfss-annual-refresh'
"""

from prefect.client.schemas.schedules import CronSchedule
from prefect.deployments import Deployment

from flows.brfss_pipeline import brfss_pipeline

deployment = Deployment.build_from_flow(
    flow=brfss_pipeline,
    name="brfss-annual-refresh",
    # January 1st at 06:00 UTC — CDC typically releases prior year data in Q4/Q1
    schedules=[CronSchedule(cron="0 6 1 1 *", timezone="UTC")],
    parameters={
        "years": [2021, 2022, 2023],
        "skip_download": False,
    },
    description=(
        "Annual BRFSS pipeline refresh. Runs January 1st to pick up the "
        "prior year's CDC data release. Update the `years` parameter each year."
    ),
    tags=["brfss", "health", "annual"],
)

if __name__ == "__main__":
    deployment.apply()
    print("Deployment 'brfss-annual-refresh' applied successfully.")
    print("Run with: prefect deployment run 'brfss-health-pipeline/brfss-annual-refresh'")
