"""
Local Scheduler for Hourly Cart Data Generation
-----------------------------------------------
Runs hourly_data_generator.main() every hour.

For local testing:
    python scripts/scheduler.py

Important:
This local scheduler only generates/uploads Blob files. Your ADF trigger is a
separate schedule that copies Blob data into SQL. In production, use a Blob
Event Trigger or orchestrate the generation step and ADF pipeline together.
"""

import logging
import time
from datetime import datetime

import schedule

from hourly_data_generator import main

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("scheduler.log"),
        logging.StreamHandler(),
    ],
)


def job() -> None:
    """Run one scheduled generation job."""
    logging.info("=" * 70)
    logging.info("SCHEDULED CART DATA GENERATION STARTED")
    logging.info("=" * 70)

    try:
        main()
        logging.info("Scheduled job completed successfully.")
    except Exception as error:
        logging.exception("Scheduled job failed: %s", error)


schedule.every(1).hours.do(job)

print("Scheduler started")
print(f"Current time: {datetime.now().replace(microsecond=0).isoformat()}")
print(f"Next run scheduled for: {schedule.next_run()}")
print("Press Ctrl+C to stop the scheduler.\n")

# Run once immediately when the scheduler starts.
job()

while True:
    schedule.run_pending()
    time.sleep(60)
