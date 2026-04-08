# scheduler.py
import schedule
import time
import logging
from datetime import datetime
from hourly_data_generator import main

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scheduler.log'),
        logging.StreamHandler()
    ]
)

def job():
    """Wrapper function for scheduled job"""
    logging.info("="*60)
    logging.info("SCHEDULED JOB TRIGGERED")
    logging.info("="*60)
    
    try:
        main()  # Run the data generation
        logging.info("✅ Job completed successfully")
    except Exception as e:
        logging.error(f"❌ Job failed with error: {str(e)}")

# Schedule the job to run every hour
schedule.every(1).hours.do(job)

print("🚀 SCHEDULER STARTED")
print(f"⏰ Current time: {datetime.now()}")
print(f"📅 Next run scheduled for: {schedule.next_run()}")
print("\nPress Ctrl+C to stop the scheduler\n")

# Run immediately on startup (optional)
print("Running initial job...")
job()

# Keep the scheduler running
while True:
    schedule.run_pending()
    time.sleep(60)  # Check every minute