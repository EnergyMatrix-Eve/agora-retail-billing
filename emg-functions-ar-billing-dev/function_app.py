import azure.functions as func
import datetime
import os
import logging

logging.warning("✅ function_app.py imported successfully")

try:
    import bill_generator
    logging.warning(f"✅ bill_generator imported from: {bill_generator.__file__}")
except Exception as e:
    logging.exception(f"❌ Failed to import bill_generator: {e}")
    bill_generator = None


def run_billing():
    logging.warning("📄 Import succeeded. Running bill_generator.main() ...")
    bill_generator.main()
    logging.warning("📄 bill_generator.main() finished.")


app = func.FunctionApp()

# --------------------------
# Environment-based schedule
# --------------------------
MODE = os.getenv("BILLING_MODE", "production").lower()
if MODE == "local":
    cron_schedule = "0 */1 * * * *"  # every 1 min for local testing
    monitor_flag = False
else:
    cron_schedule = "0 0 0 2 * *"    # run at midnight on 2nd
    monitor_flag = True

logging.warning(f"Billing Function starting in {MODE.upper()} mode with schedule: {cron_schedule}")

@app.function_name(name="MonthlyBillingTimer")
@app.schedule(schedule=cron_schedule, timezone="UTC", arg_name="myTimer", use_monitor=monitor_flag)
def monthly_billing_timer(myTimer: func.TimerRequest) -> None:
    logging.warning("🚀 monthly_billing_timer fired - entering function body")
    run_billing()
    logging.warning("✅ monthly_billing_timer completed")
