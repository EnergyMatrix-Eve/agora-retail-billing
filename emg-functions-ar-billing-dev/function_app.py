import azure.functions as func
import logging
import os
from datetime import datetime
import bill_generator

def run_billing(env_name: str):
    logging.warning(f"🚀 Running bill_generator.main() for {env_name}")
    bill_generator.main()
    logging.warning(f"✅ bill_generator.main() completed for {env_name}")


app = func.FunctionApp()

# --------------------------
# Environment-based schedule
# --------------------------
MODE = os.getenv("BILLING_MODE", "production").lower()

if MODE == "local":
    # Local testing schedule: every 1 minute for local testing
    cron_schedule = "0 */1 * * * *"
    monitor_flag = False
elif MODE == "testing":
    # Cloud testing schedule: every day at 6.45 AM UTC for cloud testing (2.45pm Perth time)
    cron_schedule = "0 45 6 * * *"
    monitor_flag = True
elif MODE == "production":
    # Production schedule: # 7 PM UTC on the 1st, which is 5 AM AEST on the 2nd
    cron_schedule = "0 0 19 1 * *"
    monitor_flag = True
else:
    cron_schedule = "0 */1 * * * *"  # default fallback (every 1 min for testing)
    monitor_flag = False

logging.warning(f"Billing Function starting in {MODE.upper()} mode with schedule: {cron_schedule}")

@app.function_name(name="MonthlyBillingTimer")
@app.schedule(schedule=cron_schedule, timezone="UTC", arg_name="myTimer", use_monitor=monitor_flag)
def monthly_billing_timer(myTimer: func.TimerRequest) -> None:
    logging.warning("🚀 monthly_billing_timer fired - entering function body")
    run_billing(MODE)
    logging.warning("✅ monthly_billing_timer completed")


# --------------------------
# Manual HTTP trigger for testing
# --------------------------
@app.function_name(name="TriggerBillingHttp")
@app.route(route="runbilling", methods=["GET", "POST"], auth_level=func.AuthLevel.FUNCTION)
def trigger_billing_http(req: func.HttpRequest) -> func.HttpResponse:
    logging.warning("🌐 Manual trigger received via HTTP")
    run_billing(MODE)
    return func.HttpResponse("✅ Billing run executed successfully!", status_code=200)
