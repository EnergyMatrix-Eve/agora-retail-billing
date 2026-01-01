import azure.functions as func
import logging
import os
from datetime import datetime
import bill_generator_test_statement#

def run_billing(env_name: str):
    logging.warning(f"🚀 Running bill_generator_test_statement.main() for {env_name}")
    bill_generator_test_statement.main()#
    logging.warning(f"✅ bill_generator_test_statement.main() completed for {env_name}")


app = func.FunctionApp()

# --------------------------
# Environment-based schedule
# --------------------------
MODE = os.getenv("BILLING_MODE", "production").lower()

if MODE == "testing":
    # Cloud testing schedule: 5 AM AEST on the 2nd, 8 AM PERTH time
    cron_schedule = "0 0 5 2 12 *" # sec min hour day month dow 
    monitor_flag = True
elif MODE == "production":
    # Production schedule: # 5 AM AEST on the 2nd, 8 AM PERTH time
    cron_schedule = "0 0 5 2 * *"
    monitor_flag = True
else:
    cron_schedule = "0 0 5 2 12 *" # default to prod
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
