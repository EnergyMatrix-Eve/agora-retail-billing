import azure.functions as func
import logging
import os

# Import your main billing logic
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
    cron_schedule = "0 */1 * * * *"  # every 1 min for local testing
    monitor_flag = False
else:
    cron_schedule = "0 0 19 1 * *"    # 7 PM UTC on the 1st, which is 5 AM AEST
    monitor_flag = True

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
