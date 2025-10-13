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
    cron_schedule = "0 */2 * * * *"  # every 2 min for local testing
    monitor_flag = False
else:
    cron_schedule = "0 0 0 2 * *"    # midnight on 2nd
    monitor_flag = True

logging.warning(f"Billing Function starting in {MODE.upper()} mode with schedule: {cron_schedule}")

@app.function_name(name="MonthlyBillingTimer")
@app.schedule(schedule=cron_schedule, timezone="UTC", arg_name="myTimer", use_monitor=monitor_flag)
def monthly_billing_timer(myTimer: func.TimerRequest) -> None:
    logging.warning("🚀 monthly_billing_timer fired - entering function body")
    run_billing()
    logging.warning("✅ monthly_billing_timer completed")


# --------------------------
# Manual HTTP trigger for testing
# --------------------------
@app.function_name(name="TriggerBillingHttp")
@app.route(route="runbilling", methods=["GET", "POST"], auth_level=func.AuthLevel.FUNCTION)
def trigger_billing_http(req: func.HttpRequest) -> func.HttpResponse:
    logging.warning("🌐 Manual trigger received via HTTP")
    run_billing()
    return func.HttpResponse("✅ Billing run executed successfully!", status_code=200)