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
#  LOCAL - every 10 minutes
# --------------------------
@app.function_name(name="LocalBillingTimer")
@app.schedule(
    schedule="0 */10 * * * *",      # every 10 minutes
    timezone="UTC",
    arg_name="myTimer",
    use_monitor=False
)
def local_billing_timer(myTimer: func.TimerRequest):
    if os.getenv("BILLING_MODE", "local").lower() == "local":
        logging.warning("🧩 LocalBillingTimer fired")
        run_billing("LOCAL")
    else:
        logging.info("⏭️ LocalBillingTimer skipped (not local mode)")

# --------------------------
#  CLOUD TEST - every day
# --------------------------
@app.function_name(name="CloudTestBillingTimer")
@app.schedule(
    schedule="0 30 5 * * *",         # daily at 5.30am UTC
    timezone="UTC",
    arg_name="myTimer",
    use_monitor=True
)
def cloud_test_billing_timer(myTimer: func.TimerRequest):
    if os.getenv("BILLING_MODE", "test").lower() == "test":
        logging.warning("🧪 CloudTestBillingTimer fired")
        run_billing("CLOUD TEST")
    else:
        logging.info("⏭️ CloudTestBillingTimer skipped (not test mode)")

# --------------------------
#  CLOUD PROD - 2nd each month
# --------------------------
@app.function_name(name="CloudProdBillingTimer")
@app.schedule(
    schedule="0 0 0 14 * *",         # 14th day of each month at midnight UTC
    timezone="UTC",
    arg_name="myTimer",
    use_monitor=True
)
def cloud_prod_billing_timer(myTimer: func.TimerRequest):
    if os.getenv("BILLING_MODE", "production").lower() == "production":
        logging.warning("🏭 CloudProdBillingTimer fired")
        run_billing("CLOUD PROD")
    else:
        logging.info("⏭️ CloudProdBillingTimer skipped (not production mode)")
