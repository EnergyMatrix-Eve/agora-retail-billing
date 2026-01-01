import os
import logging
from io import BytesIO, StringIO
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from fpdf import FPDF
from office365.runtime.auth.client_credential import ClientCredential
from office365.sharepoint.client_context import ClientContext
from office365.runtime.client_request_exception import ClientRequestException
import re
import csv
from decimal import Decimal, ROUND_HALF_UP
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple, Iterable
import matplotlib as mpl
from matplotlib.font_manager import FontProperties
from fpdf.enums import XPos, YPos
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
import msal
from decimal import Decimal, InvalidOperation
import time
import random
import hashlib
from dataclasses import dataclass, field
import math
#from dotenv import load_dotenv

# =========================
# Config
# =========================
#load_dotenv()
FOLDER    = os.getenv("FOLDER")
FOLDER_LOGO = os.getenv("FOLDER_LOGO")

# SQL connection
server = os.getenv("DB_SERVER")
database = os.getenv("DB_DATABASE")
username = os.getenv("DB_USERNAME")
password = os.getenv("DB_PASSWORD")
# History table
HISTORY_TABLE = "dbo.billing_history"
# Skip invoices already present in history (by invoice_number)
SKIP_EXISTING_INVOICES = False

GENERATE_DATE = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")

# If monthly view totals are EX-GST
GST_RATE = 0.00

# =========================
# SharePoint connection
# =========================
def get_sharepoint_context() -> ClientContext:
    """
    Authenticate to SharePoint using Azure AD App (Client Credentials Flow).
    Make sure the app registration has permissions for SharePoint Online (Sites.Selected or Sites.FullControl.All).
    """
    client_id = os.getenv("AZURE_CLIENT_ID")
    client_secret = os.getenv("AZURE_CLIENT_SECRET")
    site_url = os.getenv("SITE_URL")

    creds = ClientCredential(client_id, client_secret)
    ctx = ClientContext(site_url).with_credentials(creds)
    return ctx

def upload_bytes_to_sharepoint(file_bytes: bytes, filename: str) -> None:
    try:
        ctx = get_sharepoint_context()
        folder = ctx.web.get_folder_by_server_relative_url(FOLDER)
        folder.upload_file(filename, file_bytes).execute_query()
        logging.info(f"✅ Uploaded {filename} to SharePoint")
    except ClientRequestException as e:
        logging.error(f"SharePoint error uploading {filename}: {e}")
        raise
    except Exception as e:
        logging.error(f"Unexpected error uploading {filename}: {e}")
        raise

def _status_code_from_sp_exception(e: Exception) -> int | None:
    resp = getattr(e, "response", None)
    if resp is not None:
        return getattr(resp, "status_code", None) or getattr(resp, "status", None)
    # office365 sometimes doesn't attach response; fall back to string match
    msg = str(e)
    if "404" in msg and ("Not Found" in msg or "does not exist" in msg):
        return 404
    return None

TRANSIENT_STATUS = {429, 500, 502, 503, 504}

class SharePointLogHandler(logging.Handler):
    def __init__(self, sp, remote_filename: str, flush_every: int = 50, flush_seconds: int = 10):
        super().__init__()
        self.sp = sp
        self.remote_filename = remote_filename
        self.buffer = StringIO()
        self.flush_every = flush_every
        self.flush_seconds = flush_seconds
        self._line_count = 0
        self._last_flush = time.time()
        self.remote_filename = os.path.basename(remote_filename)

        # ✅ ensure file exists in SharePoint folder (create empty if missing)
        try:
            existing = self.sp.download_bytes(self.remote_filename)
            if existing:
                try:
                    self.buffer.write(existing.decode("utf-8"))
                except Exception:
                    pass
            else:
                self.sp.upload_bytes(self.remote_filename, b"")  # creates it
        except Exception as e:
            logging.warning(f"Log init failed ({self.remote_filename}): {e}")

        try:
            existing = self.sp.download_bytes(remote_filename)
        except ClientRequestException as e:
            # if anything slips through, do not fail the whole function
            logging.warning(f"Log file not readable yet ({remote_filename}): {e}")
            existing = None

        if existing:
            try:
                self.buffer.write(existing.decode("utf-8"))
            except Exception:
                pass

    def emit(self, record):
        msg = self.format(record)
        self.buffer.write(msg + "\n")
        self._line_count += 1

        now = time.time()
        if self._line_count >= self.flush_every or (now - self._last_flush) >= self.flush_seconds:
            self.flush()

    def flush(self):
        data = self.buffer.getvalue().encode("utf-8")
        self.sp.upload_bytes(self.remote_filename, data)
        self._line_count = 0
        self._last_flush = time.time()

    def close(self):
        try:
            # final flush
            self.flush()
        finally:
            super().close()
class SharePointClient:
    def __init__(self, site_url: str, client_id: str, client_secret: str, folder_url: str, logo_folder_url: str):
        self.site_url = site_url
        self.folder_url = folder_url
        self.logo_folder_url = logo_folder_url
        creds = ClientCredential(client_id, client_secret)
        self.ctx = ClientContext(site_url).with_credentials(creds)

    def _retry(self, fn, *, tries=6, base_sleep=1.0):
        for i in range(tries):
            try:
                return fn()
            except ClientRequestException as e:
                status = getattr(e, "response", None).status_code if getattr(e, "response", None) else None
                if status not in TRANSIENT_STATUS or i == tries - 1:
                    raise
            except Exception:
                if i == tries - 1:
                    raise
            # backoff + jitter
            sleep = base_sleep * (2 ** i) + random.uniform(0, 0.25)
            time.sleep(sleep)

    def folder(self):
        return self.ctx.web.get_folder_by_server_relative_url(self.folder_url)

    def upload_bytes(self, filename: str, data: bytes):
        def op():
            self.folder().upload_file(filename, data).execute_query()
        return self._retry(op)

    def download_bytes(self, filename: str) -> bytes | None:
        def op():
            path = f"{self.folder_url.rstrip('/')}/{filename.lstrip('/')}"
            f = self.ctx.web.get_file_by_server_relative_url(path)
            buf = BytesIO()
            f.download(buf).execute_query()
            buf.seek(0)
            return buf.read()

        try:
            return self._retry(op)
        except ClientRequestException as e:
            status = _status_code_from_sp_exception(e)
            if status == 404:
                return None
            raise

    def list_file_names(self) -> list[str]:
        def op():
            folder = self.folder()
            files = folder.files
            self.ctx.load(files)
            self.ctx.execute_query()
            return [f.name for f in files]
        return self._retry(op)

    def download_logo_bytes(self, logo_filename: str) -> bytes | None:
        def op():
            f = self.ctx.web.get_file_by_server_relative_url(f"{self.logo_folder_url.rstrip('/')}/{logo_filename}")
            buf = BytesIO()
            f.download(buf).execute_query()
            buf.seek(0)
            return buf.read()
        try:
            return self._retry(op)
        except Exception:
            return None



# =========================
# Style / Colors
# =========================
custom_colors = {
    "Firm Gas Sales": "#0089D0",
    "Overrun Charges": "#9DC0D7",
    "Transport Fee": "#425B7E",
    "Distribution Charges": "#F4A261",
    "Adjustment Charges": "#E76F51",
    "Other Charges": "#2A9D8F"
}
mpl.rcParams["font.family"] = "Arial"

# =========================
# SQL helpers
# =========================
def get_engine():
    return create_engine(
        f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+18+for+SQL+Server",  
        fast_executemany=True
    )

def load_views() -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Returns:
        monthly_df: dbo.vw_test_charges_monthly (1 row per invoice)
        breakdown_df: dbo.vw_test_charges_breakdown (charge lines)
        daily_df: dbo.vw_billing_charges_daily (daily consumption)
        basic_df: dbo.vvw_billing_consumption_basic (basic consumption)
    """
    eng = get_engine()
    with eng.begin() as conn:
        logging.info("Connected via SQLAlchemy engine.")
        monthly_df = pd.read_sql("SELECT * FROM dbo.vw_test_charges_monthly;", conn)
        breakdown_df = pd.read_sql("SELECT * FROM dbo.vw_test_charges_breakdown;", conn)
        try:
            daily_df = pd.read_sql("SELECT * FROM dbo.vw_test_charges_daily;", conn)
        except Exception:
            logging.warning("🚫 vw_test_charges_daily not found; consumption chart will be skipped.")
            daily_df = pd.DataFrame()
        try:
            basic_df = pd.read_sql("SELECT * FROM dbo.vw_billing_consumption_basic;", conn)
        except Exception:
            logging.warning("🚫 vw_billing_consumption_basic not found; consumption chart will be skipped.")
            basic_df = pd.DataFrame()

    logging.info(f"monthly rows={len(monthly_df)}, breakdown rows={len(breakdown_df)}, daily rows={len(daily_df)}, basic rows={len(basic_df)}")
    return monthly_df, breakdown_df, daily_df, basic_df


# =========================
# Header & detail shaping
# =========================
def build_invoice_headers_from_monthly(monthly: pd.DataFrame) -> pd.DataFrame:
    """
    Forward the monthly view as the canonical header/totals dataset.
    """
    if monthly.empty:
        return pd.DataFrame()

    df = monthly.copy()

    money_cols = [
        "firm_gas_amount","spot_gas_amount",
        "transport_firm_amount","transport_overrun_amount",
        "atco_usage_amount","atco_demand_amount","atco_standing_amount",
        "gas_adjustment_charges","distribution_adjustment_charges","regulatory_adjustment_charges",
        "admin_fee","late_payment_fee","total_amount","gst_amount","total_in_gst_amount"
    ]
    for c in money_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0).round(2)

    df = df.dropna(subset=["invoice_number"])
    return df.reset_index(drop=True)

def build_charges_df_from_breakdown(invoice_lines: pd.DataFrame) -> pd.DataFrame:
    """
    Convert raw breakdown rows to the table schema expected by the PDF section.
    Safely maps columns and includes invoice_number for downstream lookup.
    """
    if invoice_lines.empty:
        return pd.DataFrame(columns=[
            "invoice_number",
            "Charge Category","Charge Type","Rate","Rate UOM",
            "Unit","Unit UOM","Amount ex GST","Statement Amount ex GST"
        ])

    df = invoice_lines.copy()

    # Safely extract fields if present
    def col(c, default=""):
        return df[c] if c in df.columns else pd.Series([default] * len(df))

    out = pd.DataFrame({
        "invoice_number": col("invoice_number", default=None),

        "Charge Category": col("charge_category", default="Other"),
        "Charge Type": col("charge_type"),
        "Rate": col("rate"),
        "Rate UOM": col("rate_uom"),
        "Unit": col("unit"),
        "Unit UOM": col("unit_uom"),

        # Amounts
        "Amount ex GST": col("category_amount"),
        "Statement Amount ex GST": col("category_statement_total_amount"),
    })

    # Ordering for UI
    category_order = [
        "Firm Gas Sales", "Overrun Charges", "Transport Fee",
        "Distribution Charges", "Adjustment Charges", "Other Charges"
    ]
    out["Charge Category"] = pd.Categorical(out["Charge Category"], category_order, ordered=True)

    # Sort nicely for PDF
    out = out.sort_values(["Charge Category", "Charge Type"]).reset_index(drop=True)
    return out

# ========================================= Billing History =========================================
def ensure_billing_history_table(engine) -> None:
    q = """
    SELECT 1
    FROM sys.objects
    WHERE object_id = OBJECT_ID(N'dbo.billing_history')
      AND type = N'U'
    """
    with engine.begin() as con:
        row = con.exec_driver_sql(q).fetchone()
        if row is None:
            raise RuntimeError("Table dbo.billing_history does not exist. Please create it first.")

def build_history_row_from_monthly(m: pd.Series) -> Dict[str, Any]:
    """Build a clean and stable history row from the monthly dataframe row."""

    def get(name, default=None):
        """Return the column value if it exists and is not null."""
        if name in m and pd.notna(m[name]):
            return m[name]
        return default
    site_name = get("site_name")
    bill_start_date = pd.to_datetime(get("bill_start_date"), errors="coerce")
    bill_end_date   = pd.to_datetime(get("bill_end_date"), errors="coerce")
    bill_issue_date = pd.to_datetime(get("bill_issue_date"), errors="coerce")
    read_start_date = pd.to_datetime(get("read_start_date"), errors="coerce")
    read_end_date   = pd.to_datetime(get("read_end_date"), errors="coerce")
    total_amount = get("total_amount", default=0)
    gst_amount   = get("gst_amount", default=0)
    total_in_gst_amount = get("total_in_gst_amount")
    if total_in_gst_amount is None:
        total_in_gst_amount = float(total_amount) + float(gst_amount)
    statement_total_amount = get("statement_total_amount", default=0)
    statement_gst_amount   = get("statement_gst_amount", default=0)
    statement_total_in_gst_amount = get("statement_total_in_gst_amount")
    if statement_total_in_gst_amount is None:
        statement_total_in_gst_amount = float(statement_total_amount) + float(statement_gst_amount)
    
    invoice_agg_code = get("invoice_agg_code")
    item_listed = get("item_listed_bills")
    billing_days = get("billing_days")

    def f(v): return float(v) if v not in (None, "") else 0.0

    # ---- BUILD FINAL HISTORY RECORD ----
    h = {
        # Base identifiers
        "invoice_agg_code": invoice_agg_code,
        "item_listed_bills": item_listed,
        "billing_days": billing_days,
        "statement_number": get("statement_number"),
        "invoice_number": get("invoice_number"),
        "purchase_order_number": get("purchase_order_number"),

        # Company & account
        "company_name": get("company_name"),
        "company_code": get("company_code"),
        "account_number": get("account_number"),
        "mirn": get("mirn"),
        "distributor": get("distributor"),

        # Billing period
        "bill_start_date": bill_start_date,
        "bill_end_date": bill_end_date,
        "bill_issue_date": bill_issue_date,

        # New read dates
        "read_start_date": read_start_date,
        "read_end_date": read_end_date,

        # Core charges
        "gj_consumption": f(get("gj_consumption")),
        "firm_gas_amount": f(get("firm_gas_amount")),
        "spot_gas_amount": f(get("spot_gas_amount")),
        "atco_usage_amount": f(get("atco_usage_amount")),
        "atco_demand_amount": f(get("atco_demand_amount")),
        "atco_standing_amount": f(get("atco_standing_amount")),
        "transport_firm_amount": f(get("transport_firm_amount")),
        "transport_overrun_amount": f(get("transport_overrun_amount")),
        "gas_adjustment_charges": f(get("gas_adjustment_charges")),
        "distribution_adjustment_charges": f(get("distribution_adjustment_charges")),
        "regulatory_adjustment_charges": f(get("regulatory_adjustment_charges")),
        "admin_fee": f(get("admin_fee")),
        "late_payment_fee": f(get("late_payment_fee")),

        # New monthly-level account balances
        "opening_balance": f(get("opening_balance")),
        "payment_received": f(get("payment_received")),
        "balance_carried_forward": f(get("balance_carried_forward")),

        # New statement-level balances
        "statement_opening_balance": f(get("statement_opening_balance")),
        "statement_payment_received": f(get("statement_payment_received")),
        "statement_balance_carried_forward": f(get("statement_balance_carried_forward")),

        # Totals
        "total_amount": f(total_amount),
        "gst_amount": f(gst_amount),
        "total_in_gst_amount": f(total_in_gst_amount),
        "statement_total_amount": f(statement_total_amount),
        "statement_gst_amount": f(statement_gst_amount),
        "statement_total_in_gst_amount": f(statement_total_in_gst_amount),

        # audit field
        "generated_at_utc": datetime.utcnow(),
    }

    return h

def sanitize_history_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    cleaned: List[Dict[str, Any]] = []

    for r in rows:
        out: Dict[str, Any] = {
            # Identifiers
            "invoice_agg_code":                _to_str_or_none(r.get("invoice_agg_code")),
            "item_listed_bills":               _to_str_or_none(r.get("item_listed_bills")),
            "statement_number":                _to_str_or_none(r.get("statement_number")),
            "invoice_number":                  _to_str_or_none(r.get("invoice_number")),
            "purchase_order_number":           _to_str_or_none(r.get("purchase_order_number")),
            "company_name":                    _to_str_or_none(r.get("company_name")),
            "company_code":                    _to_str_or_none(r.get("company_code")),
            "account_number":                  _to_str_or_none(r.get("account_number")),
            "mirn":                            _to_str_or_none(r.get("mirn")),
            "distributor":                     _to_str_or_none(r.get("distributor")),
            "billing_days":                    _to_int_or_none(r.get("billing_days")),

            # Dates
            "bill_start_date":                 _to_date_or_none(r.get("bill_start_date")),
            "bill_end_date":                   _to_date_or_none(r.get("bill_end_date")),
            "bill_issue_date":                 _to_date_or_none(r.get("bill_issue_date")),
            "read_start_date":                 _to_date_or_none(r.get("read_start_date")),
            "read_end_date":                   _to_date_or_none(r.get("read_end_date")),

            # Balances (Monthly-level)
            "opening_balance":                 _to_float_or_none(r.get("opening_balance")),
            "payment_received":                _to_float_or_none(r.get("payment_received")),
            "balance_carried_forward":         _to_float_or_none(r.get("balance_carried_forward")),

            # Balances (Statement-level)
            "statement_opening_balance":       _to_float_or_none(r.get("statement_opening_balance")),
            "statement_payment_received":      _to_float_or_none(r.get("statement_payment_received")),
            "statement_balance_carried_forward": _to_float_or_none(r.get("statement_balance_carried_forward")),
            "gj_consumption":                  _to_float_or_none(r.get("gj_consumption")),

            # Charges
            "firm_gas_amount":                 _to_float_or_none(r.get("firm_gas_amount")),
            "spot_gas_amount":                 _to_float_or_none(r.get("spot_gas_amount")),
            "atco_usage_amount":               _to_float_or_none(r.get("atco_usage_amount")),
            "atco_demand_amount":              _to_float_or_none(r.get("atco_demand_amount")),
            "atco_standing_amount":            _to_float_or_none(r.get("atco_standing_amount")),
            "transport_firm_amount":           _to_float_or_none(r.get("transport_firm_amount")),
            "transport_overrun_amount":        _to_float_or_none(r.get("transport_overrun_amount")),
            "gas_adjustment_charges":          _to_float_or_none(r.get("gas_adjustment_charges")),
            "distribution_adjustment_charges": _to_float_or_none(r.get("distribution_adjustment_charges")),
            "regulatory_adjustment_charges":   _to_float_or_none(r.get("regulatory_adjustment_charges")),
            "admin_fee":                       _to_float_or_none(r.get("admin_fee")),
            "late_payment_fee":                _to_float_or_none(r.get("late_payment_fee")),

            # Totals
            "total_amount":                    _to_float_or_none(r.get("total_amount")),
            "gst_amount":                      _to_float_or_none(r.get("gst_amount")),
            "total_in_gst_amount":             _to_float_or_none(r.get("total_in_gst_amount")),
            "statement_total_amount":          _to_float_or_none(r.get("statement_total_amount")),
            "statement_gst_amount":            _to_float_or_none(r.get("statement_gst_amount")),
            "statement_total_in_gst_amount":   _to_float_or_none(r.get("statement_total_in_gst_amount")),

            # System field
            "generated_at_utc":                _to_datetime_or_none(r.get("generated_at_utc") or datetime.utcnow()),
        }

        # Skip invalid rows (required rule)
        if not out["invoice_number"]:
            continue

        cleaned.append(out)

    return cleaned

# =========================
# Charts
# =========================
def generate_pie_chart(breakdown_series: pd.Series, custom_colors_map: Dict[str, str]) -> BytesIO:
    """
    Generates a styled pie chart with:
      - Percentage labels only
      - Colored dots around the pie
      - Two-column legend with matching dots below the chart
    Returns:
        BytesIO: PNG image data ready for embedding in PDF
    """
    if breakdown_series is None or breakdown_series.empty:
        return BytesIO()

    labels = breakdown_series.index.to_list()

    colors = [custom_colors_map.get(cat, "#2A3240") for cat in labels]

    fig, ax = plt.subplots(figsize=(3.8, 3.8))

    wedges, _, autotexts = ax.pie(
        breakdown_series.values,
        autopct=lambda p: f"{int(round(p))}%", 
        startangle=90,
        pctdistance=0.65,
        colors=colors,
        textprops={"fontsize": 8, "color": "black"}
    )

    ax.axis("equal")

    # Custom label layout (colored dot + percentage)
    for i, autotext in enumerate(autotexts):
        color = wedges[i].get_facecolor()
        percent = autotext.get_text()
        autotext.set_text("")
        x, y = autotext.get_position()
        ax.text(x + 0.04, y, percent, color="black", fontsize=8, ha="left", va="center")

    # Build custom legend below
    handles = [
        plt.Line2D([0], [0], marker="o", color="w", label=cat,
                   markerfacecolor=col, markersize=8)
        for cat, col in zip(labels, colors)
    ]

    fig.legend(
        handles=handles,
        labels=labels,
        loc="lower center",
        bbox_to_anchor=(0.5, 0.15),
        ncol=2,
        frameon=False,
        fontsize=8
    )

    plt.subplots_adjust(bottom=0.25)

    # Save to BytesIO instead of file
    buf = BytesIO()
    fig.savefig(buf, format="png", dpi=300, bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)
    return buf

def generate_consumption_chart(
    mirn: str,
    read_period: str,
    cust_consumption_df: pd.DataFrame,
    contract_mdq: Optional[float],
    fig_height_mm: float = 80.0
) -> BytesIO:
    if cust_consumption_df.empty:
        return BytesIO()

    df = cust_consumption_df.copy()
    df["gas_date"] = pd.to_datetime(df["gas_date"], errors="coerce")
    df["gj_consumption"] = pd.to_numeric(df["gj_consumption"], errors="coerce")
    df = df.dropna(subset=["gas_date", "gj_consumption"]).sort_values("gas_date")
    if df.empty:
        return BytesIO()

    start_date = df["gas_date"].min()
    if pd.isna(start_date) and df["pre_read_date"] is not None:
        start_date = df["pre_read_date"]

    end_date = df["gas_date"].max()

    fig_width_mm = 180.0
    fig = plt.figure(figsize=(fig_width_mm / 25.4, fig_height_mm / 25.4))
    ax = fig.add_subplot(111)

    light_blue = "#f0f8ff"
    fig.patch.set_facecolor(light_blue)
    ax.set_facecolor(light_blue)

    fig.subplots_adjust(left=0.12, right=0.98, top=0.94, bottom=0.32)

    ax.set_title(f"MIRN: {mirn}    Read Period (Current Billing Cycle): {read_period}", fontsize=7, fontweight="bold", pad=4)

    ax.bar(df["gas_date"], df["gj_consumption"], label="Daily Consumption", alpha=0.95, color="#0089D0")

    if contract_mdq is not None and np.isfinite(float(contract_mdq)) and float(contract_mdq) > 0:
        ax.axhline(
            y=float(contract_mdq),
            linewidth=1.5,
            color="#000000",
            label="Contract MDQ",
            zorder=3
        )

    ax.set_xlim(start_date - pd.Timedelta(days=0.7), end_date + pd.Timedelta(days=0.7))
    ax.xaxis.set_major_locator(mdates.DayLocator(interval=1))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%d %b"))
    ax.margins(x=0.005)

    ax.tick_params(axis="x", labelrotation=45, labelsize=6, pad=3, length=0)
    for lbl in ax.get_xticklabels():
        lbl.set_fontweight("bold")

    max_y = float(df["gj_consumption"].max() or 0)
    mdq_y = float(contract_mdq or 0)
    ymax = max(max_y, mdq_y)
    ax.set_ylim(0, ymax * 1.08 if ymax > 0 else 10)
    ax.set_axisbelow(True)
    ax.grid(which="both", axis="both", linestyle="--", linewidth=0.5)
    ax.tick_params(axis="y", labelsize=6)
    for s in ("top", "right", "left"):
        ax.spines[s].set_visible(False)
    ax.spines["bottom"].set_linewidth(1)

    handles, labels = ax.get_legend_handles_labels()
    if handles:
        fig.legend(
            handles, labels,
            loc="lower center",
            bbox_to_anchor=(0.5, -0.1),
            ncol=max(1, len(labels)),
            frameon=False,
            prop=FontProperties(size=6, weight="bold")
        )

    buf_c = BytesIO()
    fig.savefig(buf_c, format="png", dpi=300, bbox_inches="tight", facecolor=fig.get_facecolor())
    buf_c.seek(0)
    plt.close(fig)
    return buf_c

def generate_consumption_chart_basic(
    mirn: str,
    basic_read_period: str,
    basic_cust_consumption_df: pd.DataFrame,
    contract_mdq: Optional[float],
    fig_height_mm: float = 80.0
) -> BytesIO:
    if basic_cust_consumption_df.empty:
        return BytesIO()

    df = basic_cust_consumption_df.copy()
    df["cur_read_date"] = pd.to_datetime(df["cur_read_date"], errors="coerce")
    df["gj_consumption"] = pd.to_numeric(df["gj_consumption"], errors="coerce")
    df = df.dropna(subset=["cur_read_date", "gj_consumption"]).sort_values("cur_read_date")
    if df.empty:
        return BytesIO()

    max_date = df["cur_read_date"].max()

    start_date = (max_date - pd.DateOffset(months=5)).normalize().replace(day=1)

    end_date = max_date + pd.offsets.MonthEnd(0)

    fig_width_mm = 180.0
    fig = plt.figure(figsize=(fig_width_mm / 25.4, fig_height_mm / 25.4))
    ax = fig.add_subplot(111)

    light_blue = "#f0f8ff"
    fig.patch.set_facecolor(light_blue)
    ax.set_facecolor(light_blue)

    fig.subplots_adjust(left=0.12, right=0.98, top=0.94, bottom=0.32)

    ax.set_title(f"MIRN: {mirn}    Read Period (Current Billing Cycle): {basic_read_period}", fontsize=7, fontweight="bold", pad=4)

    ax.bar(df["cur_read_date"], df["gj_consumption"], width=10, label="Consumption", alpha=0.95, color="#0089D0")

    ax.set_xlim(start_date - pd.Timedelta(days=0.7), end_date + pd.Timedelta(days=0.7))
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=1))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b"))
    ax.margins(x=0.0005)

    ax.tick_params(axis="x", labelrotation=45, labelsize=6, pad=3, length=0)
    for lbl in ax.get_xticklabels():
        lbl.set_fontweight("bold")

    ymax = float(df["gj_consumption"].max() or 0)
    ax.set_ylim(0, ymax * 1.08 if ymax > 0 else 10)
    ax.set_axisbelow(True)
    ax.grid(which="both", axis="both", linestyle="--", linewidth=0.5)
    ax.tick_params(axis="y", labelsize=6)
    for s in ("top", "right", "left"):
        ax.spines[s].set_visible(False)
    ax.spines["bottom"].set_linewidth(1)

    handles, labels = ax.get_legend_handles_labels()
    if handles:
        fig.legend(
            handles, labels,
            loc="lower center",
            bbox_to_anchor=(0.5, -0.1),
            ncol=max(1, len(labels)),
            frameon=False,
            prop=FontProperties(size=6, weight="bold")
        )

    buf_c = BytesIO()
    fig.savefig(buf_c, format="png", dpi=300, bbox_inches="tight", facecolor=fig.get_facecolor())
    buf_c.seek(0)
    plt.close(fig)
    return buf_c

def generate_accounts_mirn_chart(
    read_period: str,
    df: pd.DataFrame,
    selected_mirns: list = None,
    customer_number: str = None,
    stack_consumption_chart_only: bool = False,
    aggregated_contract_mdq: float = None,
    period_start: pd.Timestamp = None,
    period_end: pd.Timestamp = None
) -> BytesIO:
    
    if df.empty:
        return BytesIO()

    use = df.copy()
    use["gas_date"] = pd.to_datetime(use["gas_date"], errors="coerce")
    use["gj_consumption"] = pd.to_numeric(use["gj_consumption"], errors="coerce")
    use["mirn"] = use["mirn"].astype(str)
    use = use.dropna(subset=["gas_date", "gj_consumption"]).sort_values("gas_date")

    if use.empty:
        return BytesIO()
    
    if customer_number:
        use = use[use["customer_number"].astype(str) == str(customer_number)]
        if use.empty:
            return BytesIO()

    if stack_consumption_chart_only and "stack_consumption_chart" in use.columns:
        use = use[use["stack_consumption_chart"] == "Yes"]
        if use.empty:
            return BytesIO()
    
    piv = use.pivot_table(
        index="gas_date",
        columns="mirn",
        values="gj_consumption",
        aggfunc="sum"
    )

    piv_start = piv.index.min()
    piv_end   = piv.index.max()

    # Determine chart start/end
    start_date = period_start or piv_start
    end_date   = period_end or piv_end
    current_period = f"{start_date.strftime('%d-%b-%y')} to {end_date.strftime('%d-%b-%y')}"


    full_index = pd.date_range(start=start_date, end=end_date, freq="D")
    piv = piv.reindex(full_index, fill_value=0)

    # Re-assign columns, index
    cols = sorted(list(piv.columns))
    x = piv.index
    bottom = np.zeros(len(piv), dtype=float)

    if pd.isna(start_date) or pd.isna(end_date):
        return BytesIO()
    
    fig_width_mm = 180
    fig_height_mm: float = 32.0
    fig = plt.figure(figsize=(fig_width_mm / 25.4, fig_height_mm / 25.4))
    ax = fig.add_subplot(111)

    light_blue = "#f0f8ff"
    fig.patch.set_facecolor(light_blue)
    ax.set_facecolor(light_blue)

    fig.subplots_adjust(left=0.12, right=0.98, top=0.94, bottom=0.32)

    ax.set_title(f"MIRN: Aggregated Consumption    Read Period (Current Billing Cycle): {current_period}",
                 fontsize=7, fontweight="bold", pad=4)

    all_prefixes = sorted({str(m)[:10] for m in piv.columns})

    palette = [
        "#2A9D8F", "#FE9666", "#A0A0A0", "#264653", "#E76F51",
        "#8AB17D", "#F4A261", "#6D6875", "#FF9F1C", "#2E4057",
        "#9D4EDD", "#00A896", "#FF6B6B", "#3A86FF", "#8338EC",
    ] 

    prefix_color_map = {
        prefix: palette[i % len(palette)]
        for i, prefix in enumerate(all_prefixes)
    }

    for i, c in enumerate(cols):
        mirn_prefix = str(c)[:10]
        color = prefix_color_map.get(mirn_prefix, "#264653")

        y = piv[c].values.astype(float)
        ax.bar(
            x, y,
            bottom=bottom,
            label=c,
            alpha=0.95,
            color=color
        )
        bottom += np.nan_to_num(y, nan=0.0)

        # --- Safe convert aggregated MDQ ---
        try:
            aggregated_contract_mdq = float(aggregated_contract_mdq)
        except Exception:
            aggregated_contract_mdq = None

    # --- Draw Aggregated MDQ line only if valid ---
    if aggregated_contract_mdq is not None and aggregated_contract_mdq > 0:
        ax.axhline(
            y=aggregated_contract_mdq,
            linewidth=1.5,
            color="#000000",
            label="Aggregated MDQ.",
            zorder=3
        )

    # --- Safe ymax calculation ---
    max_stack = float(np.nanmax(bottom)) if len(bottom) else 0.0

    if aggregated_contract_mdq is None:
        ymax = max_stack
    else:
        ymax = max(max_stack, aggregated_contract_mdq)

    ax.set_ylim(0, ymax * 1.08 if ymax > 0 else 10)
    ax.set_axisbelow(True)
    ax.grid(which="both", axis="both", linestyle="--", linewidth=0.5)
    ax.tick_params(axis="y", labelsize=6)
    for s in ("top", "right", "left"):
        ax.spines[s].set_visible(False)
    ax.spines["bottom"].set_linewidth(1)
        
    # Adding legend for 'mirn'
    handles, labels = ax.get_legend_handles_labels()
    ncols = max(1, len(labels))
    fig.legend(
        handles, labels,
        loc="lower center",
        bbox_to_anchor=(0.5, -0.1),
        ncol=ncols,
        frameon=False,
        prop=FontProperties(size=6, weight="bold")
    )    
    
    ax.set_xlim(
        start_date - pd.Timedelta(days=1),
        end_date   + pd.Timedelta(days=1)
    )

    # Build tick marks ONLY for billing-period range
    tick_positions = pd.date_range(start=start_date, end=end_date, freq="D")

    ax.set_xticks(tick_positions)
    ax.set_xticklabels(
        [d.strftime("%d %b") for d in tick_positions],
        rotation=45,
        fontsize=6
    )

    for lbl in ax.get_xticklabels():
        lbl.set_fontweight("bold")
    ax.tick_params(axis="x", length=0)

    buf_m = BytesIO()
    fig.savefig(buf_m, format="png", dpi=300, bbox_inches="tight", facecolor=fig.get_facecolor())
    buf_m.seek(0)
    plt.close(fig)
    return buf_m

def embed_chart_in_pdf(pdf: FPDF, chart_buf: BytesIO, chart_height_mm: float, logger: Optional[logging.Logger] = None) -> None:
    logger = logger or logging.getLogger("SharePointLogger")
    """
    Embeds a chart (from a BytesIO buffer) into the PDF.
    
    Args:
        pdf (FPDF): The PDF document to embed the chart into.
        chart_buf (BytesIO): The BytesIO buffer containing the chart image data.
        chart_height_mm (float): The desired height for the chart in mm.
    """
    # Check the size of the chart buffer before embedding
    chart_size = len(chart_buf.getvalue())
    if chart_size == 0:
        logger.error("⏭️ The chart buffer is empty. Skipping chart embedding.")
        return

    y_start = pdf.get_y()  # Current y position of the PDF
    CHART_W_MM = 180.0
    pdf.image(chart_buf, x=10, y=y_start, w=CHART_W_MM, h=chart_height_mm)
    pdf.set_y(y_start + chart_height_mm + 2)  # Update the y position for the next element
    logger.info(f"Chart embedded at y={y_start}, height={chart_height_mm}")

# =========================
# Writeback helpers
# =========================
def _to_str_or_none(x: Any) -> Optional[str]:
    s = None if x is None or (isinstance(x, float) and np.isnan(x)) else str(x).strip()
    return s if s else None

def _to_float_or_none(x: Any) -> Optional[float]:
    try:
        if x is None or (isinstance(x, float) and np.isnan(x)):
            return None
        return float(x)
    except Exception:
        return None
    
def _to_int_or_none(v):
    if v is None or v == "":
        return None
    try:
        return int(v)
    except:
        try:
            return int(float(v))
        except:
            return None

def _to_date_or_none(x: Any) -> Optional[date]:
    try:
        if x is None or (isinstance(x, float) and np.isnan(x)):
            return None
        dt = pd.to_datetime(x, errors="coerce")
        if pd.isna(dt):
            return None
        return dt.date()
    except Exception:
        return None

def _to_datetime_or_none(x: Any) -> Optional[datetime]:
    if x is None:
        return None
    try:
        dt = pd.to_datetime(x, errors="coerce")
        if pd.isna(dt):
            return None
        return dt.to_pydatetime().replace(tzinfo=None)
    except Exception:
        return None

def _norm2(x: Any) -> Decimal:
    """Two-decimal money for equality compares."""
    if x is None or (isinstance(x, float) and pd.isna(x)):
        x = "0"
    return Decimal(str(x)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

def _fetch_history_variants(engine, base_invoice_number: str) -> List[Tuple[str, int, Decimal]]:
    """
    Returns list of tuples (invoice_number, suffix_int, total_inc_norm)
    Only rows where invoice_number == base OR base_\\d+ are kept.
    """
    like = base_invoice_number + "%"
    sql = f"SELECT invoice_number, total_in_gst_amount FROM {HISTORY_TABLE} WHERE invoice_number LIKE ?"
    pat = re.compile(r"^" + re.escape(base_invoice_number) + r"(?:_(\d+))?$")

    with engine.begin() as con:
        rows = con.exec_driver_sql(sql, (like,)).fetchall()

    variants: List[Tuple[str, int, Decimal]] = []
    for inv, total_in_gst_amount in rows:
        m = pat.match(inv or "")
        if not m:
            continue
        sfx = int(m.group(1) or "1")  # base counted as 1
        variants.append((inv, sfx, _norm2(total_in_gst_amount)))
    return variants

def _fetch_statement_variants(engine, base_statement_number: str) -> List[Tuple[str, int, Decimal]]:
    """
    Returns list of tuples (statement_number, suffix_int, total_inc_norm)
    Only rows where statement_number == base OR base_\\d+ are kept.
    """
    like = base_statement_number + "%"
    sql = f"""
        SELECT statement_number, total_in_gst_amount
        FROM {HISTORY_TABLE}
        WHERE statement_number LIKE ?
    """
    pat = re.compile(r"^" + re.escape(base_statement_number) + r"(?:_(\d+))?$")

    with engine.begin() as con:
        rows = con.exec_driver_sql(sql, (like,)).fetchall()

    variants: List[Tuple[str, int, Decimal]] = []
    for st_no, total_in_gst_amount in rows:
        m = pat.match(st_no or "")
        if not m:
            continue
        sfx = int(m.group(1) or "1")   # base counted as 1
        variants.append((st_no, sfx, _norm2(total_in_gst_amount)))

    return variants

def _strip_pdf_ext(name: str) -> str:
    return name[:-4] if isinstance(name, str) and name.lower().endswith(".pdf") else name

def _money2(x):
    if x is None:
        return None
    try:
        xf = float(x)
        if math.isnan(xf) or math.isinf(xf):
            return None
        return round(xf, 2)
    except Exception:
        return None

def build_statement_content_hash(
    invoices: Iterable[Tuple[str, float]]
) -> str:
    """
    invoices: iterable of (invoice_number, invoice_total_in_gst)
    """
    norm = [(str(inv).strip(), str(_money2(amt))) for inv, amt in invoices]
    norm.sort(key=lambda t: t[0])  # stable order
    payload = "|".join(f"{inv}:{amt}" for inv, amt in norm)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()

@dataclass
class StatementRunCache:
    chosen_name_by_base: dict[str, str] = field(default_factory=dict)
    emitted: set[str] = field(default_factory=set)

def _suffix_num(base: str, stmt: str) -> int:
    """base -> 0 (meaning 'base'), base_2 -> 2, etc"""
    if stmt == base:
        return 0
    m = re.fullmatch(re.escape(base) + r"_(\d+)", stmt)
    return int(m.group(1)) if m else 0

def _db_list_statement_variants(engine, base: str) -> List[str]:
    sql = f"""
        SELECT DISTINCT statement_number
        FROM {HISTORY_TABLE}
        WHERE statement_number = ? OR statement_number LIKE ?
    """
    like = f"{base}_%"
    with engine.begin() as con:
        rows = con.exec_driver_sql(sql, (base, like)).fetchall()
    return [r[0] for r in rows if r and r[0]]

def _db_get_statement_meta(engine, stmt_no: str) -> Dict[str, Any]:
    # if you DON'T have content_hash column yet, just return total
    sql = f"""
        SELECT TOP 1 statement_total_in_gst_amount
        FROM {HISTORY_TABLE}
        WHERE statement_number = ?
        ORDER BY generated_at_utc DESC
    """
    with engine.begin() as con:
        row = con.exec_driver_sql(sql, (stmt_no,)).fetchone()
    if not row:
        return {}
    return {"total": row[0], "content_hash": None}

def _db_get_statement_signature(engine, stmt_no: str) -> Dict[str, Any]:
    """
    Build a stable signature for a statement from dbo.billing_history.
    This avoids needing a content_hash column.
    """
    sql = f"""
        SELECT invoice_number, total_in_gst_amount, statement_total_in_gst_amount
        FROM {HISTORY_TABLE}
        WHERE statement_number = ?
    """
    with engine.begin() as con:
        rows = con.exec_driver_sql(sql, (stmt_no,)).fetchall()

    if not rows:
        return {}

    invoices = []
    stmt_totals = []
    for inv_no, inv_total, stmt_total in rows:
        if inv_no:
            inv_no_s = _norm_id(inv_no)
            invoices.append((inv_no_s, round(float(inv_total or 0.0), 2)))
        if stmt_total is not None:
            stmt_totals.append(round(float(stmt_total), 2))

    sig = {
        "content_hash": build_statement_content_hash(invoices),
        # statement total should be identical across rows; pick max defensively
        "total": max(stmt_totals) if stmt_totals else None,
    }
    return sig

def _root_base(stmt: str) -> str:
    """
    Only strips a duplicate suffix like _2 or _10.
    Will NOT strip account numbers like _30013.
    """
    s = (stmt or "").strip()
    parts = s.split("_")
    if len(parts) >= 4 and parts[-1].isdigit() and len(parts[-1]) <= 3:
        # Only treat as suffix if previous segment looks like an account number
        if parts[-2].isdigit() and len(parts[-2]) >= 4:
            return "_".join(parts[:-1])
    return s

def _norm_id(x) -> str:
    if x is None:
        return ""
    s = str(x).strip()

    # pandas float artifact: "30013.0" -> "30013"
    if s.endswith(".0") and s[:-2].isdigit():
        s = s[:-2]

    return s

def should_process_statement(
    eng,
    statement_number: str,
    statement_total_in_gst_amount: float,
    *,
    content_hash: Optional[str] = None,
    run_cache: Optional[StatementRunCache] = None,
    sp_index: Optional["SharePointFolderIndex"] = None,
    logger: Optional[logging.Logger] = None,
) -> Tuple[bool, str]:

    logger = logger or logging.getLogger(__name__)

    base_in = (statement_number or "").strip()
    if not base_in:
        return True, base_in

    root = _root_base(base_in)

    # ---- prevent re-suffixing in same run ----
    # Use ROOT as the key so TRI_... and TRI_..._2 share one decision.
    if run_cache and root in run_cache.chosen_name_by_base:
        chosen = run_cache.chosen_name_by_base[root]
        if chosen in run_cache.emitted:
            return False, chosen
        run_cache.emitted.add(chosen)
        return True, chosen

    curr_total = _money2(statement_total_in_gst_amount)
    curr_hash = content_hash or f"TOTAL:{curr_total}"

    # DB variants (scan using ROOT)
    db_variants = _db_list_statement_variants(eng, root)

    # SP variants (scan using ROOT)
    sp_variants = []
    if sp_index:
        try:
            sp_variants = sp_index.list_variants_stem(root)  # must be ["ROOT", "ROOT_2", ...]
        except Exception as e:
            logger.warning(f"SharePoint variant scan failed for {root}: {e}")

    # Use DB variants ONLY for equality/skip checks
    # IMPORTANT: suffix parsing must also be relative to ROOT
    for v in sorted(set(db_variants), key=lambda v: (_suffix_num(root, v), v)):
        meta = _db_get_statement_signature(eng, v) or {}
        prev_hash = meta.get("content_hash")
        prev_total = meta.get("total")

        if content_hash is not None and prev_hash and prev_hash == curr_hash:
            logger.info(f"⏭️ Statement {v} already exists with same invoice composition; skipping.")
            return False, v

        if prev_total is not None and _money2(prev_total) == curr_total:
            logger.info(f"⏭️ Statement {v} already exists with same total; skipping.")
            return False, v

    # Use BOTH DB + SP variants ONLY to choose next suffix (relative to ROOT)
    all_variants = sorted(set(db_variants + sp_variants), key=lambda v: (_suffix_num(root, v), v))
    if all_variants:
        max_sfx = max(_suffix_num(root, v) for v in all_variants)
        chosen = f"{root}_2" if max_sfx == 0 else f"{root}_{max_sfx + 1}"
    else:
        chosen = root

    if run_cache:
        run_cache.chosen_name_by_base[root] = chosen
        run_cache.emitted.add(chosen)

    logger.info(f"✅ Will process statement {chosen} (new or changed).")
    return True, chosen

def _apply_history_increment_rule(engine, row: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], str, bool]:
    """
    One history row per invoice_number unless the *same invoice_number* has a different total:
      - If any existing variant (base or suffixed) has the SAME total -> SKIP insert and return that variant's invoice_number.
      - Else create next suffix and INSERT.
      - If no variants exist -> INSERT base as-is.

    Returns: (row_to_insert_or_None, invoice_number_used, inserted_bool)
    """
    base = row.get("invoice_number")
    if not base:
        return None, "", False

    curr_total_in_gst_amount = row.get("total_in_gst_amount")
    if curr_total_in_gst_amount is None:
        curr_total_in_gst_amount = (float(row.get("total_amount") or 0) + float(row.get("gst_amount") or 0))
    curr_norm = _norm2(curr_total_in_gst_amount)

    variants = _fetch_history_variants(engine, base)
    if not variants:
        return row, base, True

    for inv_no, _sfx, tot_norm in variants:
        if tot_norm == curr_norm:
            return None, inv_no, False

    next_suffix = max(v[1] for v in variants) + 1
    r2 = dict(row)
    r2["invoice_number"] = f"{base}_{next_suffix}"
    return r2, r2["invoice_number"], True

def insert_billing_history_batch(engine, rows: List[Dict[str, Any]]) -> List[str]:
    """
    Inserts billing_history rows (one per invoice).
    Matches dbo.billing_history schema.
    Skips duplicates based on invoice_number.
    """

    if not rows:
        logging.info("No billing_history rows to insert.")
        return []

    # sanitize all rows first
    rows = sanitize_history_rows(rows)
    inserted_invoices = []

    # FULL COLUMN LIST – MUST MATCH SQL TABLE EXACTLY
    cols = [
        "invoice_agg_code",
        "item_listed_bills",
        "statement_number",
        "invoice_number",
        "purchase_order_number",
        "company_name",
        "company_code",
        "account_number",
        "mirn",
        "distributor",

        # Dates
        "bill_start_date",
        "bill_end_date",
        "bill_issue_date",
        "read_start_date",
        "read_end_date",

        "billing_days",
        "gj_consumption",

        # Monthly-level balances
        "opening_balance",
        "payment_received",
        "balance_carried_forward",

        # Statement-level balances
        "statement_opening_balance",
        "statement_payment_received",
        "statement_balance_carried_forward",

        # Charges
        "firm_gas_amount",
        "spot_gas_amount",
        "atco_usage_amount",
        "atco_demand_amount",
        "atco_standing_amount",
        "transport_firm_amount",
        "transport_overrun_amount",
        "gas_adjustment_charges",
        "distribution_adjustment_charges",
        "regulatory_adjustment_charges",
        "admin_fee",
        "late_payment_fee",

        # Totals
        "total_amount",
        "gst_amount",
        "total_in_gst_amount",
        "statement_total_amount",
        "statement_gst_amount",
        "statement_total_in_gst_amount",

        # System field
        "generated_at_utc",
    ]

    placeholders = ",".join(["?"] * len(cols))

    sql = f"""
    INSERT INTO {HISTORY_TABLE} ({','.join(cols)})
    SELECT {placeholders}
    WHERE NOT EXISTS (
        SELECT 1 FROM {HISTORY_TABLE} WHERE invoice_number = ?
    )
    """

    # Debug info
    with engine.begin() as con:
        dbname = con.exec_driver_sql("SELECT DB_NAME()").scalar()
        before = con.exec_driver_sql(f"SELECT COUNT(*) FROM {HISTORY_TABLE}").scalar()

    logging.info(f"Target DB: {dbname}; {HISTORY_TABLE} count before insert: {before}")
    logging.info(f"Inserting {len(rows)} history rows")

    inserted = 0

    # Insert rows
    with engine.begin() as con:
        for r in rows:
            inv_no = r.get("invoice_number")
            params = tuple(r.get(c) for c in cols) + (inv_no,)
            result = con.exec_driver_sql(sql, params)

            if result.rowcount > 0:
                inserted += 1
                inserted_invoices.append(inv_no)
            else:
                logging.info(f"⏭️ Duplicate history row skipped (invoice {inv_no})")

    # Final count
    with engine.begin() as con:
        after = con.exec_driver_sql(f"SELECT COUNT(*) FROM {HISTORY_TABLE}").scalar()

    logging.info(f"Inserted {inserted} new rows. After count: {after} (Δ={after - before})")

    return inserted_invoices

# =========================
# SharePoint file download
# =========================
def download_file_from_sharepoint(remote_filename: str) -> bytes | None:
    try:
        ctx = get_sharepoint_context()
        folder = ctx.web.get_folder_by_server_relative_url(FOLDER)
        file = folder.files.get_by_url(remote_filename)

        buf = BytesIO()
        file.download(buf).execute_query()
        buf.seek(0)
        return buf.read()
    except Exception as e:
        print(f"Error downloading {remote_filename}: {e}")
        return None
    
# =========================
# SharePoint duplicate helpers
# =========================

class SharePointFolderIndex:
    def __init__(self, sp, refresh_seconds: int = 300):
        self.sp = sp
        self.refresh_seconds = refresh_seconds
        self._last_refresh = 0
        self.names = set()               # casefolded filenames
        self.original = {}               # casefolded -> original filename
        self.max_suffix = {}             # base(casefold) -> max suffix int (0=base)

    def refresh(self, force: bool = False):
        now = time.time()
        if not force and (now - self._last_refresh) < self.refresh_seconds and self.names:
            return

        file_names = self.sp.list_file_names()
        self.names = {n.casefold() for n in file_names}
        self.original = {n.casefold(): n for n in file_names}

        self.max_suffix = {}
        pat = re.compile(r"^(?P<base>.+?)(?:_(?P<sfx>\d+))?\.pdf$", re.IGNORECASE)
        for n in file_names:
            m = pat.match(n)
            if not m:
                continue
            base = m.group("base")
            sfx = int(m.group("sfx") or 0)
            bcf = base.casefold()
            self.max_suffix[bcf] = max(self.max_suffix.get(bcf, 0), sfx)

        self._last_refresh = now

    def exists(self, filename: str) -> bool:
        self.refresh()
        return filename.casefold() in self.names

    def list_variants_stem(self, base: str) -> list[str]:
        """
        Returns stems (no .pdf): [base, base_2, base_3 ...] present in SharePoint.
        """
        self.refresh()
        bcf = base.casefold()
        out = []
        for name_cf in self.names:
            if not name_cf.endswith(".pdf"):
                continue
            stem_cf = name_cf[:-4]
            if stem_cf == bcf:
                out.append(base)
            elif stem_cf.startswith(bcf + "_"):
                sfx = stem_cf[len(bcf) + 1:]
                if sfx.isdigit():
                    out.append(f"{base}_{int(sfx)}")
        return out

    def remember_uploaded(self, filename: str):
        self.refresh()  # ensure structures exist
        fn_cf = filename.casefold()
        self.names.add(fn_cf)
        self.original[fn_cf] = filename

        m = re.match(r"^(?P<stem>.+?)\.pdf$", filename, re.IGNORECASE)
        if not m:
            return
        stem = m.group("stem")
        base, sfx = stem, 0
        mm = re.match(r"^(?P<base>.+?)_(?P<sfx>\d+)$", stem)
        if mm:
            base = mm.group("base")
            sfx = int(mm.group("sfx"))
        bcf = base.casefold()
        self.max_suffix[bcf] = max(self.max_suffix.get(bcf, 0), sfx)

def _get_sharepoint_total(base_filename: str) -> Optional[Decimal]:
    """
    Placeholder stub for retrieving statement total from SharePoint metadata or a log file.
    You can later extend this to read metadata or a stored CSV/JSON alongside the PDF.
    For now, return None to rely on DB comparisons only.
    """
    return None

def compute_aggregated_mdq(daily_df: pd.DataFrame,
                           selected_mirns: list,
                           start_date: Optional[pd.Timestamp],
                           end_date: Optional[pd.Timestamp]) -> Optional[float]:
    """
    Max daily GJ consumption across the selected MIRNs within the billing period.
    """
    if daily_df.empty or not selected_mirns:
        return None

    df = daily_df.copy()
    df["gas_date"] = pd.to_datetime(df["gas_date"], errors="coerce")
    df["gj_consumption"] = pd.to_numeric(df["gj_consumption"], errors="coerce")
    df["mirn"] = df["mirn"].astype(str)
    df = df.dropna(subset=["gas_date", "gj_consumption"])

    # Filter to selected MIRNs and the invoice's billing window
    df = df[df["mirn"].isin([str(m) for m in selected_mirns])]
    if pd.notna(start_date):
        df = df[df["gas_date"] >= start_date.normalize()]
    if pd.notna(end_date):
        df = df[df["gas_date"] <= end_date.normalize()]

    if df.empty:
        return None

    # Sum all selected MIRNs per day, then take the maximum day total
    daily_totals = df.groupby("gas_date", as_index=False)["gj_consumption"].sum()
    return float(daily_totals["gj_consumption"].max()) if not daily_totals.empty else None

def compute_total_consumption(daily_df: pd.DataFrame,
                           selected_mirns: list,
                           start_date: Optional[pd.Timestamp],
                           end_date: Optional[pd.Timestamp]) -> Optional[float]:
    """
    Total GJ consumption across the selected MIRNs within the billing period.
    """
    if daily_df.empty or not selected_mirns:
        return None

    df = daily_df.copy()
    df["gas_date"] = pd.to_datetime(df["gas_date"], errors="coerce")
    df["gj_consumption"] = pd.to_numeric(df["gj_consumption"], errors="coerce")
    df["mirn"] = df["mirn"].astype(str)
    df = df.dropna(subset=["gas_date", "gj_consumption"])

    # Filter to selected MIRNs and the invoice's billing window
    df = df[df["mirn"].isin([str(m) for m in selected_mirns])]
    if pd.notna(start_date):
        df = df[df["gas_date"] >= start_date.normalize()]
    if pd.notna(end_date):
        df = df[df["gas_date"] <= end_date.normalize()]

    if df.empty:
        return None

    # Sum all selected MIRNs per day, then take the maximum day total
    daily_totals = df.groupby("gas_date", as_index=False)["gj_consumption"].sum()
    return float(daily_totals["gj_consumption"].sum()) if not daily_totals.empty else None

def write_label_value(pdf,
        label: str, value: Any, x: Optional[float] = None, y: Optional[float] = None,
        label_w: float = 35, value_w: Optional[float] = None, align: str = "L",
        bold_size: float = 9, reg_size: float = 9, wrap: bool = False,
        force_two_lines: bool = False, suffix: str = "", unit: str = "",
        move_down: bool = True, line_height: float = 5
    ) -> None:
        if x is not None and y is not None:
            pdf.set_xy(x, y)
        elif x is not None:
            pdf.set_x(x)
        pdf.set_font("Arial", "B", bold_size)
        pdf.cell(label_w, line_height, str(label), new_x=XPos.RIGHT, new_y=YPos.TOP, align=align)
        value_str = "" if (value is None or str(value).strip() == "") else str(value) + unit + suffix
        pdf.set_font("Arial", "", reg_size)                
        if wrap or force_two_lines:
            current_y = pdf.get_y()
            val_x = pdf.get_x()
            if value_w is None:
                value_w = 190 - val_x - 10
            pdf.set_xy(val_x, current_y)
            pdf.multi_cell(value_w, line_height, value_str)

            # Always consume two lines if force_two_lines=True
            if force_two_lines:
                lines_used = max(2, int((pdf.get_y() - current_y) / line_height + 0.5))
                target_y = current_y + (2 * line_height)
                pdf.set_y(target_y)
            elif move_down:
                pdf.set_y(pdf.get_y())

        else:
            if value_w:
                pdf.cell(value_w, line_height, value_str, new_x=XPos.LMARGIN, new_y=YPos.NEXT, align=align)
            else:
                pdf.cell(0, line_height, value_str, new_x=XPos.LMARGIN, new_y=YPos.NEXT, align=align)


# =========================
# PDF Class
# =========================
class PDF(FPDF):
    def __init__(self, logo_filename="Agora Logo.png", *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Download logo from the SharePoint folder specified by FOLDER_LOGO
        self.logo_bytes = self.download_file_from_sharepoint(logo_filename)

    def download_file_from_sharepoint(self, logo_filename):
        ctx = get_sharepoint_context()
        try:
            file = ctx.web.get_file_by_server_relative_url(
                f"{FOLDER_LOGO.rstrip('/')}/{logo_filename}"
            )
            buf = BytesIO()
            file.download(buf).execute_query()
            buf.seek(0)
            return buf.read()
        except ClientRequestException as ex:
            print(f"SharePoint error downloading {logo_filename}: {ex}")
            return None
        except Exception as e:
            print(f"Unexpected error downloading {logo_filename}: {e}")
            return None

    def header(self):
        if self.logo_bytes:
            try:
                self.image(BytesIO(self.logo_bytes), x=10, y=5, w=71)
            except Exception as e:
                logger.error(f"Error rendering logo: {e}")
        self.set_font("Arial", "", 8)
        right_edge = 190
        lines = [
            "St Georges Terrace",
            "WA 6865",
            "PO Box Z5538",
            "Tel: 61 8 9228 1930",
            "ABN 68 612 806 381",
            "www.agoraretail.com.au"
        ]
        for i, line in enumerate(lines):
            text_width = self.get_string_width(line)
            self.set_xy(right_edge - text_width, 8 + i * 4.4)
            self.cell(text_width, 4.4, line)
        self.ln(5)
        self.set_draw_color(0, 0, 0)
        self.set_line_width(0.35)
        self.line(10, self.get_y(), 190, self.get_y())

    def footer(self):
        # Footer runs automatically on every page
        self.set_y(-15)
        self.set_font("Arial", "", 7)

        # Page number dynamic
        page_text = f"Page {self.page_no()} of {{nb}}"
        self.cell(0, 10, page_text, 0, 0, "C")

def unpack_invoice_fields(inv, breakdown):
    """Extracts all relevant invoice fields into local variables and returns them as a dict."""
    abn = inv.get("abn")
    if abn is not None:
        abn = str(abn)

    fields = {
        "invoice_agg_code":inv.get("invoice_agg_code"),
        "item_listed_bills":inv.get("item_listed_bills"),
        "stack_consumption_chart":inv.get("stack_consumption_chart"),
        "company_name": inv.get("company_name"),
        "abn": (
            f"{abn[:2]} {abn[2:5]} {abn[5:8]} {abn[8:]}"
            if abn and len(abn) == 11
            else abn
        ),
        "postal_address": inv.get("postal_address"),
        "contact_number": inv.get("contact_number"),
        "distributor_name": inv.get("distributor"),
        "emergency_contact": inv.get("emergency_number"),
        "customer_number": inv.get("customer_number"),
        "statement_account_number": inv.get("statement_account_number"),
        "purchase_order": inv.get("purchase_order_number"),
        "statement_number": inv.get("statement_number"),
        "start_date": inv.get("bill_start_date"),
        "end_date": inv.get("bill_end_date"),
        "issue_date": inv.get("bill_issue_date"),
        "due_date": inv.get("bill_due_date"),
        "billing_days": inv.get("billing_days"),
        "statement_total_amount": float(inv.get("statement_total_amount") or 0.0),
        "statement_gst_amount": float(inv.get("statement_gst_amount") or 0.0),
        "statement_total_in_gst_amount": float(inv.get("statement_total_in_gst_amount") or 0.0),
        "invoice_number": inv.get("invoice_number"),
        "acct": inv.get("account_number"),
        "mirn": inv.get("mirn"),
        "interval_metering": inv.get("interval_metering"),
        "meter_number": inv.get("meter_number"),
        "trading_name": inv.get("trading_name"),
        "premises_address": inv.get("premises_address"),
        "transmission_pipeline": inv.get("transmission_pipeline"),
        "distributor_mhq": inv.get("distribution_mhq"),
        "charge_notes": inv.get("notes"),
        "total_amount": float(inv.get("total_amount") or 0.0),
        "gst_amount": float(inv.get("gst_amount") or 0.0),
        "total_in_gst_amount": float(inv.get("total_in_gst_amount") or 0.0),
        "total_consumption": float(inv.get("gj_consumption") or 0.0),
        "read_start_date": inv.get("read_start_date"),
        "read_end_date": inv.get("read_end_date"),
        "spot_gas_amount": inv.get("spot_gas_amount"),
        "statement_opening_balance": float(inv.get("statement_opening_balance") or 0.0),
        "statement_payment_received": float(inv.get("statement_payment_received") or 0.0),
        "statement_balance_carried_forward": float(inv.get("statement_balance_carried_forward") or 0.0),
        "opening_balance": float(inv.get("opening_balance") or 0.0),
        "payment_received": float(inv.get("payment_received") or 0.0),
        "balance_carried_forward": float(inv.get("balance_carried_forward") or 0.0),
        "read_type": inv.get("read_type") or ""
    }

    # Build charges_df subset for this invoice
    invoice_number = fields["invoice_number"]
    fields["charges_df"] = build_charges_df_from_breakdown(
        breakdown[breakdown["invoice_number"] == invoice_number]
    )

    # Shared color palette
    fields["custom_colors"] = {
        "Firm Gas Sales": "#0089D0",
        "Overrun Charges": "#9DC0D7",
        "Transport Fee": "#425B7E",
        "Distribution Charges": "#F4A261",
        "Adjustment Charges": "#E76F51",
        "Other Charges": "#2A9D8F",
    }

    return fields


def generate_statement_summary_page(pdf, inv, breakdown, logger, daily, invoice_numbers, headers):
    f = unpack_invoice_fields(inv, breakdown)
    invoice_agg_code = f["invoice_agg_code"]
    item_listed_bills, company_name, abn, postal_address = f["item_listed_bills"], f["company_name"], f["abn"], f["postal_address"]
    contact_number, distributor_name = f["contact_number"], f["distributor_name"]
    emergency_contact, customer_number = f["emergency_contact"], f["customer_number"]
    statement_account_number, purchase_order = f["statement_account_number"], f["purchase_order"]
    statement_number, start_date, end_date = f["statement_number"], f["start_date"], f["end_date"]
    issue_date, due_date = f["issue_date"], f["due_date"]
    statement_total_amount = f.get("statement_total_amount") or f.get("total_amount")
    statement_total_in_gst_amount = f.get("statement_total_in_gst_amount") or f.get("total_in_gst_amount")
    statement_gst_amount = f.get("statement_gst_amount") or f.get("gst_amount")
    charges_df, custom_colors, invoice_number = f["charges_df"], f["custom_colors"], f["invoice_number"]
    statement_opening_balance, statement_payment_received, statement_balance_carried_forward = f["statement_opening_balance"], f["statement_payment_received"], f["statement_balance_carried_forward"]
    read_type = f["read_type"]
    interval_metering = f["interval_metering"]

    pdf.add_page()

    # Title
    pdf.set_font("Arial", "", 16)
    pdf.set_x(10); pdf.set_y(35)

    if invoice_agg_code is not None and item_listed_bills is None:
        pdf.cell(0, 10, "Statement", new_x=XPos.LMARGIN, new_y=YPos.NEXT, align="L")
    else:
        pdf.cell(0, 10, "Tax Invoice", new_x=XPos.LMARGIN, new_y=YPos.NEXT, align="L")
    
    pdf.ln(10)

    # Left: Customer block (use company, ABN, postal address)
    x0, y0 = 10, 45
    bg_w = 90
    line_h = 6

    # Compute an approximate block height without using split_only (compat with older fpdf2)
    postal_text = f"{postal_address or ''}\n"
    postal_lines_est = max(1, len(str(postal_text).splitlines()))
    block_h = 6 + 6 + 4 + 6 + 6 + 6

    pdf.set_fill_color(220, 230, 241)
    pdf.rect(x0, y0, bg_w, block_h, style='F')
    pdf.set_xy(x0, y0)
    pdf.set_font("Arial", "B", 10)
    pdf.cell(80, 6, str(company_name or ""), new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.cell(80, 6, f"ABN: {abn or ''}", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.cell(80, 4, "", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.cell(80, 6, "Accounts Payable", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.multi_cell(80, 6, postal_text, new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.ln(10)

    # Right: Contact & Emergency
    right_edge = 190
    block_start_x = 90
    label_width = 70
    value_width = (right_edge - block_start_x) - label_width

    pdf.set_xy(block_start_x, 45)
    pdf.set_font("Arial", "B", 9); pdf.cell(label_width, 9, "Contact Us", align='R')
    pdf.set_font("Arial", "", 9.5); pdf.cell(value_width, 9, "1800 403 093", new_x=XPos.LMARGIN, new_y=YPos.NEXT, align='R')
    pdf.set_x(block_start_x); pdf.set_font("Arial", "B", 9)
    pdf.multi_cell(label_width, 6, f"Distributor ({distributor_name or ''})\n Contact Number", align='R')
    pdf.set_xy(block_start_x + label_width, pdf.get_y() - 12)
    pdf.set_font("Arial", "", 9.5); pdf.multi_cell(value_width, 12, str(contact_number or ""), align='R')
    pdf.set_x(block_start_x); pdf.set_font("Arial", "BU", 9)
    pdf.multi_cell(label_width, 7, f"{distributor_name or ''} Gas Fault & Emergency\n Contact (24 Hrs)", align='R')
    pdf.set_xy(block_start_x + label_width, pdf.get_y() - 14)
    pdf.set_font("Arial", "U", 9.5); pdf.multi_cell(value_width, 12, str(emergency_contact or ""), align='R')

    # Divider
    y_top = 82; pdf.set_draw_color(0,0,0); pdf.set_line_width(0.35); pdf.line(10, y_top, 190, y_top)

    # Left: Account & invoice info
    pdf.set_xy(10, 85)
    write_label_value(pdf, "Customer Number", customer_number, x=10)
    write_label_value(pdf, "Account Number", statement_account_number, x=10)
    pdf.ln(4)
    write_label_value(pdf, "Purchase Order #", purchase_order, x=10, line_height=6)
    if invoice_agg_code is not None and not item_listed_bills:
        write_label_value(pdf, "Statement No.", statement_number, x=10, line_height=6)
    else:
        write_label_value(pdf, "Tax Invoice No.", statement_number, x=10, line_height=6)

    start_dt = pd.to_datetime(start_date, errors="coerce")
    end_dt   = pd.to_datetime(end_date,   errors="coerce")
    billing_period_lbl = (
        f"{start_dt.strftime('%d-%b-%y')} to {end_dt.strftime('%d-%b-%y')}"
        if pd.notna(start_dt) and pd.notna(end_dt) else ""
    )
    write_label_value(pdf, "Billing Cycle", billing_period_lbl, x=10, line_height=6)

    # Right: Issue/Due/Total
    right_start_x = block_start_x
    label_w_right = label_width
    value_w_right = value_width
    pdf.set_xy(right_start_x, 85)
    issue_dt = pd.to_datetime(issue_date, errors="coerce")
    due_dt   = pd.to_datetime(due_date,   errors="coerce")

    write_label_value(pdf, 
        "Issue Date",
        issue_dt.strftime("%d-%b-%y") if pd.notna(issue_dt) else "",
        x=right_start_x, label_w=label_w_right, value_w=value_w_right,
        align="R", bold_size=10, reg_size=10.5
    )

    write_label_value(pdf, 
        "Due Date",
        due_dt.strftime("%d-%b-%y") if pd.notna(due_dt) else "",
        x=right_start_x, label_w=label_w_right, value_w=value_w_right,
        align="R", bold_size=10, reg_size=10.5
    )

    pdf.ln(16)
    write_label_value(pdf, 
        "Total Amount Payable", f"${statement_total_in_gst_amount:,.2f}",
        x=right_start_x, label_w=label_w_right, value_w=value_w_right,
        align="R", bold_size=10, reg_size=10.5)

    # Divider
    pdf.ln(10); pdf.set_draw_color(0,0,0); pdf.set_line_width(0.35); pdf.line(10, 119, 190, 119)

    # Gas Account Summary (left)
    pdf.set_xy(10, 119)
    if invoice_agg_code is None or str(invoice_agg_code).strip() == "":
        pdf.set_font("Arial", "I", 7)
        if not (interval_metering or "").startswith("Interval") and read_type == "A":
            msg = "This invoice is based on the actual usage data"
        elif not (interval_metering or "").startswith("Interval") and read_type == "E":
            msg = "This invoice is based on the estimated usage data"
        elif not (interval_metering or "").startswith("Interval") and read_type == "S":
            msg = "This invoice is based on the substitute usage data"
        else:
            msg = "This invoice is based on usage data provided by network providers"
        pdf.cell(50, 8, msg, border=0)
    else:
        pdf.set_font("Arial", "I", 7)  
        pdf.cell(50, 8, "This invoice is based on usage data provided by network providers", border=0)

    pdf.set_fill_color(220, 230, 241); pdf.set_xy(10, 125)
    pdf.set_font("Arial", "B", 9); pdf.cell(90.5, 6, "Gas Account Summary", fill=True, new_y=YPos.NEXT)
    pdf.line(10, 131, 100, 131)

    pdf.set_xy(10, 132)
    pdf.cell(50, 6, "Opening Balance", align='L')
    pdf.set_font("Arial", "B", 9.5); pdf.cell(40, 6, f"${float(statement_opening_balance):,.2f}", new_x=XPos.LMARGIN, new_y=YPos.NEXT, align='R')
    pdf.cell(50, 6, "Payment received", align='L')
    pdf.set_font("Arial", "B", 9.5); pdf.cell(40, 6, f"${float(statement_payment_received):,.2f}", new_x=XPos.LMARGIN, new_y=YPos.NEXT, align='R')
    pdf.cell(50, 6, "Balance carried forward", align='L')
    pdf.set_font("Arial", "B", 9.5); pdf.cell(40, 6, f"${float(statement_balance_carried_forward):,.2f}", new_x=XPos.LMARGIN, new_y=YPos.NEXT, align='R')
    pdf.line(75, 149, 100, 149)

    # Current Charges summary
    pdf.set_xy(10, 149.5)
    pdf.set_font("Arial", "BU", 9)
    pdf.cell(80, 6, "Current Charges", border=0); pdf.ln(6)

    order = [
        "Firm Gas Sales",
        "Overrun Charges",
        "Transport Fee",
        "Distribution Charges",
        "Adjustment Charges",
        "Other Charges"
    ]

    s_for_pie_cat_statement = (
        charges_df.groupby("Charge Category")["Statement Amount ex GST"]
                    .apply(lambda s: pd.to_numeric(s, errors="coerce").fillna(0.0).sum())
    )

    s_for_pie_cat_statement = s_for_pie_cat_statement[s_for_pie_cat_statement > 0]

    s_for_pie_cat_statement = s_for_pie_cat_statement.reindex(order).dropna()

    for category, category_statement_total_amount in s_for_pie_cat_statement.items():
        pdf.set_x(10); pdf.set_font("Arial", "", 9)
        pdf.cell(50, 5, str(category), border=0, align="L")
        pdf.cell(40, 5, f"${float(category_statement_total_amount):,.2f}", border=0, new_x=XPos.LMARGIN, new_y=YPos.NEXT, align="R")

    pdf.set_xy(10, 185); pdf.set_font("Arial", "B", 9)
    pdf.cell(50, 4, "Total of Current Charges", align='L')
    pdf.set_font("Arial", "B", 9.5); pdf.cell(40, 4, f"${float(statement_total_amount):,.2f}", new_x=XPos.LMARGIN, new_y=YPos.NEXT, align='R')
    pdf.set_font("Arial", "B", 9); pdf.cell(50, 4, "GST Payable on Current Charges", align='L')
    pdf.set_font("Arial", "B", 9.5); pdf.cell(40, 4, f"${float(statement_gst_amount):,.2f}", new_x=XPos.LMARGIN, new_y=YPos.NEXT, align='R')
    pdf.line(75, 194, 100, 194)
    pdf.set_font("Arial", "B", 9); pdf.cell(50, 8, "Total of Current Charges Inc GST", align='L')
    pdf.set_font("Arial", "B", 9.5); pdf.cell(40, 8, f"${float(statement_total_in_gst_amount):,.2f}", new_x=XPos.LMARGIN, new_y=YPos.NEXT, align='R')
    pdf.line(10, 199.5, 100, 199.5)

    # ---- Pie Chart (right) ----
    if not s_for_pie_cat_statement.empty:
        buf1 = generate_pie_chart(s_for_pie_cat_statement, custom_colors)
        pdf.image(buf1, x=115, y=123.5, w=80)
        logger.info(f"Styled pie chart embedded for statement {statement_number} at x=115, y=123.5, w=80")

    # Footer notes & payments
    pdf.set_xy(10, 202)
    pdf.set_font("Arial", "B", 9)
    pdf.multi_cell(180, 6, "Agora Retail also operates in the retail natural gas market in Victoria supplying gas to customers who consume over ten terajoules (TJ) of gas per annum.", fill=True)
    pdf.line(10, 217, 190, 217)

    if invoice_agg_code is not None and item_listed_bills is None:
        pdf.set_xy(10, 218)
        pdf.set_fill_color(230, 230, 230)
        pdf.rect(10, 218, 180, 50, style='F')
        pdf.cell(180, 6, "Please Refer to the Following Tax Invoices to Make the Payments", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        pdf.set_font("Arial", "", 9)
        for inv_no in invoice_numbers:

            inv_row = headers.loc[headers["invoice_number"] == inv_no]

            if not inv_row.empty:
                inv_incl_gst = float(inv_row["total_in_gst_amount"].iloc[0] or 0.0)
            else:
                inv_incl_gst = 0.0

            formatted_amt = f"${inv_incl_gst:,.2f}"

            pdf.set_x(20)
            pdf.cell(
                80, 6,
                f"Tax Invoice No. {inv_no} - {formatted_amt}",
                fill=True,
                new_x=XPos.LMARGIN,
                new_y=YPos.NEXT
            )

    else:
        # EFT Block (Left)
        pdf.set_xy(10, 219)
        pdf.set_fill_color(220, 230, 241)
        pdf.set_font("Arial", "B", 9)
        pdf.cell(80, 6, "Electronic Fund Transfer (EFT)", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        pdf.cell(80, 6, f"Reference No. {statement_number}", fill=True, new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        pdf.set_font("Arial", "B", 9); pdf.cell(30, 6, "Account Name", new_x=XPos.RIGHT, new_y=YPos.TOP)
        pdf.set_font("Arial", "", 9); pdf.cell(60, 6, "Agora Retail Pty Limited", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        pdf.set_font("Arial", "B", 9); pdf.cell(30, 6, "BSB", new_x=XPos.RIGHT, new_y=YPos.TOP)
        pdf.set_font("Arial", "", 9); pdf.cell(60, 6, "182 800  (Macquarie Bank Limited)", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        pdf.set_font("Arial", "B", 9); pdf.cell(30, 6, "Account Number", new_x=XPos.RIGHT, new_y=YPos.TOP)
        pdf.set_font("Arial", "", 9); pdf.cell(60, 6, "1165 7541", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        pdf.set_font("Arial", "", 8.5); pdf.cell(30, 4, "Please send remittance Advice To", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        pdf.set_font("Arial", "", 8.5); pdf.cell(60, 4, "accounts@agoraretail.com.au", new_x=XPos.LMARGIN, new_y=YPos.NEXT)

        # Alternative Block (Right)
        pdf.set_xy(110, 219)
        pdf.set_font("Arial", "B", 9)
        pdf.cell(80, 6, "Alternative Form of Payments*", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        pdf.set_x(110)
        pdf.cell(80, 6, f"Reference No. {statement_number}", fill=True, new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        pdf.set_xy(110, pdf.get_y())
        pdf.set_font("Arial", "B", 9)
        pdf.cell(50, 4, "Call us at", new_x=XPos.RIGHT, new_y=YPos.TOP)
        pdf.set_x(145)
        pdf.set_font("Arial", "", 9)
        pdf.cell(60, 4, "1800 403 093", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        pdf.ln(14)
        pdf.set_x(110)
        pdf.set_font("Arial", "", 7.5)
        pdf.cell(50, 4, "*Surcharge fee may apply to the payment method other than EFT.")
        pdf.line(10, 258.5, 190, 258.5)
    

def generate_invoice_page1(pdf, inv, breakdown, daily, logger):
    f = unpack_invoice_fields(inv, breakdown)
    invoice_agg_code = f["invoice_agg_code"]
    item_listed_bills, company_name, abn, postal_address = f["item_listed_bills"], f["company_name"], f["abn"], f["postal_address"]
    distributor_name, emergency_contact = f["distributor_name"], f["emergency_contact"]
    customer_number, acct, premises_address = f["customer_number"], f["acct"], f["premises_address"]
    purchase_order, invoice_number, statement_number = f["purchase_order"], f["invoice_number"], f["statement_number"]
    start_date, end_date, issue_date, due_date = f["start_date"], f["end_date"], f["issue_date"], f["due_date"]
    total_in_gst_amount, total_amount, gst_amount = f["total_in_gst_amount"], f["total_amount"], f["gst_amount"]
    charges_df, custom_colors = f["charges_df"], f["custom_colors"]
    contact_number, distributor_name = f["contact_number"], f["distributor_name"]
    opening_balance, payment_received, balance_carried_forward = f["opening_balance"], f["payment_received"], f["balance_carried_forward"]
    read_type = f["read_type"]
    interval_metering = f["interval_metering"]

    pdf.add_page()

    # Title
    pdf.set_font("Arial", "", 16)
    pdf.set_x(10); pdf.set_y(35)
    pdf.cell(0, 10, "Tax Invoice", new_x=XPos.LMARGIN, new_y=YPos.NEXT, align="L")
    pdf.ln(10)

    # Left: Customer block (use company, ABN, postal address)
    x0, y0 = 10, 45
    bg_w = 90
    line_h = 6

    # Compute an approximate block height without using split_only (compat with older fpdf2)
    postal_text = f"{postal_address or ''}\n"
    postal_lines_est = max(1, len(str(postal_text).splitlines()))
    block_h = 6 + 6 + 4 + 6 + 6 + 6

    pdf.set_fill_color(220, 230, 241)
    pdf.rect(x0, y0, bg_w, block_h, style='F')
    pdf.set_xy(x0, y0)
    pdf.set_font("Arial", "B", 10)
    pdf.cell(80, 6, str(company_name or ""), new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.cell(80, 6, f"ABN: {abn or ''}", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.cell(80, 4, "", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.cell(80, 6, "Accounts Payable", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.multi_cell(80, 6, postal_text, new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.ln(10)

    # Right: Contact & Emergency
    right_edge = 190
    block_start_x = 90
    label_width = 70
    value_width = (right_edge - block_start_x) - label_width

    pdf.set_xy(block_start_x, 45)
    pdf.set_font("Arial", "B", 9); pdf.cell(label_width, 9, "Contact Us", align='R')
    pdf.set_font("Arial", "", 9.5); pdf.cell(value_width, 9, str(contact_number or ""), new_x=XPos.LMARGIN, new_y=YPos.NEXT, align='R')
    pdf.set_x(block_start_x); pdf.set_font("Arial", "B", 9)
    pdf.multi_cell(label_width, 6, f"Distributor ({distributor_name or ''})\n Contact Number", align='R')
    pdf.set_xy(block_start_x + label_width, pdf.get_y() - 12)
    pdf.set_font("Arial", "", 9.5); pdf.multi_cell(value_width, 12, str(contact_number or ""), align='R')
    pdf.set_x(block_start_x); pdf.set_font("Arial", "BU", 9)
    pdf.multi_cell(label_width, 7, f"{distributor_name or ''} Gas Fault & Emergency\n Contact (24 Hrs)", align='R')
    pdf.set_xy(block_start_x + label_width, pdf.get_y() - 14)
    pdf.set_font("Arial", "U", 9.5); pdf.multi_cell(value_width, 12, str(emergency_contact or ""), align='R')

    # Divider
    y_top = 82; pdf.set_draw_color(0,0,0); pdf.set_line_width(0.35); pdf.line(10, y_top, 190, y_top)

    # Left: Account & invoice info
    pdf.set_xy(10, 85)
    write_label_value(pdf, "Customer Number", customer_number, x=10, line_height=6)
    write_label_value(pdf, "Account Number", acct, x=10, line_height=6)
    pdf.ln(4)
    write_label_value(pdf, "Purchase Order #", purchase_order, x=10, line_height=6)
    if invoice_agg_code is not None and item_listed_bills == "Yes":
        write_label_value(pdf, "Tax Invoice No.", statement_number, x=10, line_height=6)
    else:
        write_label_value(pdf, "Tax Invoice No.", invoice_number, x=10, line_height=6)
    start_dt = pd.to_datetime(start_date, errors="coerce")
    end_dt   = pd.to_datetime(end_date,   errors="coerce")
    billing_period_lbl = (
        f"{start_dt.strftime('%d-%b-%y')} to {end_dt.strftime('%d-%b-%y')}"
        if pd.notna(start_dt) and pd.notna(end_dt) else ""
    )
    write_label_value(pdf, "Billing Cycle", billing_period_lbl, x=10, line_height=6)

    # Right: Issue/Due/Total
    right_start_x = block_start_x
    label_w_right = label_width
    value_w_right = value_width
    pdf.set_xy(right_start_x, 85)
    issue_dt = pd.to_datetime(issue_date, errors="coerce")
    due_dt   = pd.to_datetime(due_date,   errors="coerce")

    write_label_value(pdf, 
        "Issue Date",
        issue_dt.strftime("%d-%b-%y") if pd.notna(issue_dt) else "",
        x=right_start_x, label_w=label_w_right, value_w=value_w_right,
        align="R", bold_size=10, reg_size=10.5
    )

    write_label_value(pdf, 
        "Due Date",
        due_dt.strftime("%d-%b-%y") if pd.notna(due_dt) else "",
        x=right_start_x, label_w=label_w_right, value_w=value_w_right,
        align="R", bold_size=10, reg_size=10.5
    )

    pdf.ln(14)
    write_label_value(pdf, 
        "Total Amount Payable", f"${total_in_gst_amount:,.2f}",
        x=right_start_x, label_w=label_w_right, value_w=value_w_right,
        align="R", bold_size=10, reg_size=10.5)

   # Divider
    pdf.ln(10); pdf.set_draw_color(0,0,0); pdf.set_line_width(0.35); pdf.line(10, 119, 190, 119)

    # Gas Account Summary (left)
    if invoice_agg_code is None or str(invoice_agg_code).strip() == "":
        pdf.set_font("Arial", "I", 7)
        if not (interval_metering or "").startswith("Interval") and read_type == "A":
            msg = "This invoice is based on the actual usage data"
        elif not (interval_metering or "").startswith("Interval") and read_type == "E":
            msg = "This invoice is based on the estimated usage data"
        elif not (interval_metering or "").startswith("Interval") and read_type == "S":
            msg = "This invoice is based on the substitute usage data"
        else:
            msg = "This invoice is based on usage data provided by network providers"
        pdf.cell(50, 8, msg, border=0)
    else:
        pdf.set_font("Arial", "I", 7)
        pdf.cell(50, 8, "This invoice is based on usage data provided by network providers", border=0)
    pdf.set_fill_color(220, 230, 241); pdf.set_xy(10, 125)
    pdf.set_font("Arial", "B", 9); pdf.cell(90.5, 6, "Gas Account Summary", fill=True, new_y=YPos.NEXT)
    pdf.line(10, 131, 100, 131)

    pdf.set_xy(10, 132)
    pdf.cell(50, 6, "Opening Balance", align='L')
    pdf.set_font("Arial", "B", 9.5); pdf.cell(40, 6, f"${float(opening_balance):,.2f}", new_x=XPos.LMARGIN, new_y=YPos.NEXT, align='R')
    pdf.cell(50, 6, "Payment received", align='L')
    pdf.set_font("Arial", "B", 9.5); pdf.cell(40, 6, f"${float(payment_received):,.2f}", new_x=XPos.LMARGIN, new_y=YPos.NEXT, align='R')
    pdf.cell(50, 6, "Balance carried forward", align='L')
    pdf.set_font("Arial", "B", 9.5); pdf.cell(40, 6, f"${float(balance_carried_forward):,.2f}", new_x=XPos.LMARGIN, new_y=YPos.NEXT, align='R')
    pdf.line(75, 149, 100, 149)

    # Current Charges summary
    pdf.set_xy(10, 149.5)
    pdf.set_font("Arial", "BU", 9)
    pdf.cell(80, 6, "Current Charges", border=0); pdf.ln(6)

    order = [
        "Firm Gas Sales",
        "Overrun Charges",
        "Transport Fee",
        "Distribution Charges",
        "Adjustment Charges",
        "Other Charges"
    ]

    # Apply numeric conversion and aggregation
    s_for_pie_cat = (
        charges_df.groupby("Charge Category")["Amount ex GST"]
                .apply(lambda s: pd.to_numeric(s, errors="coerce").fillna(0.0).sum())
    )

    # Keep only positive values
    s_for_pie_cat = s_for_pie_cat[s_for_pie_cat > 0]

    s_for_pie_cat = s_for_pie_cat.reindex(order).dropna()

    for category, amount in s_for_pie_cat.items():
        pdf.set_x(10); pdf.set_font("Arial", "", 9)
        pdf.cell(50, 5, str(category), border=0, align="L")
        pdf.cell(40, 5, f"${float(amount):,.2f}", border=0, new_x=XPos.LMARGIN, new_y=YPos.NEXT, align="R")

    pdf.set_xy(10, 185); pdf.set_font("Arial", "B", 9)
    pdf.cell(50, 4, "Total of Current Charges", align='L')
    pdf.set_font("Arial", "B", 9.5); pdf.cell(40, 4, f"${float(total_amount):,.2f}", new_x=XPos.LMARGIN, new_y=YPos.NEXT, align='R')
    pdf.set_font("Arial", "B", 9); pdf.cell(50, 4, "GST Payable on Current Charges", align='L')
    pdf.set_font("Arial", "B", 9.5); pdf.cell(40, 4, f"${float(gst_amount):,.2f}", new_x=XPos.LMARGIN, new_y=YPos.NEXT, align='R')
    pdf.line(75, 194, 100, 194)
    pdf.set_font("Arial", "B", 9); pdf.cell(50, 8, "Total of Current Charges Inc GST", align='L')
    pdf.set_font("Arial", "B", 9.5); pdf.cell(40, 8, f"${float(total_in_gst_amount):,.2f}", new_x=XPos.LMARGIN, new_y=YPos.NEXT, align='R')
    pdf.line(10, 199.5, 100, 199.5)


    # ---- Pie Chart (right) ----
    if not s_for_pie_cat.empty:
        buf1 = generate_pie_chart(s_for_pie_cat, custom_colors)
        pdf.image(buf1, x=115, y=123.5, w=80)
        logger.info(f"Styled pie chart embedded for invoice {invoice_number} at x=115, y=123.5, w=80")

    # Footer notes & payments
    pdf.set_fill_color(220, 230, 241)
    pdf.set_xy(10, 202)
    pdf.set_font("Arial", "B", 9)
    pdf.multi_cell(180, 6, "Agora Retail also operates in the retail natural gas market in Victoria supplying gas to customers who consume over ten terajoules (TJ) of gas per annum.", fill=True)
    pdf.line(10, 217, 190, 217)

    # EFT Block (Left)
    pdf.set_xy(10, 219)
    pdf.set_font("Arial", "B", 9)
    pdf.cell(80, 6, "Electronic Fund Transfer (EFT)", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.cell(80, 6, f"Reference No. {invoice_number}", fill=True, new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.set_font("Arial", "B", 9); pdf.cell(30, 6, "Account Name", new_x=XPos.RIGHT, new_y=YPos.TOP)
    pdf.set_font("Arial", "", 9); pdf.cell(60, 6, "Agora Retail Pty Limited", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.set_font("Arial", "B", 9); pdf.cell(30, 6, "BSB", new_x=XPos.RIGHT, new_y=YPos.TOP)
    pdf.set_font("Arial", "", 9); pdf.cell(60, 6, "182 800  (Macquarie Bank Limited)", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.set_font("Arial", "B", 9); pdf.cell(30, 6, "Account Number", new_x=XPos.RIGHT, new_y=YPos.TOP)
    pdf.set_font("Arial", "", 9); pdf.cell(60, 6, "1165 7541", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.set_font("Arial", "", 8.5); pdf.cell(30, 4, "Please send remittance Advice To", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.set_font("Arial", "", 8.5); pdf.cell(60, 4, "accounts@agoraretail.com.au", new_x=XPos.LMARGIN, new_y=YPos.NEXT)

    # Alternative Block (Right)
    pdf.set_xy(110, 219)
    pdf.set_font("Arial", "B", 9)
    pdf.cell(80, 6, "Alternative Form of Payments*", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.set_x(110)
    pdf.cell(80, 6, f"Reference No. {invoice_number}", fill=True, new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.set_xy(110, pdf.get_y())
    pdf.set_font("Arial", "B", 9)
    pdf.cell(50, 4, "Call us at", new_x=XPos.RIGHT, new_y=YPos.TOP)
    pdf.set_x(145)
    pdf.set_font("Arial", "", 9)
    pdf.cell(60, 4, "1800 403 093", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.ln(14)
    pdf.set_x(110)
    pdf.set_font("Arial", "", 7.5)
    pdf.cell(50, 4, "*Surcharge fee may apply to the payment method other than EFT.")
    pdf.line(10, 258.5, 190, 258.5)


def generate_invoice_page2(pdf, inv, breakdown, daily, basic, logger):
    f = unpack_invoice_fields(inv, breakdown)
    item_listed_bills = f["item_listed_bills"]
    stack_consumption_chart = f["stack_consumption_chart"]
    invoice_number = f["invoice_number"]
    acct, mirn, transmission_pipeline = f["acct"], f["mirn"], f["transmission_pipeline"]
    distributor_name, distributor_mhq = f["distributor_name"], f["distributor_mhq"]

    start_date_dt = pd.to_datetime(f["start_date"], errors="coerce")
    end_date_dt = pd.to_datetime(f["end_date"], errors="coerce")

    read_start_date = pd.to_datetime(f["read_start_date"], errors="coerce")
    read_end_date = pd.to_datetime(f["read_end_date"], errors="coerce")
    if pd.notna(read_start_date) and pd.notna(read_end_date):
        read_period = f"{read_start_date.strftime('%d-%b-%y')} to {read_end_date.strftime('%d-%b-%y')}"
    else:
        read_period = ""
    
    interval_metering = f["interval_metering"]
    premises_address, charges_df = f["premises_address"], f["charges_df"]
    trading_name = f["trading_name"]
    total_amount, gst_amount = f["total_amount"], f["gst_amount"]
    charge_notes = f["charge_notes"]
    total_consumption = f["total_consumption"]
    invoice_agg_code = f["invoice_agg_code"]
    statement_number = f["statement_number"]
    spot_gas_amount = f["spot_gas_amount"]
    customer_number = f["customer_number"]
    meter_number = f["meter_number"]
    billing_days = f["billing_days"]

    pdf.add_page()

    label_width_left = 40
    label_width_right = 33.5
    left_x = 10
    right_x = 105
    right_value_width = 50
    font_main = "Arial"

    def set_font(weight: str = "", size: float = 8.5) -> None:
        pdf.set_font(font_main, weight, size)

    # Account Details (Left Block)
    pdf.set_xy(left_x, 35)
    set_font("", 9.5)
    if invoice_agg_code is not None and item_listed_bills == "Yes":
        write_label_value(pdf, "Tax Invoice No.", statement_number, x=10, label_w=label_width_left)
    else:
        write_label_value(pdf, "Tax Invoice No.", invoice_number, x=10, label_w=label_width_left)
    # write_label_value(pdf, "Tax Invoice No.", invoice_number, x=10, label_w=label_width_left)
    write_label_value(pdf, "Account No.", acct, x=10, label_w=label_width_left)
    write_label_value(pdf, "MIRN", mirn, x=10, label_w=label_width_left)
    write_label_value(pdf, "Trading Name", trading_name, x=10, label_w=label_width_left)
    write_label_value(pdf, "Transmission Pipeline", transmission_pipeline, x=10, label_w=label_width_left)
    write_label_value(pdf, "(if any)", "", x=10, label_w=label_width_left, line_height=3)
    pdf.ln(2)

    # Billing Details (Right Block)
    pdf.set_xy(right_x, 35)
    billing_days = _to_int_or_none(billing_days)
    if billing_days == 0:
        read_period_with_days = f"No meter read data ({billing_days} days)"
    else:
        read_period_with_days = f"{read_period} ({billing_days} days)"
    write_label_value(pdf, "Read Period", read_period_with_days, x=105, label_w=label_width_right, value_w=right_value_width)
    write_label_value(pdf, "Distributor", distributor_name, x=105, label_w=label_width_right, value_w=right_value_width)
    write_label_value(pdf, "Meter Number", meter_number, x=105, label_w=label_width_right, value_w=right_value_width)
    write_label_value(pdf, 
                      "Distributor MHQ", 
                      (f"{float(distributor_mhq):.2f}" if (pd.notna(distributor_mhq) and str(distributor_mhq).strip() != "") else ""),
                      x=105, label_w=label_width_right, value_w=right_value_width, unit=" GJ")
    write_label_value(pdf, "Premises Address", premises_address, x=105, wrap=True, force_two_lines=True, label_w=label_width_right, value_w=right_value_width)

    # Divider & Section Header
    pdf.line(10, 64.5, 190, 64.5)
    pdf.set_y(65)
    set_font("BU", 8)
    pdf.cell(60, 4, "Your Gas usage and Charges summary")
    pdf.ln(4)

    columns = [
        {"header": "Charges", "width": 60, "align": "L"},
        {"header": "Rate", "width": 30, "align": "R"},
        {"header": "Unit", "width": 15, "align": "L"},
        {"header": "Total Qty", "width": 30, "align": "R"},
        {"header": "Unit", "width": 15, "align": "L"},
        {"header": "$      ", "width": 30, "align": "R"},
    ]
    stripe_height = 4
    stripe_colors = [(220, 230, 241), (255, 255, 255)]
    x_start = 10
    y_start = 74
    total_width = sum(col["width"] for col in columns)

    def draw_table_bands() -> None:
        for i in range(15):
            pdf.set_fill_color(*stripe_colors[i % 2])
            pdf.rect(x_start, y_start + i * stripe_height, total_width, stripe_height, style='F')

    def draw_table_header() -> None:
        pdf.set_font("Arial", "B", 8)
        pdf.set_fill_color(0, 137, 208)
        pdf.set_text_color(255, 255, 255)
        for col in columns:
            pdf.cell(col["width"], 4.1, col["header"], border="TB", align=col["align"], fill=True)
        pdf.ln()
        pdf.set_text_color(0, 0, 0)

    draw_table_bands()
    draw_table_header()

    row_index = 0

    def new_detail_page() -> None:
        nonlocal row_index
        pdf.add_page()
        pdf.set_y(35)
        set_font("BU", 10); pdf.cell(100, 6, "Gas usage and Charges (continued)")
        pdf.ln(6)
        draw_table_bands()
        draw_table_header()
        row_index = 0

    for category, cat_df in charges_df.groupby("Charge Category", sort=False):
        unit_num = pd.to_numeric(cat_df.get("Amount ex GST"), errors="coerce")
        cat_df = cat_df.loc[unit_num.notna() & (unit_num.abs() > 1e-12)]
        if cat_df.empty:
            continue
        # Category header row
        if row_index >= 22:
            new_detail_page()
        pdf.set_y(y_start + row_index * stripe_height)
        pdf.set_x(x_start)
        pdf.set_font("Arial", "BU", 8)
        pdf.set_fill_color(*stripe_colors[row_index % 2])
        pdf.cell(columns[0]["width"], stripe_height, str(category), border=0, align="L", fill=True)
        for col in columns[1:]:
            pdf.cell(col["width"], stripe_height, "", border=0, align=col["align"], fill=True)
        pdf.ln(stripe_height)
        row_index += 1

        # Detail rows
        pdf.set_font("Arial", "", 8)
        for _, row in cat_df.iterrows():
            if row_index >= 22:
                new_detail_page()

            _amt = pd.to_numeric(row.get("Amount ex GST"), errors="coerce")
            amt_ex_gst_fmt = f"${_amt:,.2f}" if pd.notna(_amt) else ""

            charge_type = str(row.get("Charge Type", "")) or ""

            raw_rate = row.get("Rate")

            try:
                rate_val = Decimal(str(raw_rate))
            except (InvalidOperation, TypeError):
                rate_val = None

            if rate_val is not None:
                if charge_type in ("Firm Gas Sales", "Overrun Charges"):
                    rate_fmt = f"{rate_val:.6f}"
                else:
                    rate_fmt = f"{rate_val:.2f}"
            else:
                rate_fmt = ""

            values = [
                "          " + charge_type,
                rate_fmt,
                str(row.get("Rate UOM", "")) if pd.notna(row.get("Rate UOM")) else "",
                "" if pd.isna(row.get("Unit")) else f"{float(row.get('Unit')):,.2f}",\
                str(row.get("Unit UOM", "")) if pd.notna(row.get("Unit UOM")) else "",
                amt_ex_gst_fmt,
            ]

            pdf.set_y(y_start + row_index * stripe_height)
            pdf.set_x(x_start)
            pdf.set_fill_color(*stripe_colors[row_index % 2])
            for i, col in enumerate(columns):
                pdf.cell(col["width"], stripe_height, values[i], border=0, align=col["align"], fill=True)
            pdf.ln(stripe_height)
            row_index += 1

    # Totals box
    pdf.set_y(134.5)
    pdf.set_font("Arial", "B", 8.5)
    pdf.cell(90, 4, "Total of Current Charges", align='L')
    pdf.cell(90, 4, f"${float(total_amount):,.2f}", new_x=XPos.LMARGIN, new_y=YPos.NEXT, align='R')
    pdf.cell(90, 4, "GST Payable on Current Charges", align='L')
    pdf.cell(90, 4, f"${float(gst_amount):,.2f}", new_x=XPos.LMARGIN, new_y=YPos.NEXT, align='R')
    pdf.line(10, pdf.get_y()+1, 190, pdf.get_y()+1)

    def fmt_gj(x: Any) -> str:
                try:
                    return f"{float(x):.2f} GJ"
                except (TypeError, ValueError):
                    return ""
                
    # === Daily consumption ===
    cust_consumption = pd.DataFrame()
    if not daily.empty and "mirn" in daily.columns:
        cust_consumption = daily[daily["mirn"] == mirn].copy()

    cust_consumption_basic = pd.DataFrame()
    if not basic.empty and "mirn" in basic.columns:
        basic["cur_read_date"] = pd.to_datetime(basic["cur_read_date"], errors="coerce")
        
        start_date = (end_date_dt - pd.DateOffset(months=5)).replace(day=1)

        basic_6m = basic[basic["cur_read_date"] >= start_date]

        cust_consumption_basic = basic_6m[basic_6m["mirn"] == mirn].copy()

    # Calculate monthly_mdq (maximum daily consumption)
    monthly_mdq = None
    if not cust_consumption.empty:
        monthly_mdq = cust_consumption["gj_consumption"].max()  # Maximum daily consumption

    contract_mdq = (
        cust_consumption["retail_mdq"].iloc[0]
        if "retail_mdq" in cust_consumption.columns and not cust_consumption.empty
        else None
    )

    # === Chart sizing constants ===
    CHART_W_MM      = 180.0
    CHART_FULL_H_MM = 64.0
    CHART_HALF_H_MM = 32.0

    # === CHARTS ===
    # === Determine MIRNs eligible for stack charts ===
    stack_mirns = (
        daily.loc[
            (daily["customer_number"] == customer_number) &
            (daily["stack_consumption_chart"] == "Yes")
        ]["mirn"]
        .astype(str)
        .unique()
        .tolist()
    )

    global_start_date = (
        daily.loc[daily["mirn"].isin(stack_mirns), "read_start_date"]
        .dropna()
        .min()
    )

    global_end_date = (
        daily.loc[daily["mirn"].isin(stack_mirns), "read_end_date"]
        .dropna()
        .max()
    )

    if pd.notna(global_start_date) and pd.notna(global_end_date):
        global_read_period = f"{global_start_date.strftime('%d-%b-%y')} to {global_end_date.strftime('%d-%b-%y')}"
    else:
        global_read_period = ""

    # Flag for current MIRN
    is_stack_mirn = str(mirn) in stack_mirns

    aggregated_contract_mdq = 0.0

    if stack_consumption_chart == "Yes":
        # Sum all retail_mdq for this customer_number
        aggregated_contract_mdq = (
            daily.loc[
                (daily["customer_number"] == customer_number)
                & (daily["stack_consumption_chart"] == "Yes"),
                "retail_mdq"
            ]            
            .dropna()
            .astype(float)
            .unique()
            .sum()
        )

    selected_mirns = stack_mirns
    multi_mirn_ok = False
    label_w = 68
    value_w = 40

    # ================= Full-height Consumption Chart =================
    if not is_stack_mirn:

        interval_type = (interval_metering or "").strip()

        if not interval_type.startswith("Basic"):
            # # === Comment above Consumption Chart (Right) ===
            # pdf.set_font("Arial", "BU", 8)
            # if monthly_mdq > contract_mdq:
            #     if spot_gas_amount == 0:
            #         pdf.set_xy(137, 145)
            #         pdf.cell(100, 4, "No Overrun Charges, as this MIRN", ln=1)
            #         pdf.set_xy(137, 149)
            #         pdf.cell(100, 4, "is a part of Aggregated portfolio", ln=1)

            #     else:
            #         pdf.set_xy(137, 145)
            #         pdf.cell(100, 4, "Spot Gas Sales & Transport Charges", ln=1)
            #         pdf.set_xy(137, 149)
            #         pdf.cell(100, 4, "are billed as a separate line item", ln=1)

            # === MDQ box above Consumption Chart ===
            pdf.set_xy(10, 145)

            # MDQ box for monthly and contract MDQ
            pdf.set_font("Arial", "BU", 8)
            pdf.cell(label_w, 4, "Total Consumption for the Month")
            pdf.set_font("Arial", "U", 8)
            pdf.cell(value_w, 4, fmt_gj(total_consumption), new_x=XPos.LMARGIN, new_y=YPos.NEXT)

            pdf.set_font("Arial", "BU", 8)
            pdf.cell(label_w, 4, "MDQ of the month")
            pdf.set_font("Arial", "U", 8)
            pdf.cell(value_w, 4, fmt_gj(monthly_mdq), new_x=XPos.LMARGIN, new_y=YPos.NEXT)

            pdf.set_font("Arial", "BU", 8)
            pdf.cell(label_w, 4, "Contract MDQ")
            val = "" if contract_mdq is None else fmt_gj(contract_mdq)
            pdf.set_font("Arial", "U", 8)
            pdf.cell(value_w, 4, val, new_x=XPos.LMARGIN, new_y=YPos.NEXT)

            pdf.rect(10, 145, 90, 12)
            pdf.ln(1)

            pdf.set_xy(10, 160)
            # Generate FULL interval chart
            consumption_chart_buf = generate_consumption_chart(
                mirn=mirn,
                read_period=read_period,
                cust_consumption_df=cust_consumption,
                contract_mdq=contract_mdq,
                fig_height_mm=CHART_FULL_H_MM
            )
            embed_chart_in_pdf(pdf, consumption_chart_buf, CHART_FULL_H_MM, logger)

        else:
            # === Total Consumption box above Consumption Chart ===
            pdf.set_xy(10, 149)

            # MDQ box for monthly and contract MDQ
            pdf.set_font("Arial", "BU", 8)
            pdf.cell(label_w, 4, "Total Consumption for the Month")
            pdf.set_font("Arial", "U", 8)
            pdf.cell(value_w, 4, fmt_gj(total_consumption), new_x=XPos.LMARGIN, new_y=YPos.NEXT)

            pdf.rect(10, 145, 90, 12)
            pdf.ln(1)

            pdf.set_xy(10, 160)
            # Generate BASIC non-interval chart
            consumption_chart_buf_basic = generate_consumption_chart_basic(
                mirn=mirn,
                basic_read_period=read_period,
                basic_cust_consumption_df=cust_consumption_basic,
                contract_mdq=contract_mdq,
                fig_height_mm=CHART_FULL_H_MM
            )
            embed_chart_in_pdf(pdf, consumption_chart_buf_basic, CHART_FULL_H_MM, logger)

    # ================= Half-height Consumption Chart + Agg MDQ Box =================
    if is_stack_mirn:

        interval_type = (interval_metering or "").strip()

        if not interval_type.startswith("Basic"):
            # === MDQ box above Consumption Chart ===
            pdf.set_xy(10, 145)

            # MDQ box for monthly and contract MDQ
            pdf.set_font("Arial", "BU", 8)
            pdf.cell(label_w, 4, "Total Consumption for the Month")
            pdf.set_font("Arial", "U", 8)
            pdf.cell(value_w, 4, fmt_gj(total_consumption), new_x=XPos.LMARGIN, new_y=YPos.NEXT)

            pdf.set_font("Arial", "BU", 8)
            pdf.cell(label_w, 4, "MDQ of the month")
            pdf.set_font("Arial", "U", 8)
            pdf.cell(value_w, 4, fmt_gj(monthly_mdq), new_x=XPos.LMARGIN, new_y=YPos.NEXT)

            pdf.set_font("Arial", "BU", 8)
            pdf.cell(label_w, 4, "Contract MDQ")
            val = "" if contract_mdq is None else fmt_gj(contract_mdq)
            pdf.set_font("Arial", "U", 8)
            pdf.cell(value_w, 4, val, new_x=XPos.LMARGIN, new_y=YPos.NEXT)

            pdf.rect(10, 145, 90, 12)
            pdf.ln(1)

            pdf.set_xy(10, 160)
            # Generate HALF interval chart
            consumption_chart_buf = generate_consumption_chart(
                mirn=mirn,
                read_period=read_period,
                cust_consumption_df=cust_consumption,
                contract_mdq=contract_mdq,
                fig_height_mm=CHART_HALF_H_MM
            )
            embed_chart_in_pdf(pdf, consumption_chart_buf, CHART_HALF_H_MM, logger)
        
        else:
            # === MDQ box above Consumption Chart ===
            pdf.set_xy(10, 149)

            # MDQ box for monthly and contract MDQ
            pdf.set_font("Arial", "BU", 8)
            pdf.cell(label_w, 4, "Total Consumption for the Month")
            pdf.set_font("Arial", "U", 8)
            pdf.cell(value_w, 4, fmt_gj(total_consumption), new_x=XPos.LMARGIN, new_y=YPos.NEXT)

            pdf.rect(10, 145, 90, 12)
            pdf.ln(1)

            pdf.set_xy(10, 160)
            # Generate HALF interval chart
            consumption_chart_buf_basic = generate_consumption_chart_basic(
                mirn=mirn,
                basic_read_period=read_period,
                basic_cust_consumption_df=cust_consumption_basic,
                contract_mdq=contract_mdq,
                fig_height_mm=CHART_HALF_H_MM
            )
            embed_chart_in_pdf(pdf, consumption_chart_buf_basic, CHART_HALF_H_MM, logger)

        # === Aggregated MDQ Block ===
        aggregated_monthly_mdq = compute_aggregated_mdq(
            daily_df=daily,
            selected_mirns=stack_mirns,
            start_date=start_date_dt,
            end_date=end_date_dt
        )

        aggregated_total_consumption = compute_total_consumption(
            daily_df=daily,
            selected_mirns=stack_mirns,
            start_date=start_date_dt,
            end_date=end_date_dt
        )

        pdf.rect(10, pdf.get_y(), 90, 12)

        pdf.set_font("Arial", "BU", 8)
        pdf.cell(label_w, 4, "Total Consumption for the Month (aggregated)")
        pdf.set_font("Arial", "U", 8)
        pdf.cell(value_w, 4, fmt_gj(aggregated_total_consumption), new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        
        pdf.set_font("Arial", "BU", 8)
        pdf.cell(label_w, 4, "MDQ of the month (aggregated)")
        pdf.set_font("Arial", "U", 8)
        pdf.cell(value_w, 4, fmt_gj(aggregated_monthly_mdq), new_x=XPos.LMARGIN, new_y=YPos.NEXT)

        pdf.set_font("Arial", "BU", 8)
        pdf.cell(label_w, 4, "Contract MDQ (aggregated)")
        pdf.set_font("Arial", "U", 8)
        pdf.cell(value_w, 4, fmt_gj(aggregated_contract_mdq), new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        pdf.ln(2)

        # === Multi-MIRN Chart (Stacked) ===
        accounts_mirn_chart_buf = generate_accounts_mirn_chart(
            read_period=global_read_period,
            df=daily,
            customer_number=customer_number,
            stack_consumption_chart_only=True,
            aggregated_contract_mdq=aggregated_contract_mdq,
            period_start=global_start_date,
            period_end=global_end_date
        )

        embed_chart_in_pdf(pdf, accounts_mirn_chart_buf, CHART_HALF_H_MM, logger)
        multi_mirn_ok = True

    # ================= “Please Note” block =================
    pdf.ln(2)
    NOTE_MIN_H = 25

    try:
        page_h = getattr(pdf, "h", 297)
        b_margin = getattr(pdf, "b_margin", 10)
        if pdf.get_y() + NOTE_MIN_H > (page_h - b_margin):
            pdf.add_page()
    except Exception:
        pass

    box_y = pdf.get_y()
    pdf.set_font("Arial", "B", 8)
    pdf.multi_cell(180, 6, "Please Note", new_x=XPos.LMARGIN, new_y=YPos.NEXT)

    pdf.set_font("Arial", "", 7)
    text1 = "* Agora Retail will reconcile Network Charges for this month against the actual charges invoiced by the Network Operator and the adjustment charges will be applied accordingly on the next invoice."
    text2 = "* gasTrading Spot Prices are published on the website: https://gastrading.com.au/spot-market/historical-prices-and-volume/bid-and-scheduled"
    text3 = f"* {str(charge_notes).strip()}" if (pd.notna(charge_notes) and str(charge_notes).strip()) else ""

    pdf.multi_cell(180, 4, text1, border=0, new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.multi_cell(180, 4, text2, border=0, new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.multi_cell(180, 4, text3, border=0)

    box_bottom = pdf.get_y()
    box_h = max(NOTE_MIN_H, box_bottom - box_y)
    pdf.rect(10, box_y, 180, box_h)

# =========================
# MAIN
# =========================
def main():
    # ---- Logging setup ----
    log_filename = "Bill_Generating_Log.log"
    global logger, sp_index

    logger = logging.getLogger("SharePointLogger")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    logger.propagate = False  # prevents double-logging if root logger has handlers

    skipped_duplicates = 0

    site_url        = os.getenv("SITE_URL")
    client_id       = os.getenv("AZURE_CLIENT_ID")
    client_secret   = os.getenv("AZURE_CLIENT_SECRET")
    folder_url      = os.getenv("FOLDER")
    logo_folder_url = os.getenv("FOLDER_LOGO")

    missing = [k for k, v in {
        "SITE_URL": site_url,
        "AZURE_CLIENT_ID": client_id,
        "AZURE_CLIENT_SECRET": client_secret,
        "FOLDER": folder_url,
        "FOLDER_LOGO": logo_folder_url
    }.items() if not v]

    if missing:
        raise RuntimeError(f"Missing required environment variables: {', '.join(missing)}")

    sp = SharePointClient(
        site_url=site_url,
        client_id=client_id,
        client_secret=client_secret,
        folder_url=folder_url,
        logo_folder_url=logo_folder_url
    )

    # ---- Log handler that writes the log file back to SharePoint ----
    sp_handler = SharePointLogHandler(sp, log_filename)
    sp_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(sp_handler)
    # ---- ALSO log to console (Azure Functions / local host output) ----
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(console_handler)

    logger.info("=============== Starting PDF generation with charts ===============")

    # ---- SharePoint folder index (used by should_process_statement) ----
    sp_index = SharePointFolderIndex(sp)
    sp_index.refresh(force=True)

    # ----- Connect to SQL ----
    monthly, breakdown, daily, basic = load_views()
    # if monthly.empty:
    #     logger.error("No data returned from dbo.vw_test_charges_monthly. Exiting.")
    #     return
    # if breakdown.empty:
    #     logger.error("No data returned from dbo.vw_test_charges_breakdown. Exiting.")
    #     return
    # if daily.empty:
    #     logger.error("No data returned from dbo.vw_test_charges_daily. Exiting.")
    #     return
    # if basic.empty:
    #     logger.error("No data returned from dbo.vw_billing_consumption_basic. Exiting.")
    #     return

    headers = build_invoice_headers_from_monthly(monthly)

    # Engine & table check
    eng = get_engine()
    ensure_billing_history_table(eng)
    stmt_run_cache = StatementRunCache()
    # ---- For each statement ----
    for original_statement_number, statement_group in headers.groupby("statement_number"):
        # 1) Build a stable list of (invoice_number, invoice_total) for this statement
        inv_totals_df = (
            statement_group[["invoice_number", "total_in_gst_amount"]]
            .dropna(subset=["invoice_number"])
            .assign(
                invoice_number=lambda d: d["invoice_number"].map(_norm_id),
                total_in_gst_amount=lambda d: d["total_in_gst_amount"].astype(float).round(2),
            )
            .groupby("invoice_number", as_index=False)["total_in_gst_amount"]
            .sum()
        )

        invoice_pairs = list(zip(inv_totals_df["invoice_number"], inv_totals_df["total_in_gst_amount"]))
        total_statement_amount = float(inv_totals_df["total_in_gst_amount"].sum().round(2))

        # IMPORTANT: sort for stability
        invoice_pairs = sorted(invoice_pairs, key=lambda t: t[0])

        content_hash = build_statement_content_hash(invoice_pairs)

        process, final_statement_number = should_process_statement(
            eng,
            original_statement_number,
            total_statement_amount,
            content_hash=content_hash,
            run_cache=stmt_run_cache,
            sp_index=sp_index,
            logger=logger
        )

        if not process:
            skipped_duplicates += 1
            continue

        logger.info(f"Processing Statement {final_statement_number} with {len(statement_group)} invoices")

        pdf = PDF()
        pdf.alias_nb_pages()
        invoices_emitted = 0
        summary_added = False

        # IMPORTANT: inject final_statement_number into the statement row BEFORE PDF generation
        # first_row = statement_group.iloc[0].copy()
        # first_row["statement_number"] = final_statement_number
        # invoice_numbers = (
        #     statement_group["invoice_number"]
        #     .dropna()
        #     .astype(str)
        #     .tolist()
        # )
        # generate_statement_summary_page(pdf, first_row, breakdown, logger, daily, invoice_numbers, headers)

        # ---- For each invoice in this statement ----
        for _, inv in statement_group.iterrows():
            inv = inv.copy()
            inv["statement_number"] = final_statement_number
            original_invoice_number = inv["invoice_number"]

            hist_row = build_history_row_from_monthly(inv)
            r2, invoice_number, do_insert = _apply_history_increment_rule(eng, hist_row)

            if not do_insert:
                skipped_duplicates += 1
                logger.info(f"Duplicate invoice with same total: {invoice_number}; skipping.")
                continue

            if not summary_added:
                first_row = statement_group.iloc[0].copy()
                first_row["statement_number"] = final_statement_number

                invoice_numbers = (
                    statement_group["invoice_number"]
                    .dropna()
                    .astype(str)
                    .tolist()
                )

            generate_statement_summary_page(pdf, first_row, breakdown, logger, daily, invoice_numbers, headers)
            summary_added = True
            
            inv["invoice_number"] = invoice_number

            # Page 1 (conditions)
            # 
            if original_invoice_number != original_statement_number:
                if not (inv["item_listed_bills"] == "Yes"):
                    generate_invoice_page1(pdf, inv, breakdown, daily, logger)

            # Page 2 always
            generate_invoice_page2(pdf, inv, breakdown, daily, basic, logger)
            invoices_emitted += 1
    
            # Insert invoice history
            if r2 is not None and do_insert:
                insert_billing_history_batch(eng, [r2])
                logger.info(f"Inserted history for {invoice_number}")
            
        # ---- Save statement PDF with **incremented name** ----
        if invoices_emitted == 0:
            logger.info(f"⏭️ Statement {final_statement_number} produced 0 new invoices; skipping PDF upload.")
            continue

        generated_ts = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S%f")
        pdf_filename = f"{final_statement_number}_Generated_{generated_ts}.pdf"

        pdf_content = pdf.output(dest="S")  # returns a str in fpdf2
        if isinstance(pdf_content, str):
            pdf_content = pdf_content.encode("latin1")

        upload_bytes_to_sharepoint(pdf_content, pdf_filename)

        logger.info(f"Statement uploaded: {pdf_filename}")

        logger.info(f"✅ ✅ ✅ Statement uploaded: {pdf_filename}")

    logger.info(f"======= Completed PDF generation; Skipped {skipped_duplicates} duplicate invoices =======")

if __name__ == "__main__":
    main()